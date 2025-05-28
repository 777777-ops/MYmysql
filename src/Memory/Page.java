package Memory;

import DataIO.BytesIO;

import java.nio.ByteBuffer;
import java.util.*;

/*

    page_level     : (1字节)  0
    page_space     : (1字节)  1
    page_offset    : (4字节)  2
    page_num       : (2字节)  6
    page_spare     : (3字节)  8
    page_used      : (3字节)  11
    page_min       : (3字节)  14   (目前不用)

   page_next_offset: (4字节)   21
   page_prev_offset: (4字节)   25

    page_slots     :(64 - 183字节)         给40个槽

   伪最大和伪最小
    伪最大         : 256 - HEAD  ----- 255
    伪最小         : 256 - 2HEAD ----- 256 - HEAD -1
    256字节开始记录索引记录数据
*/

//索引页
public abstract class Page{    //一张表文件最多储存1MB  1**20位，页的偏移量要为4字节
    //行数据
    protected Table table;
    //页数据  （新页表给出）
    protected byte page_level;                     //该页的层级,判断是否为叶子页 0x00是叶子页，0xff是删除标志，剩余非叶子页  (1字节)
    protected byte page_space;                     //该页的大小 KB      ！（没做无符号最大只能是127  即64  默认16）
    protected int page_offset;                     //该页的偏移量
    //页数据  （新页默认值）
    protected short page_num;                      //该页中索引记录的数量  (用于处理 范围检索)  (也可以用来计算页已占空间)
    protected int page_spare;                      //空闲指针，相对地址
    protected int page_used;                       //末尾指针，用于读取时截取末尾,相对地址
    protected int page_min;                        //伪最小值指针，相对地址  用于初始化整页中的索引记录
    protected List<Integer> page_slots_offset;     //四十个槽的相对偏移量
    //  (不放入磁盘中)
    protected ByteBuffer page_buffer;              //维护一个该页在字节文件中的数组，致力于减少IO次数
    protected HashMap<Integer,Object> objectMap;   //维护一个索引值的缓冲表,减少反序列索引值的次数

    public static final int INDEX_RECORD_HEAD = 11;                      //索引记录头
    public static final int RECORD_HEAD = INDEX_RECORD_HEAD + 4;         //记录头
    public static final int INDEX_HEAD = INDEX_RECORD_HEAD + 4;          //索引头
    public static final int PAGE_HEAD = 256;                             //页头的字节长度
    public static final int MAX = PAGE_HEAD - INDEX_RECORD_HEAD;         //伪最大
    public static final int MIN = PAGE_HEAD - 2 * INDEX_RECORD_HEAD;     //伪最小

    //初始化，由表分配     (完整)
    public Page(byte page_level,byte page_space,int page_offset,Table table){
        this.table = table;              //表数据

        this.page_level = page_level;    //页层级
        this.page_space = page_space ;   //页大小
        this.page_offset = page_offset;  //页偏移量
        this.page_num = 0;               //页中数量默认为0
        this.page_spare = 256;           //空闲指针默认为256
        this.page_used = 256;            //末尾指针默认为256
        this.page_min = 0  ;             //伪最小值默认为256

        this.page_slots_offset = new ArrayList<>(); //槽地址
        this.page_buffer = ByteBuffer.allocate(PAGE_HEAD);
        this.objectMap = new HashMap<>();

        //初始化伪最大和伪最小
        initPage();
    }

    //初始化，反序列化
    public Page(byte page_level,byte page_space,int page_offset,short page_num,int page_spare,
                int page_used,int page_min,List<Integer> page_slots_offset,Table table){
        this.table = table;

        this.page_level = page_level;
        this.page_space = page_space;
        this.page_offset = page_offset;
        this.page_num = page_num;
        this.page_spare = page_spare;
        this.page_used = page_used;
        this.page_min = page_min;
        this.page_slots_offset = page_slots_offset;

        if(page_level < 0) return;
        //初始化缓冲数组
        this.page_buffer = ByteBuffer.wrap(BytesIO.readDataInto(page_used,page_offset,table.table_name));
        this.objectMap = new HashMap<>();
    }

    //伪最大值和伪最小值的初始化
    private void initPage(){
        //初始化
        page_buffer.position(MIN);
        page_buffer.put(initFakeIndexRecord(INDEX_RECORD_HEAD,(byte)0x02,0,MAX)); //伪最小
        page_buffer.position(MAX);
        page_buffer.put(initFakeIndexRecord(INDEX_RECORD_HEAD,(byte)0x03,MIN,0)); //伪最大
        //修改数值
        setOwned(MIN,(byte)1);
        setOwned(MAX,(byte)1);
        //插入槽数组中
        page_slots_offset.add(MIN);
        page_slots_offset.add(MAX);
    }

    /*
    //伪最大值和伪最小值的初始化  (用于反序列时)
    private void initPageDe(){
        byte[] data = new byte[IndexRecord.HEAD_LENGTH];
        page_buffer.position(256 - 2 * IndexRecord.HEAD_LENGTH);
        page_buffer.get(data);
        FakeIndexRecord min = (FakeIndexRecord) IndexRecord.deSerialize(data,this.table);
        page_buffer.get(data);
        FakeIndexRecord max = (FakeIndexRecord) IndexRecord.deSerialize(data,this.table);


    }
     */

    /*************************搜索***********************************/

    //二分搜索槽  返回刚好比索引值小一点的那个槽位  (不能相同)
    protected int slotsSearch(Object index_key){
        int min = 0,max = page_slots_offset.size()-1;
        int mid = (min + max)/2;
        int mid_offset = page_slots_offset.get(mid);
        while(min + 1 != max) {
            int flag = compare(index_key,mid_offset);

            if(flag > 0) min = mid;
            else  max = mid;

            mid = (min + max)/2;
            mid_offset = page_slots_offset.get(mid);
        }
        return mid;
    }

    //线性定位到索引所要插入位置的前一个索引  带查找开始的偏移量，为了补充二分
    protected int lineSearch(Object index_key,int begin_offset) {
        int prt_offset = begin_offset;
        int prev_offset = prt_offset;
        while(compare(index_key,prt_offset) > 0)        //循环直到prt索引大于等于index_key
        {
            prev_offset = prt_offset;                                 //prev指针一直在prt指针的前一位
            prt_offset = getNextOffset(prt_offset) ;              //将prt后移
        }
        return prev_offset;
    }

    //集成上面两种搜索   返回小于index_key的第一个节点偏移量
    public int Search(Object index_key){
        int slot_head_num = slotsSearch(index_key);
        int slot_head = page_slots_offset.get(slot_head_num);
        return lineSearch(index_key,slot_head);
    }

    /************************槽操作******************************/

    //通过索引偏移量查找槽头
    protected int slotNumSearch(int offset){
        if(offset == MIN){return 0;}
        else if(offset == MAX || getType(offset) == (byte)0x11){return this.page_slots_offset.size()-1;}
        else{
            Object index_key = getIndex_key(offset);
            int head_num = slotsSearch(index_key);
            //有可能offset是槽头
            if(page_slots_offset.get(head_num + 1) == offset) head_num++;
            return  head_num;
        }
    }

    //检查槽内数量是否达到分裂阈值     false代表未分裂  true代表已分裂
    protected boolean checkSlotSplit(int slot_head_num){
        int slot_head = this.page_slots_offset.get(slot_head_num);  //获取槽头偏移量
        int head_owned = getOwned(slot_head);
        if(head_owned < table.SlotSplit)
            return false;

        //重新计算一波槽内节点数量
        reCalculateSlotHeadOwned(slot_head_num);
        head_owned = getOwned(slot_head);
        if(head_owned < table.SlotSplit)
            return false;

        int mid = head_owned/2;
        int mid_n = mid;
        int prt = slot_head;               //移动指针  移动mid_n次
        while(mid_n-- > 0) {prt = getNextOffset(prt);}
        //prt指向要分裂的节点
        page_slots_offset.add(slot_head_num + 1,prt);          //插入一个新的槽头
        setOwned(prt,(byte)(head_owned - mid));             //新槽头的数量更新
        setOwned(slot_head,(byte)mid);                      //旧槽头的数量更新

        return true;
    }

    //重新计算槽头中包含的节点数量
    private void reCalculateSlotHeadOwned(int slot_head_num){
        int slot_head = this.page_slots_offset.get(slot_head_num);  //获取槽头偏移量
        int num = 1;
        int prt = getNextOffset(slot_head);
        //节点不为空且节点不是槽头
        while(prt != 0 && getOwned(prt) == 0){num++;prt = getNextOffset(prt);}
        //重新赋值
        setOwned(slot_head,(byte)num);
    }

    /*******************************插入*************************/

    /*
    //插入一个新的节点   在prev后面插入一整个节点字节数组    //用于页合并
    protected void insert(int prev,int slot_head_num,int offset,byte[] indexKey_bytes){

        page_buffer.position(offset);
        page_buffer.put(indexKey_bytes);
        //如果插入的是工具节点，不作槽操作
        if(getType(offset) != (byte)0x11)
        {
            setOwned(offset,(byte)0x00);
            addOwned(slot_head_num);
        }
        //前后指针改变
        int next = getNextOffset(prev);
        setNextOffset(offset,next);
        setPrevOffset(next,offset);
        setNextOffset(prev,offset);
        setPrevOffset(offset,prev);

    }
    */

    //重新规划整个page_buffer   用于重构页或者页分裂的新页
    public abstract void resetAllBuffer(byte[] data);

    /*
        请明确槽数量的作用！ 不必实时更新owned!
        槽内节点数量的作用仅仅是判断一个槽是否应该分裂的标准
        所以可以owned数大于实际数
        而不可以owned数小于实际数  否则分裂就会出现问题
        明确了这点，要求了在插入节点时必须严谨地更新owned
        而删除节点时，可以软删除owned
    */

    //空闲指针自增
    protected void spareAdd(){
        int add = getNodeLength();
        if(page_spare == page_used){ //如果空闲指针在最后
            page_spare += add;  page_used += add;
        }else{                                        //如果空闲指针不在字节数组最后
            int next_offset = getNextOffset(page_spare);
            if(next_offset == 0) page_spare = page_used;    //如果已经没有内部空闲空间
            else page_spare = next_offset;
        }
        //更新缓冲数组
        bufferExpand();

    }

    //根据page_used 一旦page_used增大就扩充 page_buffer缓冲数组
    protected void bufferExpand(){
        if(page_buffer.capacity() <= page_used){
            ByteBuffer buffer = ByteBuffer.allocate(page_used);
            //原缓冲数组的游标一定要在0
            page_buffer.position(0);
            buffer.put(page_buffer);
            this.page_buffer = buffer;
        }
    }

    /*
        在内存中修改缓冲数组的速度是纳秒级，而单次磁盘I/O可能需要毫秒级
        文件系统和磁盘硬件对批量写入有优化（如页缓存、块设备调度）。单次写入16KB的效率远高于多次写入小数据。
    */
    /******************************删除***********************************/

    //根据位置删除
    public void offsetDelete(int offset,int NoLeafPageModel){

        // model=2为硬删除  model=1为软删除  软删除只删除根
        if(offset == MAX || offset == MIN) throw new RuntimeException("逻辑错误");
        int next = getNextOffset(offset);
        int prev = getPrevOffset(offset);
        //删除到槽头的情况
        if(getOwned(offset) > 0 && getType(offset) != (byte)0x11 ){
            int slot_head_num = slotNumSearch(offset);                   //原槽位
            int prev_head = page_slots_offset.get(slot_head_num - 1);    //前一个槽头
            setOwned(prev_head,(byte)(getOwned(prev_head) + getOwned(offset)));  //两槽头合并
            page_slots_offset.remove(slot_head_num);                     //原槽位删除
            //节点删除
            nodeDelete(offset);
            setNextOffset(prev,next);
            setPrevOffset(next,prev);

            checkSlotSplit(slot_head_num - 1);              //前槽头检查分裂
        }
        //删到工具节点的情况
        else if(getType(offset) == (byte)0x11 && prev != Page.MIN){
            //普通删除
            nodeDelete(offset);
            setNextOffset(prev,next);
            setPrevOffset(next,prev);
            //让前一个节点成为新工具节点
            if(getOwned(prev) == 0) page_slots_offset.set(page_slots_offset.size() - 1,prev);
            else {page_slots_offset.remove(page_slots_offset.size() - 1);}
            setOwned(prev,(byte)2);
            setType(prev,(byte)0x11);
            setIndex_key_bytes(prev,new byte[]{0,0,0,0});

        }
        else{
            //普通删除
            nodeDelete(offset);
            setNextOffset(prev,next);
            setPrevOffset(next,prev);
        }


        //叶子节点的前后改变
        if(this instanceof PageNoLeaf p){
            int son = p.getLeftPage(offset);
            if(NoLeafPageModel == 2) {
                //硬删除
                table.clearRootPage(son);
            }else {
                //软删除
                table.adjustLeafPage(son);
                table.deletePageInSpace(table.getPage(son));
            }
        }
    }

    //范围删除,给出一个双闭区间的索引值,删除该页中在此双闭区间中的所有索引值
    public abstract int[] delete(Object index_key_begin,Object index_key_end);

    //范围删除,给出一个单区间的索引值,要求>=  或者<=该索引值的节点都要被删除
    public abstract int deleteOneSide(Object index_key, int model);

    //逻辑删除 ：删除节点、更新空闲链表、页中节点数减一
    protected void nodeDelete(int prt){
        //节点软删除
        page_buffer.position(prt);
        page_buffer.put((byte)0x01); //打上删除标志
        //空闲指针修改
        spareDelete(prt);
        //缓冲区删除
        objectMap.remove(prt);
        if(this instanceof PageLeaf pl){pl.valuesMap.remove(prt);}        //如果是叶子页还要删除值缓冲区
        //页数量减一
        page_num--;
    }

    //删除操作时的空闲指针   新指针插入在空闲指针后面的第一个指针
    protected void spareDelete(int offset){
        if(page_used == page_spare)  //空闲指针与page_used重叠
        {
            page_spare = offset;
            setNextOffset(offset, page_used);
        }
        else{
            int spare_next = getNextOffset(page_spare);  //空闲指针的下一个指针
            setNextOffset(offset,spare_next);            //新指针的下一个指针改变
            setNextOffset(page_spare,offset);            //空闲指针指向新指针
        }
    }

    /******************************页合并*****************************************/

    //检查当前页是否需要合并
    public boolean checkPageMerge(){
        if(page_num < 2 && this != table.getRoot() && this.page_level != (byte)0xff)
            return true;
        else
            return false;
    }

    //本页与page页进行合并 page页被删除
    //public abstract void merge(Page page,int model);

    /******************************分裂和页优化***********************************/

    /*
        空闲指针维护的问题：
            目前空闲链表维护较为简单，即直接在空闲头指针后面添加新删除的节点偏移量，不考虑物理上的连续
            另外一种是: 根据新删除的节点的物理偏移量去插入空闲指针，这种每删除一次就要遍历一次，资源开销较大，但在重整页结构时有着较好的性能

            包括在删除操作时，是不轻易去改变page_used值的，实现起来开销也挺大，要判断page_used上一个长度的字节数组数据是否被删除
          但其实是没有必要的：因为只要是触发了页分裂，就说明空闲链表一定是填充满的了，不存在空闲空间
    */
    //页优化 重整整页的物理空间 处理掉所有的碎片空间 使得 page_used == page_spare（不保证逻辑上连续）
    private void sortingMyPage(){
        if(page_used == page_spare) return;   //无意义的重整
        byte[] new_page_bytes = new byte[0];  //新数组
        //遍历页中所有记录
        int prt = getNextOffset(page_slots_offset.get(0));
        while(getType(prt) != (byte)0x03){
            byte[] bytes = getBytes(prt);
            new_page_bytes = ByteTools.concatBytes(new_page_bytes,bytes);
            prt = getNextOffset(prt);
        }
        //重新创建一个缓冲数组
        resetAllBuffer(new_page_bytes);
        //缓冲索引清空
        objectMap.clear();
    }

    //检查当前页是否达到分裂阈值
    public boolean checkPageSplit(){
        int percent = (page_used * 100)/(((int)page_space) * Table.KB);
        if(page_slots_offset.size() <= 2)  return false;
        if(this instanceof PageLeaf && page_num >=5)
            return true;

        if(this instanceof PageNoLeaf && page_num >= 5)
            return true;
        return  false;
    }

    /*
    protected void PageSplit(PageNoLeaf parent,int offset){

        //对半分裂
        int n = page_num/2;
        int prt = getNextOffset(page_slots_offset.get(0));    //本页的第一个节点
        byte[] this_page_bytes = new byte[0];
        //拼接所有的本页的前一半节点
        while(n-- > 0){
            this_page_bytes = ByteTools.concatBytes(this_page_bytes,getBytes(prt));
            prt = getNextOffset(prt);
        } //现在的prt还未拼接

        //创建一个新的页
        Page rightPage = table.insertPage(this.page_level);
        //向父页中插入新节点
        parent.insert(offset,getIndex_key_bytes(prt),this.page_offset,rightPage.page_offset);
        //检查本页是否为非叶子页
        if(this instanceof PageNoLeaf){
            setType(prt,(byte)0x11);    //将中间节点转化为辅助节点插入本页新更新的缓冲数组中
            this_page_bytes = ByteTools.concatBytes(this_page_bytes,getBytes(prt));
            prt = getNextOffset(prt);
        }
        //拼接本页的后一半节点
        byte[] new_page_bytes = new byte[0];
        while(prt != MAX){
            new_page_bytes = ByteTools.concatBytes(new_page_bytes,getBytes(prt));          //将本节点的字节数组连接
            prt = getNextOffset(prt);
        }
        //将本页中移出的数据插入新页中
        rightPage.resetAllBuffer(new_page_bytes);

        if(this instanceof PageLeaf){
            //需要更新原叶子页后面的那个页
            if(((PageLeaf)this).page_next_offset != 0){
                PageLeaf next = (PageLeaf) table.getPage(((PageLeaf)this).page_next_offset);
                next.page_prev_offset = rightPage.page_offset;
                ((PageLeaf)rightPage).page_next_offset = next.page_offset;
            }
            ((PageLeaf)this).page_next_offset = rightPage.page_offset;
            ((PageLeaf)rightPage).page_prev_offset = this.page_offset;

        }
        //如果本页是表的根页  那么将根页转交给父页
        if(table.getRoot() == this) table.setRoot(parent);
        //更新本页缓冲数组
        resetAllBuffer(this_page_bytes);
    }
    */


    /******************************序列页头*******************************/


    //完全序列化页头
    protected abstract void serializeHead();

    //序列化页头
    protected void serializeSameHead(){
        page_buffer.position(0);
        //页层级
        page_buffer.put(page_level);
        //页空间
        page_buffer.put(page_space);
        //页偏移量
        page_buffer.putInt(page_offset);
        //页索引记录数量
        page_buffer.putShort(page_num);
        //页空闲指针
        page_buffer.put(ByteTools.intTo3Bytes(page_spare));
        //页末尾指针
        page_buffer.put(ByteTools.intTo3Bytes(page_used));
        //伪最小值指针
        page_buffer.put(ByteTools.intTo3Bytes(page_min));

        page_buffer.position(64);
        for (Integer i : page_slots_offset) {
            page_buffer.put(ByteTools.intTo3Bytes(i));
        }


    }

    //根据页头反序列化页   (不完全)   :传递的数组必须是长度为256字节的页头
    public static Page deSerialize(byte[] data,Table table){
        Page instance;
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte page_level ;
        byte page_space ;
        int page_offset ;
        short page_num  ;
        int page_min    ;
        int page_spare  ;
        int page_used   ;
        List<Integer> page_slots_offset = new ArrayList<>();

        //辅助数组
        byte[] threeBytes = new byte[3];

        //页层级
        page_level = buffer.get();
        //页大小
        page_space = buffer.get();
        //页偏移量
        page_offset = buffer.getInt();
        //页数量
        page_num = buffer.getShort();
        //页空闲指针
        buffer.get(threeBytes);
        page_spare = ByteTools.threeBytesToInt(threeBytes);
        //页末尾指针
        buffer.get(threeBytes);
        page_used = ByteTools.threeBytesToInt(threeBytes);
        //页伪最小值指针
        buffer.get(threeBytes);
        page_min = ByteTools.threeBytesToInt(threeBytes);

        //槽数组
        buffer.position(64);
        buffer.get(threeBytes);
        int slot_offset = ByteTools.threeBytesToInt(threeBytes);
        while(slot_offset > 0){
            page_slots_offset.add(slot_offset);
            buffer.get(threeBytes);
            slot_offset = ByteTools.threeBytesToInt(threeBytes);
        }

        if(page_level == (byte) 0x00){
            int page_next_offset;
            int page_prev_offset;

            //重新定位到
            buffer.position(21);
            page_next_offset = buffer.getInt();
            page_prev_offset = buffer.getInt();
            instance = new PageLeaf(page_level,page_space,page_offset,page_num,page_spare,page_used,page_min,
                    page_slots_offset,table,page_next_offset,page_prev_offset);

        }  //叶子页
        else if(page_level > (byte)0x00){
            instance = new PageNoLeaf(page_level,page_space,page_offset,page_num,page_spare,page_used,page_min,
                    page_slots_offset,table);

        }  //非叶子页
        else throw new RuntimeException("反序列的页已经被删除");

        return instance;
    }

    //写入磁盘
    public void serializePageOut(){
        this.serializeHead();  //序列化页头
        BytesIO.writeDataOut(page_buffer.array(),page_offset,table.table_name);
    }
    /******************************索引记录的数组操作**********************/


    //索引记录头中相同的数据偏移量
    public static final int NEXT_OFFSET = 1;
    public static final int PREV_OFFSET = 5;
    public static final int REC_TYPE    = 9;
    public static final int OWNED       = 10;

    //--------------------------//

    //offset是相对页的偏移量
    //表头序列     由页分配本记录的长度、偏移量、下一条记录的地址、以及该索引记录的类型
    public void help_indexRecord(ByteBuffer buffer,byte rec_type,int prev_offset,int next_offset){
        buffer.position(0);   //从头开始写
        //删除标志为0
        buffer.put((byte)0x00);
        //写入下一条记录的相对偏移量
        buffer.putInt(next_offset);
        //写入上一条的相对偏移量
        buffer.putInt(prev_offset);
        //记录类型
        buffer.put(rec_type);
        //槽的数量  默认为0
        buffer.put((byte)0x00);

    }

    //行记录
    public byte[] initRecord(int length,byte rec_type,int prev_offset,int next_offset,int heap_no,
                                    byte[] index_key,byte[] values) {
        ByteBuffer buffer = ByteBuffer.allocate(length);
        //相同表头写入
        help_indexRecord(buffer,rec_type,prev_offset,next_offset);
        //记录独特标识
        buffer.putInt(heap_no);
        //索引值
        buffer.put(index_key);
        //索引行数据
        buffer.put(values);
        //返回
        return buffer.array();
    }

    //索引
    public byte[] initIndexKey(int length,byte rec_type,int prev_offset,int next_offset,
                                      int leftPage_offset, byte[] index_key){
        ByteBuffer buffer = ByteBuffer.allocate(length);
        //相同表头写入
        help_indexRecord(buffer,rec_type,prev_offset,next_offset);
        //左页偏移量
        buffer.putInt(leftPage_offset);
        //索引值
        buffer.put(index_key);
        //返回
        return buffer.array();
    }

    //伪最大最小   length一定就是记录头  11
    public byte[] initFakeIndexRecord(int length,byte rec_type,int prev_offset,int next_offset){
        ByteBuffer buffer = ByteBuffer.allocate(length);
        //直接就是这个
        help_indexRecord(buffer,rec_type,prev_offset,next_offset);
        return buffer.array();
    }

    //获取本节点的字节数组长度
    protected int getNodeLength(){
        int getLength;
        if(page_level == (byte)0x00){getLength = table.getRecord_maxLength();}
        else if(page_level > (byte)0x00){getLength = table.getIndex_length();}
        else throw new RuntimeException("无法检测的page_level");
        return getLength;
    }

    //获取本节点的字节数组
    public byte[] getBytes(int offset){
        byte[] bytes = new byte[getNodeLength()];
        page_buffer.position(offset);
        page_buffer.get(bytes);
        return bytes;
    }

    //获取本节点是否被删除
    protected boolean isDelete(int offset){
        page_buffer.position(offset);
        if(page_buffer.get() == (byte)0x01)
            return true;
        else if(page_buffer.get() == (byte)0x00)
            return false;
        else throw new RuntimeException("无法识别的删除标准");
    }

    //--------------------------//

    //修改下一条记录的偏移量
    protected void setNextOffset(int offset,int next_offset){
        //游标到要修改数据的下方
        page_buffer.position(offset + NEXT_OFFSET);
        page_buffer.putInt(next_offset);
    }

    //获取下一条记录的数据
    public int getNextOffset(int offset){
        //游标到要获取数据的下方
        page_buffer.position(offset + NEXT_OFFSET);
        return page_buffer.getInt();
    }

    //修改本节点的偏移量
    protected void setPrevOffset(int offset,int prev_offset){
        //游标到要修改数据的下方
        page_buffer.position(offset + PREV_OFFSET);
        page_buffer.putInt(prev_offset);
    }

    /*TODO*/
    //获取上一条记录的数据
    public int getPrevOffset(int offset){
        //游标到要获取数据的下方
        page_buffer.position(offset + PREV_OFFSET);
        return page_buffer.getInt();
    }
    //--------------------------//

    //修改槽中数量
    public void setOwned(int offset,byte owned_n){
        //游标到要修改数据的下方
        page_buffer.position(offset + OWNED);
        page_buffer.put(owned_n);
    }

    //获取槽中数量
    public byte getOwned(int offset){
        //游标到要获取数据的下方
        page_buffer.position(offset + OWNED);
        return page_buffer.get();
    }

    //槽头加一
    public void addOwned(int offset){
        byte owned = getOwned(offset);
        if(owned == 0) throw new RuntimeException("非槽头操作");
        setOwned(offset,(byte)(owned + 1));
    }

    //--------------------------//

    /*
    //比较两索引记录的大小     (还未进化)   //非叶子页中的辅助节点不能使用  会出错
    protected int compare(int a_offset,int b_offset){
        final byte MIN = (byte)0x02;
        final byte MAX = (byte)0x03;
        final byte MAX_I = (byte) 0x11;
        //伪最大最小
        byte a_type = getType(a_offset);
        byte b_type = getType(b_offset);
        if(a_type == MIN || b_type == MAX || b_type == MAX_I) return -1;
        if(a_type == MAX || a_type == MAX_I || b_type == MIN) return 1;
        //普通节点
        Object a = getIndex_key(a_offset);
        Object b = getIndex_key(b_offset);
        //强转
        if(a instanceof Integer)
            return (int)a - (int)b;
        else if(a instanceof Float){
            float f = (float)a - (float)b;
            if(f > 0)     return 1;
            else if(f <0) return -1;
            else          return 0;
        }else if(a instanceof String)
            return a.toString().compareTo(b.toString());
        else throw new RuntimeException("无法进行比较的类型");
    }

     */

    //已知一个索引的情况下，比较与另一个索引的大小
    public int compare(Object index_key,int b_offset){
        final byte MIN = (byte)0x02;
        final byte MAX = (byte)0x03;
        final byte MAX_I = (byte) 0x11;
        if(getType(b_offset) == MIN) return 1;
        if(getType(b_offset) == MAX || getType(b_offset) == MAX_I)return -1;
        //普通节点
        Object b = getIndex_key(b_offset);
        //强转
        if(index_key instanceof Integer)
            return (int)index_key - (int)b;
        else if(index_key instanceof Float){
            float f = (float)index_key - (float)b;
            if(f > 0)     return 1;
            else if(f <0) return -1;
            else          return 0;
        }else if(index_key instanceof String)
            return index_key.toString().compareTo(b.toString());
        else throw new RuntimeException("无法进行比较的类型");
    }

    //获取索引值
    public Object getIndex_key(int offset){
        //查看缓冲池是否有
        Object a = objectMap.get(offset);
        if(a!=null) return a;
        //获取类型
        byte rec_type = getType(offset);
        //移动到索引位置
        if(rec_type == (byte)0x00)page_buffer.position(offset + RECORD_HEAD);
        else if(rec_type == (byte)0x01)page_buffer.position(offset + INDEX_HEAD);
        else throw new RuntimeException("除了0x01和0x02外，其他都不能获取索引值");
        //反序列索引值
        short length = 0;
        if(table.getPrimaryKey() != null){length = table.getFieldLength(table.getPrimaryKey());}
        a = ByteTools.deSerializeSingleObject(page_buffer,length);
        //插入缓冲池中
        objectMap.put(offset,a);
        return a;
    }

    //获取索引值字节数组
    public byte[] getIndex_key_bytes(int offset){
        //获取类型
        byte rec_type = getType(offset);
        //移动到索引位置
        if(rec_type == (byte)0x00)page_buffer.position(offset + RECORD_HEAD);
        else if(rec_type == (byte)0x01)page_buffer.position(offset + INDEX_HEAD);
        else throw new RuntimeException("除了0x01和0x02外，其他都不能获取索引值");
        //获取索引键值的长度  =  索引节点字节长度 - 索引节点头字节长度
        byte[] bytes = new byte[table.getIndex_length() - INDEX_HEAD];
        page_buffer.get(bytes);
        return bytes;
    }

    //修改索引值字节数组
    public void setIndex_key_bytes(int offset,byte[] bytes){
        //缓冲池删除
        objectMap.remove(offset);
        //获取类型
        byte rec_type = getType(offset);
        //移动到索引位置
        if(rec_type == (byte)0x00)page_buffer.position(offset + RECORD_HEAD);
        else if(rec_type == (byte)0x01 || rec_type == (byte)0x11)page_buffer.position(offset + INDEX_HEAD);
        else throw new RuntimeException("除了0x00和0x01和0x11外，其他都不能修改索引值");
        //将索引值数组放入
        page_buffer.put(bytes);

    }

    //--------------------------//

    //获取索引记录的类型
    public byte getType(int offset){
        //游标到要获取数据的下方
        page_buffer.position(offset + REC_TYPE);
        return page_buffer.get();
    }

    //修改索引记录的类型
    public void setType(int offset,byte type){
        //游标到要修改数据的下方
        page_buffer.position(offset + REC_TYPE);
        page_buffer.put(type);
    }

    //--------------------------//

    /******************************************************************/
    public byte getPage_level() {return page_level;}
    public byte getPage_space() {return page_space;}
    public int getPage_offset() {return page_offset;}
    public short getPage_num() {return page_num;}
    public int getPage_spare() {return page_spare;}
    public int getPage_used() {return page_used;}
    public int getPage_min() {return page_min;}
    public List<Integer> getPage_slots_offset(){return page_slots_offset;}
    public void setPage_num(int num){this.page_num = (short) num;}
    //返回本页的所有数据集合
    public LinkedHashMap<String,Object> getPage_all(){
        LinkedHashMap<String,Object> map = new LinkedHashMap<>();
        map.put("page_level",page_level);
        map.put("page_space",page_space + "B");
        map.put("page_offset",page_offset);
        map.put("page_num",page_num);
        map.put("page_spare",page_spare);
        map.put("page_used",page_used);
        for (int i = 0; i < page_slots_offset.size(); i++) {
            map.put("slot" + i,page_slots_offset.get(i));
        }
        if(this instanceof PageLeaf){
            map.put("page_prev_offset",((PageLeaf)this).page_prev_offset);
            map.put("page_next_offset",((PageLeaf)this).page_next_offset);
        }
        return map;
    }

    //返回本页的节点的所有字段
    public abstract String[] getNodeProperties();

    //(0)返回本页的节点的所有字段的基础信息
    protected Object[] getNodeAllBasic(int prt){
        Object[] objects = new Object[7];
        if(getType(prt) == (byte)0x11) objects[0] = null;
        else objects[0] = getIndex_key(prt);
        objects[1] = 0;
        objects[2] = prt;
        objects[3] = getType(prt);
        objects[4] = getOwned(prt);
        objects[5] = getPrevOffset(prt);
        objects[6] = getNextOffset(prt);
        return  objects;
    }

    //(1)返回本页的节点的所有字段的信息
    protected abstract Object[] getNodeAll(int prt);

    //(2)返回所有节点的信息
    public Object[][] getAllNodeData(){
        Object[][] result = new Object[page_num][];
        int prt = getNextOffset(MIN);
        int i = 0;
        while(i<page_num){
            result[i++] = getNodeAll(prt);
            prt = getNextOffset(prt);
        }
        return result;
    }


}

