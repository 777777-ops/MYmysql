package Memory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

//非叶子页节点
public class PageNoLeaf extends Page{

    //初始化非叶子页，创建一个新页时一定会有一个新的索引  和一个伴随的左页 (未检查)
    public PageNoLeaf(byte page_level,byte page_space,int page_offset,Table table){
        super(page_level,page_space,page_offset,table);
        initPageNoLeaf();
    }

    //初始化非叶子页，反序列
    public PageNoLeaf(byte page_level, byte page_space, int page_offset, short page_num, int page_spare, int page_used,
                      int page_min,List<Integer> page_slots_offset,Table table){
        super(page_level,page_space,page_offset,page_num,page_spare,page_used,page_min,page_slots_offset,table);
    }

    //初始化工具节点
    private void initPageNoLeaf(){
        //插入辅助索引
        int offset = page_spare; //插入位置为空闲指针
        spareAdd(); //空闲指针自增并且扩大缓冲数组
        page_buffer.position(offset);
        page_buffer.put(initIndexKey(table.getIndex_length(),(byte)0x11,MIN,MAX,
                0,new byte[0]));
        //修改槽：将末尾槽头改为工具节点  而不是MAX
        setOwned(MAX,(byte)0);
        setOwned(offset,(byte)2);
        page_slots_offset.set(1,offset);

        //前后指针改变
        setNextOffset(MIN,offset);
        setPrevOffset(MAX,offset);
        //工具节点也算一个节点
        page_num++;

    }

    /***************************查询*****************************************/

    //给出一个本页的子页，查询该子页的所在的节点的前节点
    protected int findPrevNode(Page son){
        int son_offset = son.page_offset;
        int prt;          //parent中寻找prev的游标
        if(son.page_num == 0){
            //没有索引键值，只能从头找到尾
            prt = Page.MIN;
        }else {
            prt = Search(son.getIndex_key(son.getNextOffset(Page.MIN)));
        }
        int next = getNextOffset(prt);
        while(getLeftPage(next) != son_offset && next != MAX){
            prt = next;
            next = getNextOffset(next);
        }
        if(next == MAX)
            return 0;  //没找到
        else
            return prt;
    }

    /***************************插入*****************************************/

    //重新规划整个page_buffer   用于重构页或者页分裂的新页
    public void resetAllBuffer(byte[] data){
        objectMap.clear();
        page_num = 0;                                 //重构页中节点数
        this.page_slots_offset = new ArrayList<>();   //重构槽的偏移量数组
        page_spare = PAGE_HEAD + data.length;         //重构空闲指针
        page_used = PAGE_HEAD + data.length;          //重构末尾指针
        page_slots_offset.add(MIN);

        //截取页头数组
        page_buffer.position(0);
        byte[] page_head_bytes = new byte[PAGE_HEAD];
        page_buffer.get(page_head_bytes);
        //创建一个新的字节数组缓冲池
        page_buffer = ByteBuffer.allocate(PAGE_HEAD + data.length);
        page_buffer.put(page_head_bytes); page_buffer.put(data);

        //开始遍历插入数组
        int slot_head = MIN;   int head_num = 1;  int max_num = table.SlotSplit ;
        int pos = PAGE_HEAD;   int node_length = getNodeLength();
        while(pos < page_used){
            setNextOffset(pos,pos + node_length);   //将节点的下一个节点指针指向物理逻辑上的下一个指针
            setPrevOffset(pos,pos - node_length);                               //测试易观察
            if(head_num < max_num){head_num ++; setOwned(pos,(byte)0);}    //槽头还没满
            else{                                                          //槽头已满 -> 记录槽头值  更新槽头至下一个槽头
                setOwned(slot_head,(byte)head_num);                        //分配上一个槽头  本槽头要等下一个槽头分配
                head_num = 1; slot_head = pos;
                page_slots_offset.add(slot_head);
            }
            page_num++;
            pos += node_length;

        }//此时pos == page_used

        if(getType(slot_head) != (byte)0x11) {
            setOwned(slot_head, (byte)(head_num - 1));                 //分配最后一个槽头
            pos -= node_length;
            page_slots_offset.add(pos);                             //槽头数组新增一个工具节点
            setOwned(pos,(byte)2);                                  //将工具节点槽数改为2
        }
        else {
            pos -= node_length;
            setOwned(pos,(byte)2);
        }
        //此时的pos是工具节点
        if(getType(pos) != (byte)0x11)  throw new RuntimeException("逻辑错误");

        setNextOffset(MIN,PAGE_HEAD);                  //伪最小指向页头末尾
        setPrevOffset(PAGE_HEAD,MIN);
        setNextOffset(pos,MAX);                        //将工具节点指向伪最大值
        setPrevOffset(MAX,pos);
        setIndex_key_bytes(pos,new byte[]{0,0,0,0});

        //deSerializeIndexRecords();

    }

    //插入一个新的索引节点
    public void insert(int next,byte[] index_key_bytes,int leftPage_offset,int rightPage_offset) {
        int prev = getPrevOffset(next);
        int slot_head_num = slotNumSearch(prev);       //找出槽头位置
        int slot_head = page_slots_offset.get(slot_head_num);
        setOwned(slot_head,(byte)(getOwned(slot_head)+1));  //槽头自增一

        int offset = page_spare;                   //新节点的偏移量就是空闲指针
        spareAdd();                                //空闲指针自增
        setNextOffset(prev,offset);                //前节点的后节点为本节点
        setPrevOffset(next,offset);                //后节点的前节点为本节点

        setLeftPage(next,rightPage_offset);        //后节点的左页改为要插入的右页
        //插入！
        page_buffer.position(offset);
        page_buffer.put(
                initIndexKey(table.getIndex_length(),(byte)0x01,prev,next,leftPage_offset,index_key_bytes)
        );
        page_num ++;
        checkSlotSplit(slot_head_num);            //检查更新槽头是否需要分裂
    }

    //页合并特殊方法!  慎重
    public void insert(int prev,byte[] node_bytes){
        int next = getNextOffset(prev);
        //获取空闲指针
        int offset = page_spare;
        spareAdd();
        //插入
        page_buffer.position(offset);
        page_buffer.put(node_bytes);
        //槽全为0
        setOwned(offset,(byte)0);
        setType(offset,(byte)0x01);
        //改变前后指针
        setNextOffset(offset,next);
        setPrevOffset(offset,prev);
        setPrevOffset(next,offset);
        setNextOffset(prev,offset);
        page_num ++;
    }


    /****************************删除节点********************************/


    //范围删除,给出一个双闭区间的索引值,删除该页中在此双闭区间中的所有索引值  delete_begin和delete_end分别是(、)本身不被删除
    public int[] delete(Object index_key_begin,Object index_key_end){
        int prt = Search(index_key_begin);     //所要删除节点的前一个节点偏移量
        int delete_begin = getNextOffset(prt);    //获取prt的下一个偏移量   即第一个需要删除的节点
        if(compare(index_key_begin,delete_begin) == 0)  delete_begin = getNextOffset(delete_begin);

        prt  = delete_begin;
        while(compare(index_key_end,prt) >= 0) //prt遍历直到索引值不等于要删除的索引值 [)
        {
            int next_offset = getNextOffset(prt);  //保存下一条记录的指针   因为空闲指针可能会改变原先的下一条指针
            //非叶子页删除特有的     当删除的头尾相同
            if(prt == delete_begin) {prt = next_offset;continue;}
            //删除节点、更新空闲链表   (这也代表着一个节点的删除)
            offsetDelete(prt,2);
            //遍历置下一个节点
            prt = next_offset;
        }
        //此时prt到达一个index_key>index_key_end的节点
        int delete_end = prt;
        return new int[]{getLeftPage(delete_begin),getLeftPage(delete_end)};
    }

    //范围删除,给出一个单区间的索引值,要求>=  或者<=该索引值的节点都要被删除
    public int deleteOneSide(Object index_key,int model){
        int prt = Search(index_key);    //所要删除节点的前一个节点偏移量
        int result;
        //>=    delete_begin 是(
        if(model == 1){
            int delete_begin = getNextOffset(prt);              //获取prt的下一个偏移量
            if(compare(index_key,delete_begin) == 0)  delete_begin = getNextOffset(delete_begin);

            result = getLeftPage(delete_begin);
            prt = getNextOffset(delete_begin);      //辅助指针移动到第一个删除节点  的后面
            while(getType(prt)!=0x03){
                int next_offset = getNextOffset(prt);
                //删除节点、更新空闲链表   (这也代表着一个节点的删除)
                offsetDelete(prt,2);
                prt = next_offset;           //下移
            }
            if(getType(prt)!=(byte)0x03)  throw new RuntimeException("逻辑错误");
            return result;
        }
        //<=    delete_end是)
        else if(model == 2){
            while(compare(index_key,prt)>=0)  prt = getNextOffset(prt);   //prt这时 > index_dex
            int delete_end = prt;
            result = getLeftPage(delete_end);
            prt = getNextOffset(MIN);  //prt为本页的第一个节点
            while(prt != delete_end){
                int next_offset = getNextOffset(prt);
                //删除节点、更新空闲链表   (这也代表着一个节点的删除)
                offsetDelete(prt,2);
                prt = next_offset;           //下移
            }
            return result;
        }
        else throw new RuntimeException("错误的model");


    }

    /****************************合并*************************************/

    /*
    //该页吞并page页  model 1代表该页在左  2代表该页在右
    public void merge(Page page,int model){
        if(page.page_level != this.page_level)  throw new RuntimeException("逻辑错误");
        if(page.page_num == 0)  return;      //page页的节点数量为0，不做任何处理
        //右顺序插入
        if(model == 1){
            //第一步：先将本页中的工具节点改为普通节点    //槽数组末尾留到最后更新
            int last = page_slots_offset.get(page_slots_offset.size() - 1);             //新插入节点的物理空间
            setType(last,(byte)0x01);
            setOwned(last,(byte)0x00);
            int slot_head_num = page_slots_offset.size() - 2; addOwned(slot_head_num);   //槽头加一
            checkSlotSplit(slot_head_num);                                             //检查更新
            //第二步：将合并的页中的节点插入至本页中的末尾
            int page_prt = page.getNextOffset(MIN);
            while(page.page_num > 0){
                int offset = page_spare;  spareAdd();                                        //确定新插入的物理位置
                insert(last,slot_head_num,offset,page.getBytes(page_prt));                   //插入
                if(checkSlotSplit(slot_head_num)) slot_head_num++;                           //如果槽分裂了，要移动到下一个槽位

                page_prt = page.getNextOffset(page_prt);                                     //遍历至下一个page节点
                last = offset;                                                               //更新last
                page_num ++;     page.page_num --;
            }
            //第三步，完善旧工具节点
            if(getType(last) != (byte)0x11)  throw new RuntimeException("逻辑错误");
            int tool = page_slots_offset.get(page_slots_offset.size() - 1);
            findIndex(tool);                          //旧索引节点是没有索引键值的，需要重新搜索
            page_slots_offset.set(page_slots_offset.size() - 1,last);

        }
        //左顺序插入
        else if(model == 2){
            //第一步：将page的所有节点插入进本页
            int last = MIN;
            int slot_head_num = 0;
            int page_prt = page.getNextOffset(MIN);
            while(page.page_num > 0){
                int offset = page_spare;    spareAdd();     //新节点插入的物理位置
                insert(last,slot_head_num,offset,page.getBytes(page_prt));                   //插入
                if(checkSlotSplit(slot_head_num)) slot_head_num++;                           //如果槽分裂了，要移动到下一个槽位

                page_prt = page.getNextOffset(page_prt);                                     //遍历至下一个page节点
                last = offset;                                                               //更新last
                page_num ++;     page.page_num --;
            }
            //第二步：完善last  此时last是新工具节点
            if(getType(last) != (byte)0x11)  throw new RuntimeException("逻辑错误");
            addOwned(slot_head_num);                                 //因为insert不对工具节点进行槽操作
            setType(last,(byte)0x01);
            setOwned(last,(byte)0);
            findIndex(last);                                          //因为这是工具节点 没有索引值，要手动搜索

        }else throw new RuntimeException("错误的model");
    }

    //寻找该页中某一节点的索引值
    protected void findIndex(int offset){
        int next = getNextOffset(offset);
        //如果下一个节点就是伪最大节点  那么说明本节点就是工具节点
        if(next == MAX) {
            setType(offset,(byte)0x11);
            setIndex_key_bytes(offset,new byte[]{0,0,0,0});
            return;
        }
        Page rightPage = table.getPage(getLeftPage(next));

        Stack<byte[]> stack = new Stack<>();   //栈中储存着可能利用到的索引
        //只要右页不是叶子页 就一直递推到叶子页
        while(true){
            //储存本层可能利用的索引
            int first = rightPage.getNextOffset(MIN);
            if(first == MAX) break;
            if(rightPage.getType(first) != (byte)0x11) {
                stack.push(rightPage.getIndex_key_bytes(first));
            }
            //如果到叶子层 不用下层
            if(rightPage.page_level == (byte)0x00) break;
            //到下一层
            PageNoLeaf page = (PageNoLeaf)rightPage;
            rightPage = table.getPage(page.getLeftPage(first));
        }
        //为空说明该节点的右页没有一层可以利用索引
        if(stack.isEmpty()){
            /*
            if(getType(next) == (byte)0x11) offsetDelete(next);
            else {setIndex_key_bytes(offset,getIndex_key_bytes(next));offsetDelete(next);}

            if(getType(next) == (byte)0x11)  {
                setType(offset,(byte) 0x11);  //为了不破坏查找功能 只要键值为空的都是0x11
                setIndex_key_bytes(offset,new byte[]{0,0,0,0});
            }
            else setIndex_key_bytes(offset,getIndex_key_bytes(next));

        }else setIndex_key_bytes(offset,stack.pop());
    }

     */


    /*********************************序列*******************************************/

    //完全序列化页头      (非叶子页目前没有其他属性)
    protected void serializeHead(){
        page_buffer.position(0);
        serializeSameHead();
    }

    /******************************索引记录的数组操作**********************/

    public static final int LEFT_PAGE_OFFSET = 11;

    //--------------//

    //获取左页信息
    public int getLeftPage(int offset){
        //游标定位到左页信息的下方
        page_buffer.position(offset + LEFT_PAGE_OFFSET);
        return page_buffer.getInt();
    }

    //修改左页信息
    private void setLeftPage(int offset,int leftPage_offset){
        //游标定位到左页信息的下方
        page_buffer.position(offset + LEFT_PAGE_OFFSET);
        page_buffer.putInt(leftPage_offset);
    }

    //--------------//

    //字段名字
    public String[] getNodeProperties(){
        return new String[]{"index_key","delete_flag","offset","rec_type","n_owned","prev_node_offset","next_node_offset","leftPage"};
    }

    //字段信息
    protected Object[] getNodeAll(int prt){
        Object[] result = new Object[7 + 1];
        int i = 0;
        for (Object object : getNodeAllBasic(prt))
            result[i++] = object;
        result[i] = getLeftPage(prt);
        return result;
    }

    /***********************************************************/

    //测试点：插入索引记录，以及槽分组。
    public static void main(String[] args) {

    }
}
