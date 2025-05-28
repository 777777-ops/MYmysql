package Memory;

import UI.TableFrame;

import java.nio.ByteBuffer;
import java.util.*;

//叶子页
public class PageLeaf extends Page{
    public int page_prev_offset;                            //上页的偏移量
    public int page_next_offset;                            //下页的偏移量
    protected HashMap<Integer,Object[]>  valuesMap = new HashMap<>();       //该页中数据的缓冲池


    //初始化叶子页 ：由表给出数据  （完善）
    public PageLeaf(byte page_level,byte page_space,int page_offset,Table table)    {
        super(page_level,page_space,page_offset,table);
        this.page_next_offset = 0;
        this.page_prev_offset = 0;   //初始化为0

    }

    //初始化叶子页  反序列  未完整反序列
    public PageLeaf(byte page_level, byte page_space, int page_offset, short page_num, int page_spare, int page_used,
                    int page_min, List<Integer> page_slots_offset, Table table, int page_next_offset, int page_prev_offset) {
        super(page_level,page_space,page_offset,page_num,page_spare,page_used,page_min,page_slots_offset,table);
        this.page_prev_offset = page_prev_offset;
        this.page_next_offset = page_next_offset;

    }

    /********************************查询************************************/

    //查看索引是否在本页中
    public boolean contain(Object index_key){
        int prt = Search(index_key);
        prt = getNextOffset(prt);
        if(prt == MAX)  return false;
        return getIndex_key(prt) == index_key;
    }

    //查询该记录的信息  (通过主键)
    public Table.SearchResult searchValues(Object index_key){
        int prev = Search(index_key);
        int prt = getNextOffset(prev);
        if(prt == MAX)  return null;
        else return new Table.SearchResult(this.page_offset,prt,getValues(prt));
    }

    /**********************************插入*********************************/

    //插入
    public void insert(Object[] values,int heap_no,Object index_key){
        int offset = page_spare;   //本记录的偏移量就是空闲指针
        spareAdd();                //空闲指针增加,更新缓冲数组大小
        int next; int prev;
        //-----逻辑插入----//

        int slot_head_num = slotsSearch(index_key);       //二分找出槽头
        int slot_head = page_slots_offset.get(slot_head_num);
        setOwned(slot_head,(byte)(getOwned(slot_head)+1));  //槽头自增一

        prev = lineSearch(index_key,slot_head);    //所要插入的节点的前节点
        next = getNextOffset(prev);                 //本节点继承prt的下一个节点偏移量
        setNextOffset(prev,offset);                //prt的下一个节点偏移量就是本节点
        setPrevOffset(next,offset);
        page_num++;


        //序列索引值
        byte[] index_bytes;
        if(table.getPrimaryKey() == null) index_bytes = ByteTools.serializeSingleObject(index_key,Integer.class,(short)0);
        else  //有主键
        {
            String primaryName = table.getPrimaryKey();
            index_key = values[table.getFieldIndex(primaryName)];  //获取主键
            Class<?> type = table.getFieldType(primaryName);       //主键类型
            short length = table.getFieldLength(primaryName);      //主键长度
            index_bytes = ByteTools.serializeSingleObject(index_key,type,length);
        }
        objectMap.put(offset,index_key);  //顺便进入一下缓冲池

        //行数据序列
        int index = 0;
        byte[] values_bytes = new byte[0];
        for (Table.TableColumn tableColumn : table.getFieldsProperty()) {             //字段名
            Class<?> type = tableColumn.type;
            short length = tableColumn.length;
            Object obj = values[index++];                     //字段相应的数据
            byte[] b2 = ByteTools.serializeSingleObject(obj, type,length);//根据字段类型、数据生成的二进制数据
            values_bytes = ByteTools.concatBytes(values_bytes, b2);             //将二进制数据都拼接起来
        }

        //插入！
        page_buffer.position(offset);
        page_buffer.put(
                initRecord(table.getRecord_maxLength(),(byte)0x00,prev,next,heap_no,
                        index_bytes,values_bytes)
        );

        checkSlotSplit(slot_head_num);            //检查更新槽头是否需要分裂
    }

    //重新规划整个page_buffer   用于重构页或者页分裂的新页   //所插入的数组是逻辑物理上连续的  //要重新分配槽
    public void resetAllBuffer(byte[] data){
        valuesMap.clear();
        objectMap.clear();                            //双缓冲池清空
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

        //开始遍历插入数组   重新分配槽
        int slot_head = MIN;   int head_num = 1;  int max_num = table.SlotSplit - 1;
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
        }
        setOwned(slot_head,(byte)head_num);                                //分配最后一个槽头
        page_slots_offset.add(MAX);                    //槽头数组新增一个伪最大

        pos -= node_length;
        setNextOffset(pos,MAX);                        //将最后一个节点指向伪最大值
        setPrevOffset(MAX,pos);
        setNextOffset(MIN,PAGE_HEAD);                  //伪最小指向页头末尾
        setPrevOffset(PAGE_HEAD,MIN);
    }

    /**********************************合并**********************************/

    /*
    //本页与page页进行合并 page页被删除
    public void merge(Page page,int model){
        if(page.page_level != this.page_level)  throw new RuntimeException("逻辑错误");
        if(page.page_num == 0)  return;      //page页的节点数量为0，不做任何处理

        int last;   int slot_head_num;
        //右顺序插入
        if(model == 1){
            //找到本页的最后一个节点
            int prt = page_slots_offset.get(page_slots_offset.size() - 2);
            int prt_next = getNextOffset(prt);
            while(prt_next != MAX){
                prt = prt_next;
                prt_next = getNextOffset(prt_next);
            }
            last = prt;
            slot_head_num = page_slots_offset.size() -2;
        }
        //左顺序插入
        else if(model == 2){
            last = MIN;
            slot_head_num = 0;
        }else throw new RuntimeException("错误的model");

        int page_prt = page.getNextOffset(MIN);
        while(page.page_num > 0)
        {
            int offset = page_spare;  spareAdd();
            insert(last,slot_head_num,offset,page.getBytes(page_prt));
            if(checkSlotSplit(slot_head_num)) slot_head_num++;

            page_prt = page.getNextOffset(page_prt);                                     //遍历至下一个page节点
            last = offset;                                                               //更新last
            page_num ++;     page.page_num --;
        }
    }
    */

    /**********************************删除**********************************/

    /*
        delete中的delete_begin是第一次需要删除的节点
        deleteOneSide中的delete_begin是需要删除的节点集前一个节点
    */

    //范围删除,给出一个双闭区间的索引值,删除该页中在此双闭区间中的所有索引值
    public int[] delete(Object index_key_begin,Object index_key_end){

        int delete_begin = Search(index_key_begin);    //所要删除节点的前一个节点偏移量
        int prt = getNextOffset(delete_begin);    //获取prt的下一个偏移量   即第一个需要删除的节点
        while(compare(index_key_end,prt) >= 0) //prt遍历直到索引值不等于要删除的索引值 [)
        {
            //保存下一条记录的指针   因为空闲指针可能会改变原先的下一条指针
            int next_offset = getNextOffset(prt);
            //删除节点、更新空闲链表   (这也代表着一个节点的删除)
            offsetDelete(prt,2);
            //遍历置下一个节点
            prt = next_offset;
        }
        if(prt == delete_begin) {throw new RuntimeException("无法找到删除节点的索引值");}   /*TODO*///异常不抛出
        return new int[0];
    }

    //范围删除,给出一个单区间的索引值,要求>=  或者<=该索引值的节点都要被删除
    public int deleteOneSide(Object index_key,int model){
        int prt = Search(index_key);    //所要删除节点的前一个节点偏移量
        //>=    delete_begin 是(
        if(model == 1) {
            int delete_begin = prt;   //delete_begin不作删除
            prt = getNextOffset(delete_begin);
            while(getType(prt)!=0x03){
                int next_offset = getNextOffset(prt);
                //删除节点、更新空闲链表   (这也代表着一个节点的删除)
                offsetDelete(prt,2);
                prt = next_offset;           //下移
            }
            if(getType(prt)!=(byte)0x03)  throw new RuntimeException("逻辑错误");
            return 0;
        }
        //<=    delete_end是)
        else if(model == 2){
            while(compare(index_key,prt)>=0)  prt = getNextOffset(prt);   //prt这时 > index_dex
            int delete_end = prt;
            prt = getNextOffset(MIN);  //prt为本页的第一个节点
            while(prt != delete_end){
                int next_offset = getNextOffset(prt);
                //删除节点、更新空闲链表
                offsetDelete(prt,2);
                prt = next_offset;           //下移
            }
            return 0;
        }
        else throw new RuntimeException("错误的model");
    }


    /************************************序列******************************/

    //完全序列化页头
    protected void serializeHead(){
        serializeSameHead();
        page_buffer.position(21);
        page_buffer.putInt(page_next_offset);
        page_buffer.putInt(page_prev_offset);

    }

    /******************************索引记录的数组操作**********************/

    //获取记录的全部信息
    public Object[] getValues(int offset){
        int BEGIN_OFFSET = table.getIndex_length() - INDEX_HEAD + RECORD_HEAD;
        Object[] values = valuesMap.get(offset);
        if(values == null){
            //游标到记录数据字节的开头
            page_buffer.position(offset + BEGIN_OFFSET);
            values = deSerializeData(page_buffer,table); //反序列数据
            valuesMap.put(offset,values);                //保存到缓冲池中
        }
        return values;
    }

    //部分反序列化记录数据
    private Object[] deSerializeData(ByteBuffer buffer, Table table) {
        int Head = buffer.position();             //数据字节数组的头下标
        //获取字段数据
        Collection<Table.TableColumn> collection = table.getFieldsProperty();
        Object[] values = new Object[collection.size()];
        int index = 0;
        for (Table.TableColumn tableColumn : collection) {
            buffer.position(tableColumn.offset + Head);
            short length = tableColumn.length;
            values[index] = ByteTools.deSerializeSingleObject(buffer,length);
            index ++;
        }
        return values;
    }

    /****************************************记录的字节数组操作*********************************/

    public static final int HEAP_NO = 11;

    //获取heap_no
    public int getHeapNo(int prt){
        page_buffer.position(prt + HEAP_NO);
        return page_buffer.getInt();
    }

    //节点的属性
    public String[] getNodeProperties(){
        String[] fieldNames = table.getFieldNamesArr();
        String[] strings = new String[8 + fieldNames.length];
        strings[0] = "index_key";
        strings[1] = "delete_flag";
        strings[2] = "offset";
        strings[3] = "rec_type";
        strings[4] = "n_owned";
        strings[5] = "prev_node_offset";
        strings[6] = "next_node_offset";
        strings[7] = "heap_no";
        int index = 8;
        for (String fieldName : fieldNames) {
            strings[index++] = fieldName;
        }
        return strings;
    }

    //所有信息
    protected Object[] getNodeAll(int prt){
        Object[] result = new Object[7 + 1 + table.getFieldNamesArr().length];
        int i = 0;
        for (Object object : getNodeAllBasic(prt))
            result[i++] = object;
        result[i++] = getHeapNo(prt);
        for (Object value : getValues(prt))
            result[i++] = value;
        return result;
    }

    /**************************************测试点*********************************/

    /*
    public static void main1(String[] args) {
        //创建一个table对象
        LinkedHashMap<String, Table.TableColumn> columnHashMap = new LinkedHashMap<>();
        columnHashMap.put("年龄", new Table.TableColumn(Integer.class,(short) 0,false,true,false));
        Table t0 = new Table("t0",columnHashMap,"年龄");
        columnHashMap = null;


        t0.insertRec(170);
        t0.insertRec(180);
        t0.insertRec(190);
        t0.insertRec(200);
        t0.insertRec(210);
        t0.deSerializeAllPage();

        t0.insertRec(220);
        t0.deSerializeAllPage();

        t0.insertRec(230);
        t0.deSerializeAllPage();
        t0.insertRec(240);
        t0.deSerializeAllPage();
        t0.insertRec(250);
        t0.deSerializeAllPage();
        t0.insertRec(260);
        t0.deSerializeAllPage();
        t0.insertRec(270);
        t0.deSerializeAllPage();


        t0.insertRec(280);
        t0.deSerializeAllPage();
        t0.insertRec(290);
        t0.deSerializeAllPage();
        t0.insertRec(300);
        t0.deSerializeAllPage();
        t0.insertRec(310);
        t0.deSerializeAllPage();
        t0.insertRec(320);
        t0.deSerializeAllPage();

        t0.insertRec(10);
        t0.deSerializeAllPage();
        t0.insertRec(30);
        t0.deSerializeAllPage();
        t0.insertRec(20);
        t0.deSerializeAllPage();
        t0.insertRec(201);
        t0.insertRec(202);
        t0.deSerializeAllPage();


        t0.delete(10,20);
        t0.deSerializeAllPage();
        t0.deSerializeAllPage();


        t0.close();

    }

    public static void main(String[] args) {
        //创建一个table对象
        LinkedHashMap<String, Table.TableColumn> columnHashMap = new LinkedHashMap<>();
        columnHashMap.put("年龄", new Table.TableColumn(Integer.class,(short) 0,false,true,false));
        Table t0 = new Table("t0",columnHashMap,"年龄");
        columnHashMap = null;

        new TableFrame(t0);
        /*
        t0.insertRec(42);
        t0.insertRec(178);
        t0.insertRec(15);
        t0.insertRec(93);
        t0.insertRec(127);
        t0.insertRec(64);
        t0.insertRec(199);
        t0.insertRec(7);
        t0.insertRec(156);
        t0.insertRec(81);
        t0.insertRec(33);
        t0.insertRec(112);
        t0.insertRec(145);
        t0.insertRec(28);
        t0.insertRec(167);
        t0.insertRec(59);
        t0.insertRec(104);
        t0.insertRec(192);
        t0.insertRec(3);
        t0.insertRec(76);
        t0.insertRec(138);
        t0.insertRec(21);
        t0.insertRec(187);
        t0.insertRec(51);
        t0.insertRec(116);
        t0.insertRec(89);
        t0.insertRec(134);
        t0.insertRec(12);
        t0.insertRec(163);
        t0.insertRec(97);

    }

    public static void main3(String[] args) {
        LinkedHashMap<String, Table.TableColumn> columnHashMap = new LinkedHashMap<>();
        columnHashMap.put("名字", new Table.TableColumn(String.class,(short) 5,false,true,false));
        Table t0 = new Table("t0",columnHashMap,"名字");
        columnHashMap = null;

        t0.insertRec("你好");
        t0.getRoot().deSerializeIndexRecords();
        t0.insertRec("世界");
        t0.insertRec("编程");
        t0.insertRec("学习");
        t0.insertRec("代码");
        t0.insertRec("数据");
        t0.insertRec("结构");
        t0.insertRec("算法");
        t0.insertRec("开发");
        t0.insertRec("测试");
        t0.insertRec("软件");
        t0.insertRec("系统");
        t0.insertRec("网络");
        t0.insertRec("安全");
        t0.insertRec("人工智能");
        t0.insertRec("大数据");
        t0.insertRec("云计算");
        t0.insertRec("物联网");
        t0.insertRec("区块链");
        t0.insertRec("前端");
        t0.insertRec("后端");
        t0.insertRec("移动");
        t0.insertRec("设计");
        t0.insertRec("产品");
        t0.insertRec("运营");
        t0.insertRec("用户");
        t0.insertRec("体验");
        t0.insertRec("项目");
        t0.insertRec("团队");
        t0.insertRec("技术");

        new TableFrame(t0);
    }
    */

    public static void main(String[] args) {

        LinkedHashMap<String, Table.TableColumn> columnHashMap = new LinkedHashMap<>();
        columnHashMap.put("名字", new Table.TableColumn(String.class,(short) 5,false,true,false));
        columnHashMap.put("年龄", new Table.TableColumn(Integer.class,(short) 0,false,false,false));
        columnHashMap.put("工资", new Table.TableColumn(Float.class,(short) 0,false,false,false));
        Table t0 = new Table("t0",columnHashMap,"名字");

        new TableFrame(t0);
    }
}
