package Memory;
import DataIO.BytesIO;
import Memory.Cache.CacheStrategy;
import Memory.Cache.LRUCacheStrategy;
import Memory.Event.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class Table {
    public final static int KB = 1024;

    /*
        table_name:初始化新表给出
        field     :初始化新表给出
        primaryKey:初始化新表给出

        intToString         : 初始化表时生成      (不进入磁盘)
        record_maxLength    : 初始化新表时生成    (进入磁盘)

        root                :     新表都有默认值
        default_key         :     新表都有默认值
        last_offset         :     新表都有默认值
    */
    //表信息
    public String table_name;
    private LinkedHashMap<String,TableColumn> fields = new LinkedHashMap<>();  //根据字段名字寻找字段信息   字段集
    private Page root;                           //该表的根页
    private final String primaryKey;                   //主键的名字  //主键如果没有就是null  //索引记录中的的index_key永远不为null
    private int default_key;                     //如果没有主键，默认的主键
    private int table_used;                      //表文件大小           单位B
    private final int record_maxLength;                //记录字节的总占用长度（用于检测溢出）

    //不用写入文件中的变量
    public final HashSet<String> indexSet = new HashSet<>();            //该表中已创建的索引集合
    public final HashMap<Integer,Object> deleteMap = new HashMap<>();   //删除日志  Integer是页地址 object是主键 在表进入磁盘前要处理
    private final CacheStrategy cache = new LRUCacheStrategy(100,this);    //页缓冲池
    public final EventBus eventBus = new EventBus();
    private final int index_length;                    //主键字节长度(包括索引记录头的)

    //表可改变的全局变量  (写入)
    public int RecordLengthAdd;                  //行数据字节可拓展的字节数   (默认64)
    public int SlotSplit;                        //页中槽分裂的阈值          (默认5)
    public int PageLeafSpace;                    //叶子页大小       单位KB   (默认16)   (未实现)
    public int PageNoLeafSpace;                  //非叶子页大小              (默认2)    (未实现)
    public int PageSplit;                        //页分裂阈值       单位%    (默认70)   (未实现)


    //字段信息的内部类
    public static class TableColumn{
        public Class<?> type;           //字段类型
        public short length;            //如果是字符串，所规定的最大长度  //再长也不能超过100
        public boolean couldNull;       //能否为null
        public boolean isPrimary;       //是否为主键
        public boolean couldRepeated;   //能否被重复

        public short offset;            //字段所在的字节位置
        public int index;               //字段在数据数组的下标  //不放入磁盘  //在字段生成时设置

        //初始化 字符串 无约束
        public TableColumn(Class<?> type,short length) {
            this.type = type;this.length = length;
            couldNull = true; isPrimary = false; couldRepeated = true;   //默认值
        }

        //初始化 有约束
        public TableColumn(Class<?> type,short length
                ,boolean couldNull,boolean isPrimary,boolean isRepeated){
            this.type = type;this.length = length;
            this.couldNull = couldNull; this.isPrimary = isPrimary; this.couldRepeated = isRepeated;
        }

        //初始化 根据字节标识设置约束  用于反序列
        public TableColumn(Class<?> type,short length,byte constraint,short offset) {
            couldRepeated = false; isPrimary = false; couldRepeated = false;
            int sum = (int)constraint;
            if(sum >= 4){couldRepeated = true; sum -= 4;}
            if(sum >= 2){isPrimary = true; sum -= 2;}
            if(sum >= 1){couldNull = true; sum -= 1;}
            this.type = type;
            this.length = length;
            this.offset = offset;
        }

        //返回该字段属性对应的字节标识
        public byte getFieldBooleanByte(){
            int a1,a2,a3; a1 = a2 = a3 =0;
            if(couldNull) a1 = 1;
            if(isPrimary) a2 = 1;
            if(couldRepeated) a3 = 1;
            int sum = a1  + a2 * 2 + a3 * 4;
            return (byte)sum;
        }
    }

    //该类用于页分裂和页合并 用于存储量变量，变量一是页地址，变量二是在该页中的某个节点
    public static class Pair{
        public int page_offset;
        public int node_offset;
        public Pair(int page_offset,int node_offset)
        {
            this.page_offset = page_offset;
            this.node_offset = node_offset;
        }
    }

    //创建一张新表       //要求给出一个包含字段名字和字段信息的hashmap表
    public Table(String tableName,LinkedHashMap<String,TableColumn> fields,String primaryName){
        //新表可改变的全局变量
        RecordLengthAdd = 64;
        SlotSplit = 5;
        PageLeafSpace = 16;
        PageNoLeafSpace = 2;
        PageSplit = 10;       //(测试采用10 )
        //新表默认值
        this.table_used =  2 * KB;
        this.default_key = 0;
        this.root = insertPage((byte)0x00);
        //新表创建时必须给出的变量
        this.table_name = tableName;
        this.fields = fields;
        this.primaryKey = primaryName;
        //根据给出的变量初始化的数据
        initColumnOffset();         //给每个字段都添加上偏移量 和 数组索引
        record_maxLength = getFieldsLength() + calculateIndexLength() + Page.RECORD_HEAD + RecordLengthAdd;
        index_length = calculateIndexLength() + Page.INDEX_HEAD;
        if(primaryName != null) indexSet.add(primaryName);

        register();

    }

    //构造函数：反序列一张表
    public Table(String tableName,String primaryName,int root_offset,int table_used,int default_key,int record_maxLength,
                 int RecordLengthAdd,int SlotSplit,int PageSpilt,
                 LinkedHashMap<String,TableColumn> fields,HashMap<Integer,Byte> spareList){
        this.primaryKey = primaryName;
        this.default_key = default_key;
        this.table_name = tableName;
        this.record_maxLength = record_maxLength;
        this.table_used = table_used;

        //系统变量
        this.RecordLengthAdd = RecordLengthAdd;
        this.SlotSplit = SlotSplit;
        this.PageSplit = PageSpilt;
        PageLeafSpace = 16;
        PageNoLeafSpace = 2;

        this.fields = fields;
        this.spareList = spareList;
        //根页
        if(root_offset == 0) root = null;
        else root = getPage(root_offset);
        //根据给出的变量初始化的数据
        index_length = calculateIndexLength() + Page.INDEX_HEAD;
        initValueIndex();
        if(primaryName != null ) indexSet.add(primaryName);

        register();
    }

    //注册事件
    public void register(){
        eventBus.registerHandler(new CurdEventHandler.selectWayHandler(this));
        eventBus.registerHandler(new CurdEventHandler.insertHandler(this));
        eventBus.registerHandler(new CurdEventHandler.selectHandler(this));
        eventBus.registerHandler(new CurdEventHandler.deleteHandler(this));
        eventBus.registerHandler(new PageManagerHandler.SpiltHandler(this));
        eventBus.registerHandler(new PageManagerHandler.MergeHandler(this));
    }


    /****************************字段**********************************/

    //插入字段   要找到字段字节数组中可被插入的物理位置
    public void insertColumn(String name,TableColumn column){
        //第一步：查重

        if(fields.containsKey(name))  throw new RuntimeException("存在相同命名的字段");   //字段名是否重复
        if(primaryKey != null && column.isPrimary) throw new RuntimeException("表中已有主键");
        else if(primaryKey == null &&column.isPrimary){throw new RuntimeException("暂不直接表重构（主键更改）");}  /*TODO*/
        //第二步：查找是否有可以使字段字节数据存放的空间
        int b;
        int[] arr = new int[getFieldsLength()];
        int columnLength = ByteTools.singleObjectLength(column.type,column.length);
        //两次遍历arr  第一次标记  第二次找位置
        for (TableColumn value : fields.values()) {
            int index = value.offset;
            int byteLength = ByteTools.singleObjectLength(value.type,value.length);
            //将已经有数据的字节位置首尾打上标记
            arr[index] = index + byteLength - 1;
            arr[index + byteLength - 1] = -1;
        }
        //第二次：找位置   //维护两个指针计算p1是头 p2是尾f
        int p1 = 0;
        int head = p1;     //结果
        while(true){
            while(p1 < arr.length && arr[p1] > 0){
                p1 = arr[p1] + 1;
            }
            int length = 0;
            head = p1;
            while(p1 < arr.length && arr[p1] == 0){
                p1++; length++;
            }
            if(length >= columnLength || p1 == arr.length){break;}
        }
        //检查是否要触发页溢出 或 页重整
        if(head + columnLength + calculateIndexLength() + Page.RECORD_HEAD > this.record_maxLength){
            /*TODO*/
            throw new RuntimeException("页字段字节溢出了,暂不处理该细节");
        }

        //第三步：插入
        column.offset = (short)head;
        column.index = fields.size();
        fields.put(name,column);
    }

    //表创建:初始化字段集：给每个字段都添加上偏移量   用于创表初期 以及 字段整理碎片化
    private void initColumnOffset(){
        int index = 0;
        short last_offset = 0;
        for (TableColumn value : fields.values()) {
            value.offset = last_offset;                                             //设置偏移量
            value.index = index++;
            last_offset += ByteTools.singleObjectLength(value.type,value.length);   //自增
        }
    }

    //给每个字段分配数组索引
    private void initValueIndex(){
        int index = 0;
        for (TableColumn column : fields.values()) {
            column.index = index++;
        }
    }

    //返回字段集合在字节数组中的总长度
    public int getFieldsLength(){
        int last_offset = 0;
        for (TableColumn value : fields.values()) {
            int offset = value.offset + ByteTools.singleObjectLength(value.type,value.length);
            if(offset > last_offset)
                last_offset = offset;
        }
        return last_offset;
    }

    /*************************空间管理********************************/

    private HashMap<Integer,Byte> spareList = new HashMap<>();   //表的空闲空间

    //空间上删除一个页
    protected void deletePageInSpace(Page page){
        page.page_level = -1;
        if(!spareList.containsKey(page.page_offset)) {
            spareList.put(page.page_offset,page.page_space);
            cache.popPage(page.page_offset);
        }
    }

    //清扫一个根下面的所有页  包括根    写个遍历
    public void clearRootPage(int pageRoot_offset){
        Page page = getPage(pageRoot_offset);
        int prt = page.getNextOffset(Page.MIN);
        Stack<Pair> stack = new Stack<>();
        while(true){

            //上去一层
            while(prt == Page.MAX && !stack.isEmpty()){
                deletePageInSpace(page);        //结束该页
                Pair pair = stack.pop();
                page = getPage(pair.page_offset);
                prt = page.getNextOffset(pair.node_offset);
            }
            //遍历结束
            if(prt == Page.MAX && stack.isEmpty()){deletePageInSpace(page); break;}
            //遍历到最底层
            while(page instanceof PageNoLeaf p){
                stack.push(new Pair(page.page_offset,prt));   //记录该层
                page = getPage(p.getLeftPage(prt));
                prt = page.getNextOffset(Page.MIN);
            }
            //叶子页 （最底层）
            if(page.page_level == 0){
                adjustLeafPage(page.page_offset);   //修改所删除页的前后节点
                prt = Page.MAX;
                deletePageInSpace(page);
            }
        }
    }

    //创建一个新页
    public Page insertPage(byte level){

        Page instance;
        int offset = findSpareSpace(level);
        if(level == (byte)0x00){
            instance = new PageLeaf(level,(byte) PageLeafSpace,offset,this);
        } else if(level > (byte)0x00){
            instance = new PageNoLeaf(level,(byte)PageNoLeafSpace,offset,this);
        }else {throw new RuntimeException("非法page_levee");}
        //缓冲池更新
        cache.pushPage(instance);
        return instance;
    }

    //找到表中可插入的富余空间
    private int findSpareSpace(byte level){
        int space;
        if(level == 0) space = PageLeafSpace;
        else space = PageNoLeafSpace;

        //空闲列表搜索
        for (Map.Entry<Integer, Byte> set : spareList.entrySet()) {
            if(space == set.getValue()){
                int offset = set.getKey();
                spareList.remove(offset);
                return offset;
            }
        }
        //空闲列表没有就返回表末尾
        int a = table_used;
        table_used += space * KB;
        return a;
    }

    /****************************查询********************************/

    //查询有三种  一是主键的B+查询  二是二次索引的双B+查询  三是顺序查询
    //查询的辅助结构
    public static class SearchResult{
        public int page_offset;          //页偏移量
        public int offset;            //节点的偏移量
        public Object[] values;          //节点数据
        public SearchResult(int page_offset,int offset,Object[] values){
            this.page_offset = page_offset; this.offset = offset; this.values = values;
        }
    }

    /****************************插入*************************************/

    //外接
    public void insert(List<String> strings){eventBus.execute( /*TODO*/
            new CurdEvent.insertEvent(turnStringsToObjects(strings)));}

    //外接
    public void insert(String... strings){insert(Arrays.asList(strings));}


    //----插入检查且转换-------//

    //根据字段集的属性检查每一个插入的字段行数据是否可行  给Frame外接
    private Object[] turnStringsToObjects(List<String> strings) {
        if(strings.size() != fields.size())  throw new RuntimeException("字段个数不符合");
        //插入记录的Object
        Object[] values = new Object[strings.size()];

        int index = 0;
        for (Map.Entry<String, TableColumn> map : fields.entrySet()) {
            String str = strings.get(index);
            Object object = checkObject(map.getKey(),str);
            values[index] = object;
            index++;
        }
        return values;

    }

    //根据字段集属性检查单个字段的数据是否符合该字段的属性
    public Object checkObject(String field,String str){
        Class<?> type = getFieldType(field);
        Object object;
        if (type == Integer.class){
            try {
                object = Integer.parseInt(str); // 这会抛出异常
            } catch (NumberFormatException e) {
                throw new RuntimeException(field + "为整数int,但" + str + "不是整数int"); /*TODO*/
            }
        }else if(type == String.class){
            char first = str.charAt(0);
            if(first == '\"' || first == '\'') {
                object = str.substring(1,str.length() - 1);
            }
            else throw new RuntimeException(field + "为字符串String,但" + str + "不是字符串String");
        }else if(type == Float.class){
            try {
                object = Float.parseFloat(str); // 这会抛出异常
            } catch (NumberFormatException e) {
                throw new RuntimeException(field + "为浮点型float,但" + str + "不是浮点型float");
            }
        }else if(type == Boolean.class){
            if(str.equalsIgnoreCase("TRUE")) object = true;
            else if(str.equalsIgnoreCase("FALSE")) object = false;
            else throw new RuntimeException(field + "为布尔型boolean,但" + str + "不是布尔型boolean");
        }else throw new RuntimeException("非法字段类型");
        return object;
    }

    /****************************删除********************************/

    //删除语句
    public void delete(String conditions){
        eventBus.execute(new CurdEvent.deleteEvent(conditions));
    }


    //调整某一叶子页的前后顺序  (叶子页的删除)
    protected void adjustLeafPage(int page_offset){
        Page page = getPage(page_offset);
        if(page instanceof PageLeaf leaf) {
            if(leaf.page_next_offset != 0) {
                PageLeaf next_page = (PageLeaf) getPage(leaf.page_next_offset);
                next_page.page_prev_offset = leaf.page_prev_offset;
            }
            if(leaf.page_prev_offset != 0) {
                PageLeaf prev_page = (PageLeaf) getPage(leaf.page_prev_offset);
                prev_page.page_next_offset = leaf.page_next_offset;
            }
        }
    }

    /****************************页合并*********************************/


    /****************************页缓冲池*******************************/

    //根据页地址返回页
    public Page getPage(int page_offset) {
        return cache.getPage(page_offset);
    }

    //关闭该表   即序列化缓冲池中所有的表
    public void close(){
        //检查删除日志中是否有需要合并的
        eventBus.execute(new PageManagerEvent.MergeEvent());
        //关闭缓冲池中的所有的页
        cache.close();
        //写入
        writeTable();
    }


    /****************************逻辑操作*******************************/

    //计算一个索引键值占用的字节数
    private int calculateIndexLength(){
        int primaryLength;
        //索引值的长度
        if(this.primaryKey == null){primaryLength = 5;}
        else{
            TableColumn tableColumn = fields.get(primaryKey);
            primaryLength = ByteTools.singleObjectLength(tableColumn.type,tableColumn.length);
        }
        return primaryLength;
    }

    /****************************序列***********************************/

    //序列化该表的信息
    /*
        String:  表名
        String: 主键名字
        int: 根页
        int: 表大小
        int: 默认主键
        int: 行数据总占用的字节长度
        int: 行数据扩张预留的字节长度
        int: 页中槽分裂阈值
        int: 页分裂阈值
        512-1023: 字段数据
        1024-2047: 空闲空间

     */
    //序列化该表的信息
    public byte[] serializeTable(){
        ByteBuffer buffer = ByteBuffer.allocate(2*KB);
        byte[] bytes;
        buffer.position(0);
        //写入表名
        bytes = table_name.getBytes(StandardCharsets.UTF_8);
        buffer.put((byte)bytes.length);   //字符串字节的长度
        buffer.put(bytes);                //字符串
        //写入主键名
        if(primaryKey == null)
            buffer.put((byte)0);
        else {
            bytes = primaryKey.getBytes(StandardCharsets.UTF_8);
            buffer.put((byte) bytes.length);
            buffer.put(bytes);
        }
        //根页
        if(root == null)  buffer.putInt(0);
        else buffer.putInt(root.page_offset);
        //表大小
        buffer.putInt(table_used);
        //默认主键
        buffer.putInt(default_key);
        //行数据总占用的字节长度，包括（扩张的）
        buffer.putInt(record_maxLength);

        //---系统变量---//

        //行数据扩张预留的字节长度
        buffer.putInt(RecordLengthAdd);
        //页中槽分裂阈值
        buffer.putInt(SlotSplit);
        //页分裂阈值
        buffer.putInt(PageSplit);
        //字段数据
        buffer.position(512);
        for (Map.Entry<String, TableColumn> entry : fields.entrySet()) {
            bytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            buffer.put((byte)bytes.length);
            buffer.put(bytes);
            TableColumn column = entry.getValue();
            //字段属性
            buffer.putShort(column.offset);                            //字段在字节数组中的位置
            buffer.put(ByteTools.typeToByte(column.type));             //将字段类型转为相应的字节
            buffer.put((byte) column.length);                          //将字段类型长度转为单字节
            buffer.put(column.getFieldBooleanByte());                  //将字段的约束转为相应的字节
        }
        //空闲空间
        buffer.position(KB);
        for (Map.Entry<Integer, Byte> entry : spareList.entrySet()) {
            buffer.putInt(entry.getKey());
            buffer.put(entry.getValue());
        }
        return buffer.array();
    }

    //反序列化创建一个新的表
    public static Table deSerializeTable(byte[] table_data){
        ByteBuffer buffer = ByteBuffer.wrap(table_data);
        Table table ;
        String table_name;   //表名
        String primaryKey;   //主键名
        int root_offset;   int table_used;
        int default_key;     //默认主键
        int record_maxLength; //行数据字节占用长度
        LinkedHashMap<String,TableColumn> fields = new LinkedHashMap<>(); //字段属性
        HashMap<Integer,Byte> spareList = new HashMap<>();
        int str_length;    //字符串长度
        byte[] bytes;      //用来读取字符串的字节数组
        int RecordLengthAdd;   //行数据扩张预留的字节长度
        int SlotSpilt;  int PageSpilt;

        //表名
        buffer.position(0);
        str_length = buffer.get();
        bytes = new byte[str_length];
        buffer.get(bytes);
        table_name = new String(bytes,StandardCharsets.UTF_8);
        //主键名
        str_length = buffer.get();
        if(str_length == 0) primaryKey = null;
        else{
            bytes = new byte[str_length];
            buffer.get(bytes);
            primaryKey = new String(bytes,StandardCharsets.UTF_8);
        }

        root_offset = buffer.getInt();    //根页
        table_used =  buffer.getInt();    //表大小
        default_key = buffer.getInt();    //默认主键
        record_maxLength= buffer.getInt();//行数据扩张预留的字节长度
        //系统变量
        RecordLengthAdd = buffer.getInt();
        SlotSpilt = buffer.getInt();
        PageSpilt = buffer.getInt();
        //字段属性
        buffer.position(512);
        str_length = buffer.get();
        while(str_length != 0)
        {
            String field_name;
            TableColumn tableColumn;
            Class<?> type;   //字段类型
            short length;      //字段类型长度
            short offset;
            byte constraint; //字段约束
            //字段名
            bytes = new byte[str_length];
            buffer.get(bytes);
            field_name = new String(bytes,StandardCharsets.UTF_8);
            //字段在字节数组中的位置
            offset = buffer.getShort();
            //字段类型
            type = ByteTools.byteToType(buffer.get());
            //字段类型长度
            length =buffer.get();
            //字段约束
            constraint = buffer.get();
            tableColumn = new TableColumn(type,length,constraint,offset);
            fields.put(field_name,tableColumn);
            //继续循环
            str_length = buffer.get();
        }
        //空闲空间
        buffer.position(KB);
        while (true){
            int key = buffer.getInt();  byte value = buffer.get();
            if(key == 0) break;
            spareList.put(key,value);
        }

        table = new Table(table_name,primaryKey,root_offset,table_used,default_key,record_maxLength,
                RecordLengthAdd,SlotSpilt,PageSpilt,
                fields,spareList);
        return table;
    }

    //将该表的信息写入文件中
    public void writeTable(){BytesIO.writeDataOut(this.serializeTable(),0,this.table_name);}


    /**************************获取*********************************/

    //返回字段集
    public LinkedHashMap<String,TableColumn> getFields(){return this.fields;}
    //返回字段名字数组
    public String[] getFieldNamesArr(){
        return fields.keySet().toArray(new String[0]);
    }
    //返回字段的数据类型
    public Class<?> getFieldType (String name){
        return fields.get(name).type;
    }
    //返回字段在数据数组中的位置
    public int getFieldIndex(String name){return fields.get(name).index;}
    //返回主键名字
    public String getPrimaryKey(){return this.primaryKey;}
    //返回该表的根页
    public Page getRoot(){return this.root;}
    //返回该表中的记录占用的总字节长度
    public int getRecord_maxLength(){return this.record_maxLength;}
    //返回该表的主键索引的字节长度
    public int getIndex_length(){return this.index_length;}
    //返回该表中已创建索引的字段集合
    public HashSet<String> getIndexSet(){return  this.indexSet;}
    //返回表大小
    public int getUsed(){return this.table_used;}
    //返回表空闲空间
    public String getSpare(){
        StringBuilder str = new StringBuilder();
        for (Integer i : spareList.keySet())
            str.append(i).append(" ");
        return str.toString();
    }
    //返回默认主键 并且加一
    public int getDefault_key(int addNum){int nowKey = this.default_key; this.default_key += addNum; return nowKey;}
    //-------------------//

    //返回字段属性的迭代器
    protected Collection<TableColumn> getFieldsProperty(){return fields.values();}
    //返回字段的数据长度
    protected short getFieldLength(String name){return fields.get(name).length;}
    //修改根页
    public void setRoot(Page page){this.root = page;}







    /**********************************************************/

    //测试点：表数据写入读出二进制文件
    public static void main(String[] args) {
        LinkedHashMap<String, Table.TableColumn> columnHashMap = new LinkedHashMap<>();
        columnHashMap.put("年龄", new Table.TableColumn(int.class,(short) 0,false,true,false));
        columnHashMap.put("是否参加医保", new Table.TableColumn(boolean.class,(short) 0));
        columnHashMap.put("支付额度", new Table.TableColumn(float.class,(short) 0));
        columnHashMap.put("姓名", new Table.TableColumn(String.class,(short) 10));

        Table t0 = new Table("t0",columnHashMap,null);
        //写入字节文件
        BytesIO.writeDataOut(t0.serializeTable(),0,t0.table_name);
        //读出字节文件
        byte[] bytes = BytesIO.readDataInto(1024,0,t0.table_name);
        Table t1 = Table.deSerializeTable(bytes);

        System.out.println();
        System.out.println(t1.getPrimaryKey());
    }

}


