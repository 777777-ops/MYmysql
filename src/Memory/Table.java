package Memory;
import DataIO.BytesIO;

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
    private final HashMap<Page,Integer> page_cache = new HashMap<>();    //页缓冲池
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
        else root = deSerializePage(root_offset);
        //根据给出的变量初始化的数据
        index_length = calculateIndexLength() + Page.INDEX_HEAD;
        initValueIndex();
        if(primaryName != null ) indexSet.add(primaryName);
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
            deletePageInCache(page.page_offset);
        }
    }

    //清扫一个根下面的所有页  包括根    写个遍历
    public void clearRootPage(int pageRoot_offset){
        Page page = deSerializePage(pageRoot_offset);
        int prt = page.getNextOffset(Page.MIN);
        Stack<Pair> stack = new Stack<>();
        while(true){

            //上去一层
            while(prt == Page.MAX && !stack.isEmpty()){
                deletePageInSpace(page);        //结束该页
                Pair pair = stack.pop();
                page = deSerializePage(pair.page_offset);
                prt = page.getNextOffset(pair.node_offset);
            }
            //遍历结束
            if(prt == Page.MAX && stack.isEmpty()){deletePageInSpace(page); break;}
            //遍历到最底层
            while(page instanceof PageNoLeaf p){
                stack.push(new Pair(page.page_offset,prt));   //记录该层
                page = deSerializePage(p.getLeftPage(prt));
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
    protected Page insertPage(byte level){

        Page instance;
        int offset = findSpareSpace(level);
        if(level == (byte)0x00){
            instance = new PageLeaf(level,(byte) PageLeafSpace,offset,this);
        } else if(level > (byte)0x00){
            instance = new PageNoLeaf(level,(byte)PageNoLeafSpace,offset,this);
        }else {throw new RuntimeException("非法page_levee");}
        //缓冲池更新
        page_cache.put(instance,0);   /*TODO*/
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

    //获取一张表中在顺序逻辑上的第一页
    private int getFirstPageOffset(){
        Page page = root;
        while(page instanceof PageNoLeaf p){
            int page_offset = p.getLeftPage(p.getNextOffset(Page.MIN));
            page = deSerializePage(page_offset);
        }
        return page.page_offset;
    }

    //根据主键查询某一记录的信息   （B+）
    public List<SearchResult> searchPrimary(Object[] index_key,String operator){
        Page page = root;
        while(page instanceof PageNoLeaf p)
        {
            int prev = p.Search(index_key[0]);     //所要进入页的前节点
            int next = p.getNextOffset(prev);   //所要进入页的后节点
            //如果next节点等于index_key  prev和next全部后移
            if(p.compare(index_key[0],next) == 0){prev = next ;next = p.getNextOffset(next);}
            page = deSerializePage(p.getLeftPage(next));
        }
        PageLeaf leaf = ((PageLeaf)page);
        SearchResult head = leaf.searchValues(index_key[0]);      //头节点
        List<SearchResult> result = new ArrayList<>();         //结果
        switch (operator){
            case "==": {
                int prt = head.offset;
                if (leaf.compare(index_key[0], prt) == 0) result.add(head);
                return result;
            }
            case ">=": {
                int page_offset = head.page_offset;
                while (page_offset != 0) {
                    leaf = (PageLeaf) deSerializePage(page_offset);
                    int prt;
                    if(page_offset == head.page_offset) prt = head.offset;
                    else prt = leaf.getNextOffset(Page.MIN);
                    while (prt != Page.MAX) {
                        result.add(new SearchResult(page_offset,prt,leaf.getValues(prt)));
                        prt = leaf.getNextOffset(prt);
                    }
                    page_offset = leaf.page_next_offset;
                }
                return result;
            }
            case "<=":{
                int page_offset = getFirstPageOffset();
                while (page_offset != head.page_offset){
                    leaf = (PageLeaf) deSerializePage(page_offset);
                    int prt = leaf.getNextOffset(Page.MIN);
                    while (prt != Page.MAX) {
                        result.add(new SearchResult(page_offset,prt,leaf.getValues(prt)));
                        prt = leaf.getNextOffset(prt);
                    }
                    page_offset = leaf.page_next_offset;
                }
                //此时的page_offset到了head页
                leaf = (PageLeaf) deSerializePage(page_offset);
                int prt = leaf.getNextOffset(Page.MIN);
                while(prt != head.offset){
                    result.add(new SearchResult(page_offset,prt,leaf.getValues(prt)));
                    prt = leaf.getNextOffset(prt);
                }
                //此时prt就是目标节点 需判断是否 ==
                if (leaf.compare(index_key, prt) == 0) result.add(head);
                return result;
            }
            case "to":{
                int page_offset = head.page_offset;
                int prt = head.offset;
                while(leaf.compare(index_key[1],prt) >= 0){
                    result.add(new SearchResult(page_offset,prt,leaf.getValues(prt)));
                    prt = leaf.getNextOffset(prt);
                    if(prt == Page.MAX){
                        page_offset = leaf.page_next_offset;  if(page_offset == 0) break;
                        leaf = (PageLeaf)deSerializePage(page_offset);
                        prt = leaf.getNextOffset(Page.MIN);
                    }
                }
                return  result;
            }
            default: throw new RuntimeException("不可能到达的语句");
        }

    }

    //所有的行数据全部都呈现出来  （线性）
    public List<SearchResult> searchLinked(){
        List<SearchResult> result = new ArrayList<>();
        int page_offset = getFirstPageOffset();
        while(page_offset != 0){
            PageLeaf leaf = (PageLeaf) deSerializePage(page_offset);
            int prt = leaf.getNextOffset(Page.MIN);
            while(prt != Page.MAX){
                result.add(new SearchResult(page_offset,prt,leaf.getValues(prt)));
                prt = leaf.getNextOffset(prt);
            }
            page_offset = leaf.page_next_offset;
        }
        return result;
    }

    /****************************插入*************************************/

    //插入一条新记录
    private void insertRec(Object[] values) throws RuntimeException {

        //从table中获取独特标识
        int heap_no = this.getDefault_key(1);
        //获取主键
        Object index_key;
        if(primaryKey == null) index_key = heap_no;
        else index_key = values[getFieldIndex(primaryKey)];
        //定位到叶子页
        Stack<Pair> stack = new Stack<>();
        Page page = root;
        while(page instanceof PageNoLeaf p)
        {
            int prev = p.Search(index_key);     //所要进入页的前节点
            int prt = p.getNextOffset(prev);   //所要进入页的节点
            //如果next节点等于index_key  prev和next全部后移
            if(p.compare(index_key,prt) == 0){prt = p.getNextOffset(prt);}
            //进入栈   栈中存放进入页的节点的前一个节点，以及页
            stack.push(new Pair(page.page_offset,prt));
            page = deSerializePage(p.getLeftPage(prt));

        }
        //page现在是叶子页了
        if(((PageLeaf)page).contain(index_key))  throw new RuntimeException("该表中已有该主键");
        //插入
        ((PageLeaf)page).insert(values,heap_no,index_key);
        //需要页分裂
        while(page.checkPageSplit()){
            PageNoLeaf parent;   int prt = Page.PAGE_HEAD;
            if(stack.isEmpty()) {parent = (PageNoLeaf) this.insertPage((byte)(page.page_level + 1)); root = parent;}
            else{Pair pair = stack.pop(); parent = (PageNoLeaf) deSerializePage(pair.page_offset); prt= pair.node_offset;}
            //分裂
            page.PageSplit(parent,prt);
            page = parent;           //page跳转至父页
        }
    }

    //外接
    public void insert(List<String> strings) throws RuntimeException {insertRec(checkObjects(strings));}

    public void insert(String... strings) throws RuntimeException{insertRec(checkObjects(Arrays.asList(strings)));}


    //----插入检查且转换-------//

    /*
    //根据字段集的属性检查每一个插入的字段行数据是否可行   默认顺序：无指定顺序  valueMap必须是空的
    private boolean checkObjects(Object... objects) throws RuntimeException {
        //Objects长度与fields不一样
        if(objects.length != fields.size())  return false;

        int index = 0;
        for (Map.Entry<String, TableColumn> map : fields.entrySet()) {
            Object object = objects[index];
            TableColumn value = map.getValue();
            //类型不一样
            if(object.getClass() != value.type && object != null) {valuesMap.clear();return false;}
            //如果是字符串 长度超过规定
            if(object instanceof String)
                if(((String)object).length() > value.length){valuesMap.clear(); return false;}
            //该值为null 且明确不能为null
            if(object == null && !value.couldNull){valuesMap.clear(); return false;}
            //明确不能重复
            if(!value.couldRepeated){/*TODO}

            //没问题
            valuesMap.put(map.getKey(),object);

            //下一个object
            index++;
        }
        return  true;
    }



     */

    //根据字段集的属性检查每一个插入的字段行数据是否可行  给Frame外接  /*TODO*/
    private Object[] checkObjects(List<String> strings) throws RuntimeException{
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
    public Object checkObject(String field,String str) throws RuntimeException{
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

    //根据主键删除记录    外接
    public void delete(Object[] index_key,String operator){
        switch (operator) {
            case ">=" -> deleteOneSide(index_key[0],1);
            case "<=" -> deleteOneSide(index_key[0],2);
            case "==" -> {delete(index_key[0],index_key[0]);}
            default -> {delete(index_key[0], index_key[1]);}
        }

    }

    //根据给出的SearchResult进行删除  外接
    public void delete(SearchResult sr){
        PageLeaf leaf = (PageLeaf)deSerializePage(sr.page_offset);
        int prev = leaf.getPrevOffset(sr.offset);
        int next = leaf.getNextOffset(sr.offset);
        Object object = leaf.getIndex_key(sr.offset);   //主键
        leaf.offsetDelete(sr.offset,2);
        leaf.setNextOffset(prev,next);
        leaf.setPrevOffset(next,prev);
        //合并缓冲
        deleteMap.putIfAbsent(sr.page_offset, object);
    }

    //根据主键删除记录   （带范围的）  between and
    private void delete(Object index_key_begin,Object index_key_end){
        int[] arr;         //本层的删除操作
        Page left = root;
        Page right = root;
        arr = left.delete(index_key_begin,index_key_end);
        while(arr.length != 0 && arr[0] != 0){
            //到下一层
            left = deSerializePage(arr[0]);
            right = deSerializePage(arr[1]);
            //缓存主键  并且更改前后页
            if(left.page_level == 0) {
                deleteMap.putIfAbsent(arr[0], index_key_begin);
                deleteMap.putIfAbsent(arr[1], index_key_end);
                if(left != right && ((PageLeaf)left).page_next_offset != right.page_offset){
                    ((PageLeaf)left).page_next_offset = right.page_offset;
                    ((PageLeaf)right).page_prev_offset = left.page_offset;
                }
            }

            if(left == right)
                arr = left.delete(index_key_begin,index_key_begin);
            else {
                arr[0] = left.deleteOneSide(index_key_begin,1);
                arr[1] = right.deleteOneSide(index_key_end,2);
            }
        }
    }

    //根据主键删除记录    >=  <=
    private void deleteOneSide(Object index_key,int model){
        Page page = root;
        //开始遍历
        int down_page = page.deleteOneSide(index_key,model);
        while(down_page != 0){
            if(page.page_level == 1)
                deleteMap.putIfAbsent(down_page, index_key);
            page = deSerializePage(down_page);
            down_page = page.deleteOneSide(index_key,model);
        }
        if(model == 1) ((PageLeaf)page).page_next_offset = 0;
        if(model == 2) ((PageLeaf)page).page_prev_offset = 0;
    }

    //调整某一叶子页的前后顺序  (叶子页的删除)
    protected void adjustLeafPage(int page_offset){
        Page page = deSerializePage(page_offset);
        if(page instanceof PageLeaf leaf) {
            if(leaf.page_next_offset != 0) {
                PageLeaf next_page = (PageLeaf) deSerializePage(leaf.page_next_offset);
                next_page.page_prev_offset = leaf.page_prev_offset;
            }
            if(leaf.page_prev_offset != 0) {
                PageLeaf prev_page = (PageLeaf) deSerializePage(leaf.page_prev_offset);
                prev_page.page_next_offset = leaf.page_next_offset;
            }
        }
    }

    /****************************页合并*********************************/


    //根据删除日志，进行页合并
    public void allLeafPageMergeCheck(){
        Stack<Pair> stack;
        Page page;
        //遍历每一个已经删除的主键
        for (Map.Entry<Integer, Object> map : deleteMap.entrySet()) {
            if(deSerializePage(map.getKey()).page_level < 0) continue; /*TODO*/  //page_level < 0代表已被删除

            Object index_key = map.getValue();
            //根据被删除的主键查找页路径  可能会有偏差需要方法调整
            stack = findWay_ToTargetPageOffset(index_key,map.getKey());
            page = deSerializePage(stack.pop().page_offset);
            if(page.page_offset != map.getKey()){throw new RuntimeException("...");}
            //从叶子页层层递归上去检查是否有需要合并的页
            while (page.checkPageMerge()) {
                pageMergeControl(page,stack);
                page = deSerializePage(stack.pop().page_offset);
            }

            stack.clear();
            page = root; //重新开始
        }
        while(root.page_num == 1 && root instanceof PageNoLeaf p){
            root = deSerializePage(p.getLeftPage(p.getNextOffset(Page.MIN)));
        }
        //清空
        deleteMap.clear();
    }

    //----------------//

    //下面四个方法用于精确定位 删除日志中的由于树结构发生调整 而导致的旧主键出现定位偏差的页地址

    //给出主键 和 页地址 根据B+树结构找到该主键所在页的路径
    private Stack<Pair> findWay_ToTargetPageOffset(Object index_key,int target_offset){
        Stack<Pair> stack = indexKeyFindWay(index_key);
        Page page = deSerializePage(stack.peek().page_offset);
        //出现了偏差
        if(page.page_offset != target_offset){
            Page target = deSerializePage(target_offset);
            //如果目标页没有节点   寻求左页帮忙
            if(target.page_num == 0){
                stack = findWay_LeftRun(target_offset);
            }else{
                stack = indexKeyFindWay(target.getIndex_key(target.getNextOffset(Page.MIN)));
            }
        }
        return stack;
    }

    //根据主键找路径
    private Stack<Pair> indexKeyFindWay(Object index_key){
        Stack<Pair> stack = new Stack<>();
        Page page = root;
        while (page instanceof PageNoLeaf p){
            //将该层的页保存
            int prev = p.Search(index_key);
            int prt = p.getNextOffset(prev);
            if(p.compare(index_key,prt) == 0)  prt = p.getNextOffset(prt);
            stack.push(new Pair(page.page_offset,prt));
            //进入下一层
            int page_offset = p.getLeftPage(prt);
            page = deSerializePage(page_offset);
        }
        stack.push(new Pair(page.page_offset,0));
        return stack;
    }

    //仅给出叶子页地址  返回正确完整路径  (左)
    private Stack<Pair> findWay_LeftRun(int target_offset){
        PageLeaf target = (PageLeaf) deSerializePage(target_offset);
        //从左页找起
        int prev = target.page_prev_offset;
        while(prev != 0){
            target = (PageLeaf) deSerializePage(prev);
            if(target.page_num > 0) break;
            //遍历
            prev = target.page_prev_offset;
        }
        //找不到匹配的左页
        if(prev == 0) {
            //组一个第一页的路径
            Page page = root;
            Stack<Pair> stack = new Stack<>(); int prt = page.getNextOffset(Page.MIN);
            while(page instanceof PageNoLeaf p){
                stack.push(new Pair(page.page_offset,prt));
                page = deSerializePage(p.getLeftPage(prt));
                prt = page.getNextOffset(Page.MIN);
            }
            stack.push(new Pair(page.page_offset,0));
            return findWay(stack,target_offset);
        }
        else {
            Object index_key = target.getIndex_key(target.getNextOffset(Page.MIN));
            return findWay(indexKeyFindWay(index_key),target_offset);
        }

    }

    //给出一条路径，根据路径查找目标页的路径 (左)
    private Stack<Pair> findWay(Stack<Pair> stack,int target_offset){
        Page page = deSerializePage(stack.pop().page_offset);
        int prt = Page.MIN;
        while (true){
            //到达叶子层
            while(page instanceof PageNoLeaf p){
                stack.push(new Pair(page.page_offset,prt));
                page = deSerializePage(p.getLeftPage(prt));
                prt = page.getNextOffset(Page.MIN);
            }
            prt = Page.MAX;
            //判断是否相等
            if(page.page_offset == target_offset) {
                stack.push(new Pair(page.page_offset,0));
                return stack;
            }
            while(prt == Page.MAX && !stack.isEmpty()){
                Pair pair = stack.pop();
                page = deSerializePage(pair.page_offset);
                prt = pair.node_offset;
                prt = page.getNextOffset(prt);   //下一个节点
            }
            if(prt == Page.MAX && stack.isEmpty())
                break;
        }
        throw new RuntimeException("无法找到相应的叶子页");
    }

    //---------------------//

    //页合并的控制端
    private boolean pageMergeControl(Page page,Stack<Pair> stack){
        //PageNoLeaf ancestor;   int ancestor_offset;
        Pair pair = stack.peek();
        PageNoLeaf parent = (PageNoLeaf) deSerializePage(pair.page_offset);
        int prev = parent.getPrevOffset(pair.node_offset);   //page页节点的前节点
        int next = parent.getNextOffset(pair.node_offset);   //page页节点的后节点
        Page neighbor = null;                                                 //page页的邻居页
        //开始找邻居页
        if(page.page_num == 0) {pageMerge(page,pair,null,3,null,0); return true;} //无需寻找邻居页，直接删除
        //父母页中只有一个工具节点的情况  寻找左邻居和右邻居都比较麻烦
        if(prev == Page.MIN && next == Page.MAX){
            return extremeLeftNeighbor(page,stack) || extremeRightNeighbor(page,stack);
        }
        //左极端情况  寻找左邻居麻烦
        else if(prev == Page.MIN){
            neighbor = deSerializePage(parent.getLeftPage(next));
            if(neighbor.page_num + page.page_num < 5){
                pageMerge(page,pair,neighbor,2,null,0);  return true;
            }
            //右邻居失败 找左邻居
            return extremeLeftNeighbor(page,stack);
        }
        //右极端情况  寻找右邻居麻烦
        else if(next == Page.MAX){
            neighbor = deSerializePage(parent.getLeftPage(prev));
            if(neighbor.page_num + page.page_num < 5){
                pageMerge(page,pair,neighbor,1,parent,prev);  return true;
            }
            //左邻居失败 找右邻居
            return extremeRightNeighbor(page,stack);
        }
        //页节点在中间的情况，最为普遍,处理最简单
        else{
            neighbor = deSerializePage(parent.getLeftPage(next));
            if(neighbor.page_num + page.page_num < 5){
                pageMerge(page,pair,neighbor,2,null,0);  return true;
            }
            neighbor = deSerializePage(parent.getLeftPage(prev));
            if(neighbor.page_num + page.page_num < 5){
                pageMerge(page,pair,neighbor,1,parent,prev);  return true;
            }
            return false;
        }
    }

    //处理 找邻居页中的极端左情况    是pageMerge的辅助方法
    private boolean extremeLeftNeighbor(Page page,Stack<Pair> stack){
        Stack<Pair> s = new Stack<>();  s.addAll(stack);         //克隆一个stack栈来使用
        Pair pair = s.pop();
        PageNoLeaf parent = (PageNoLeaf) deSerializePage(pair.page_offset);         //page的父母页
        int prt = pair.node_offset;                                            //父母页中当前页节点前节点
        int prev = parent.getPrevOffset(prt);
        PageNoLeaf ancestor = null;                                                 //极端情况肯定有祖先页
        int ancestor_offset = 0;                                                  //祖先节点
        Page neighbor = null;                                                       //邻居页
        //找祖先页
        while(prev == Page.MIN){
            if(s.isEmpty()) { break;}  //已经到根页了，操作页就是最极端的左页
            pair = s.pop();
            parent = (PageNoLeaf) deSerializePage(pair.page_offset);
            prev = parent.getPrevOffset(pair.node_offset);
        }
        //找邻居   prev != Page.MIN就说明找到祖先了
        if(prev != Page.MIN){
            ancestor = parent;
            ancestor_offset = prev;
            neighbor = deSerializePage(parent.getLeftPage(prev));
            while(neighbor.page_level != page.page_level){
                neighbor = deSerializePage(
                        ((PageNoLeaf)neighbor).getLeftPage
                                (neighbor.page_slots_offset.get(neighbor.page_slots_offset.size() - 1)));
            }
        }
        //邻居存在  且可以合并
        if(neighbor != null && neighbor.page_num + page.page_num < 5){
            pageMerge(page,stack.peek(),neighbor,1,ancestor,ancestor_offset);
            return true;
        }
        else return false;
    }

    //处理 找邻居页中的极端右情况    是pageMerge的辅助方法
    private boolean extremeRightNeighbor(Page page,Stack<Pair> stack){
        Stack<Pair> s = new Stack<>();  s.addAll(stack);         //克隆一个stack栈来使用
        Pair pair = s.pop();
        PageNoLeaf parent = (PageNoLeaf) deSerializePage(pair.page_offset);         //page的父母页
        int next = parent.getNextOffset(pair.node_offset);//父页中当前页节点后节点
        PageNoLeaf ancestor = null;                                                 //极端情况肯定有祖先页
        int ancestor_offset = 0;                                                  //祖先节点
        Page neighbor = null;                                                       //邻居页
        //找祖先页
        while(next == Page.MAX){
            if(s.isEmpty()) { break;}  //已经到根页了，操作页就是最极端的右页
            pair = s.pop();
            parent = (PageNoLeaf) deSerializePage(pair.page_offset);
            next = parent.getNextOffset(pair.node_offset);
        }
        //找邻居   next != Page.MAX就说明找到祖先了
        if(next != Page.MAX){
            ancestor = parent;
            ancestor_offset = next;
            neighbor = deSerializePage(parent.getLeftPage(next));
            while(neighbor.page_level != page.page_level){
                neighbor = deSerializePage(
                        ((PageNoLeaf)neighbor).getLeftPage
                                (neighbor.getNextOffset(Page.MIN)));
            }
        }
        //邻居存在  且可以合并
        if(neighbor != null && neighbor.page_num + page.page_num < 5){
            pageMerge(page,stack.peek(),neighbor,2,ancestor,ancestor_offset);
            return true;
        }
        else return false;
    }

    //页合并（三步）  第一步:将两页合并  第二步：父页删除节点 第三步：祖先页更新索引键
    private void pageMerge(Page page,Pair pair,Page neighbor,int model,Page ancestor,int ancestor_offset){
        PageNoLeaf parent = (PageNoLeaf) deSerializePage(pair.page_offset);
        int offset = pair.node_offset;
        //特殊情况： 本页已无节点，父页直接删除本页节点即可
        if(model == 3){
            parent.offsetDelete(offset,1);
        }else {
            //第一步
            neighbor.merge(page, model);
            if (page.page_num != 0) throw new RuntimeException("逻辑错误");
            if (parent.getLeftPage(offset) != page.page_offset) throw new RuntimeException("逻辑错误");
            //第二步
            parent.offsetDelete(offset,1);
            if (parent.page_num == 0 && root == parent) root = neighbor;
            //第三步
            if (ancestor != null) {
                PageNoLeaf ancestor_page = (PageNoLeaf) deSerializePage(ancestor.page_offset);
                ancestor_page.findIndex(ancestor_offset);
            }
        }
    }

    /****************************页缓冲池*******************************/

    //在缓冲池中删除某一页
    public void deletePageInCache(int page_offset){
        Page page = null;
        //遍历page_cache  找到page_offset对应的page
        for (Page pageInCache : page_cache.keySet()) {
            //目标页地址的页在缓冲池中
            if(pageInCache.page_offset == page_offset){
                page = pageInCache;
                break;
            }
        }
        if(page == null)  return;
        else{page_cache.remove(page);}
    }

    //根据页地址返回页 1.若页缓冲池中存在 直接取  2.若缓冲池中不存在，刷新页缓冲池并且反序列化
    public Page deSerializePage(int page_offset){

        //该页偏移量的对应的页
        Page page = null;

        //遍历page_cache  找到page_offset对应的page
        for (Page pageInCache : page_cache.keySet()) {
            //目标页地址的页在缓冲池中
            if(pageInCache.page_offset == page_offset){
                page = pageInCache;
                break;
            }
        }

        //如果页不在缓冲池中  反序列化后插入或替换page_cache
        if(page == null) {
            //根据偏移量反序列页
            byte[] data;
            data = BytesIO.readDataInto(Page.PAGE_HEAD,page_offset,table_name);
            page = Page.deSerialize(data,this);
            //缓冲池大小小于5    直接插入
            if(page_cache.size() < 100/*TODO*/) {page_cache.put(page,0); return page;}
            //缓冲池大小不小于5  遍历page_cache 找到 value最小的替换
            Page min_key = getMinKey();
            //关闭min_key页
            closePage(min_key);
            //删除调用值最小的page
            page_cache.remove(min_key);
            page_cache.put(page,0);
        }
        //如果页在缓冲池中   将调用值加一
        else{
            int value = page_cache.get(page);
            page_cache.replace(page,value + 1);
        }
        return page;

    }

    //找到缓冲池中调取数最小的页
    private Page getMinKey() {
        Page min_key = null;
        int min_value = 999999;
        for (Map.Entry<Page, Integer> entry : page_cache.entrySet()) {
            Page key = entry.getKey();
            Integer value = entry.getValue();
            if (value < min_value) {
                min_value = value;
                min_key = key;
            }
        }
        //水个异常
        if(min_key == null) throw new RuntimeException("无法找到最小的页");
        return min_key;
    }

    //关闭某一个页
    public void closePage(Page page) {
        page.serializeHead();  //序列化页头
        BytesIO.writeDataOut(page.page_buffer.array(),page.page_offset,table_name);  //写入磁盘
    }

    //关闭该表   即序列化缓冲池中所有的表
    public void close(){
        //检查删除日志中是否有需要合并的
        allLeafPageMergeCheck();
        //关闭缓冲池中的所有的页
        for (Page page : page_cache.keySet()) {
            closePage(page);
        }
        closePage(root);
        BytesIO.writeDataOut(serializeTable(),0,table_name);

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
    //-------------------//

    //返回字段属性的迭代器
    protected Collection<TableColumn> getFieldsProperty(){return fields.values();}
    //返回字段的数据长度
    protected short getFieldLength(String name){return fields.get(name).length;}
    //返回默认主键 并且加一
    protected int getDefault_key(int addNum){int nowKey = this.default_key; this.default_key += addNum; return nowKey;}
    //修改根页
    protected void setRoot(Page page){this.root = page;}







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


