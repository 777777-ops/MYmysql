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
    public /*TODO*/ LinkedHashMap<String,TableColumn> fields;  //根据字段名字寻找字段信息   字段集
    private Page root;                           //该表的根页
    private String primaryKey;                   //主键的名字  //主键如果没有就是null  //索引记录中的的index_key永远不为null
    private int default_key;                     //如果没有主键，默认的主键
    private int last_offset;                     //表文件中最后一个可以被插入的偏移量指针
    private int table_used;                      //表文件大小           单位B
    private int record_maxLength;                //记录字节的总占用长度（用于检测溢出）  //下面还有一个实际占用长度(不常用)
    //不用写入文件中的变量
    private int[] valueOffset;                   //各个字段数据的数据头
    private int index_length;                    //主键字节长度(包括索引记录头的)
    private HashMap<Page,Integer> page_cache;    //页缓冲池
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

        //初始化 字符串 无约束
        public TableColumn(Class<?> type,short length) {
            this.type = type;this.length = length; couldNull = true; isPrimary = false; couldRepeated = true;
        }
        //初始化 有约束
        public TableColumn(Class<?> type,short length,boolean couldNull,boolean isPrimary,boolean isRepeated){
            this.type = type;this.length = length; this.couldNull = couldNull; this.isPrimary = isPrimary; this.couldRepeated = isRepeated;
        }
        //初始化 根据字节标识设置约束
        public TableColumn(Class<?> type,short length,byte constraint) {
            couldRepeated = false; isPrimary = false; couldRepeated = false;
            int sum = (int)constraint;
            if(sum >= 4){couldRepeated = true; sum -= 4;}
            if(sum >= 2){isPrimary = true; sum -= 2;}
            if(sum >= 1){couldNull = true; sum -= 1;}
            this.type = type;
            this.length = length;
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

    //该类用于页分裂和页合并  A是父页  B是该页在父页中的节点的前一个节点
    public static class Pair{
        public int page_offset;
        public int page_pre_offset;
        public Pair(int page_offset,int page_pre_offset)
        {
            this.page_offset = page_offset;
            this.page_pre_offset = page_pre_offset;
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
        this.last_offset = KB;
        this.table_used =  KB;
        this.default_key = 0;
        this.page_cache = new HashMap<>(); //页缓冲池
        this.root = insertPage((byte)0x00);
        //新表创建时必须给出的变量
        this.table_name = tableName;
        this.fields = fields;
        this.primaryKey = primaryName;
        //根据给出的变量初始化的数据
        valueOffset = calculateRecordLength();
        record_maxLength = valueOffset[valueOffset.length - 1] + calculateIndexLength() + Page.RECORD_HEAD + RecordLengthAdd;
        index_length = calculateIndexLength() + Page.INDEX_HEAD;

    }

    //构造函数：反序列一张表
    public Table(String tableName,LinkedHashMap<String,TableColumn> fields,String primaryName,
                 int default_key,int root_offset,int last_offset,int record_maxLength,int RecordLengthAdd,int SlotSplit){
        this.primaryKey = primaryName;
        this.last_offset = last_offset;
        this.default_key = default_key;
        this.table_name = tableName;
        this.fields = fields;
        this.record_maxLength = record_maxLength;

        //新表可改变的全局变量
        this.RecordLengthAdd = RecordLengthAdd;
        this.SlotSplit = SlotSplit;

        //根页
        if(root_offset == 0) root = null;
        else root = deSerializePage(root_offset);

        //页缓冲池
        page_cache = new HashMap<>();

        //根据给出的变量初始化的数据
        index_length = calculateIndexLength() + Page.INDEX_HEAD;
        valueOffset = calculateRecordLength();
    }

    /*
            删除和插入操作都伴随着页合并和页分裂，可以通过栈Stack存储“路过的父页”--叶子页的父页去降低 页合并和页分裂
         在查找父页节点上的logN的时间开销。但考虑到将来通过改变字段重构页的可能性，暂时不启动Stack，转而在每个页中都记录
         父页的偏移量
     */

    /****************************查询********************************/

    /*
    //根据主键查询是否在表中  用于检查主键是否存在表中
    public boolean contain(Object index_key){
        Page page = root;
        while(page instanceof PageNoLeaf)
        {
            int offset = ((PageNoLeaf)page).findEnterPage(index_key);      //所要进入的页的偏移量
            page = deSerializePage(offset);
        }
        //page现在是叶子页了   直接插入
        return ((PageLeaf)page).contain(index_key);
    }

     */

    //根据主键查询某一记录的信息   private
    private LinkedHashMap<String,Object> searchMap(Object index_key){
        Page page = root;
        while(page instanceof PageNoLeaf)
        {
            PageNoLeaf p = (PageNoLeaf) page;
            int prev = p.Search(index_key);     //所要进入页的前节点
            int next = p.getNextOffset(prev);   //所要进入页的后节点
            //如果next节点等于index_key  prev和next全部后移
            if(p.compare(index_key,next) == 0){prev = next ;next = p.getNextOffset(next);}
            page = deSerializePage(p.getLeftPage(next));
        }
        return  ((PageLeaf)page).searchRecord(index_key);
    }

    //根据主键查询某一记录的信息   外接
    public void search(Object index_key){
        LinkedHashMap<String,Object> map = searchMap(index_key);
        if(map == null)
            System.out.println("表中无符合条件的记录");
        else{
            for (Map.Entry<String, Object> entry: map.entrySet()) {
                System.out.println(entry.getKey() + ": " + entry.getValue());
            }
        }
    }

    /****************************插入*************************************/

    //插入一个新页
    public Page insertPage(byte level){

        Page instance;
        if(level == (byte)0x00){
            instance = new PageLeaf(level,(byte) PageLeafSpace,last_offset,this);
        } else if(level > (byte)0x00){
            instance = new PageNoLeaf(level,(byte)PageNoLeafSpace,last_offset,this);
        }else {throw new RuntimeException("非法page_levee");}

        //更新last_offset和table_used
        lastOffsetAdd(level);
        //缓冲池更新
        page_cache.put(instance,0);
        return instance;
    }

    //更新表中的last_offset和table_used;
    private void lastOffsetAdd(byte level){
        int add;
        if(level == (byte) 0x00){add = PageLeafSpace * KB;}           //叶子页
        else if (level < (byte) 0x7f) {add = PageNoLeafSpace * KB;}   //非叶子页
        else{ throw new RuntimeException("非法level值"); }

        if(last_offset == table_used){ //如果空闲指针在最后
            last_offset += add;  table_used += add;
        }else{                       //如果空闲指针不在字节数组最后
            /*TODO*/
        }
    }

    //插入一条新记录
    public void insertRec(LinkedHashMap<String,Object> valuesMap) throws RuntimeException{

        //从table中获取独特标识
        int heap_no = this.getDefault_key(1);
        //获取主键
        Object index_key;
        if(primaryKey == null) index_key = heap_no;
        else index_key = valuesMap.get(primaryKey);
        //定位到叶子页
        Stack<Pair> stack = new Stack<>();
        Page page = root;
        while(page instanceof PageNoLeaf)
        {
            PageNoLeaf p = (PageNoLeaf) page;
            int prev = p.Search(index_key);     //所要进入页的前节点
            int next = p.getNextOffset(prev);   //所要进入页的后节点
            //如果next节点等于index_key  prev和next全部后移
            if(p.compare(index_key,next) == 0){prev = next ;next = p.getNextOffset(next);}
            //进入栈   栈中存放进入页的节点的前一个节点，以及页
            stack.push(new Pair(page.page_offset,prev));
            page = deSerializePage(p.getLeftPage(next));

        }
        //page现在是叶子页了
        if(((PageLeaf)page).contain(index_key))  throw new RuntimeException("该表中已有该主键");
        //插入
        ((PageLeaf)page).insert(valuesMap,heap_no,index_key);
        //需要页分裂
        while(page.checkPageSplit()){
            PageNoLeaf parent;   int prev = Page.MIN;
            if(stack.isEmpty()) {parent = (PageNoLeaf) this.insertPage((byte)(page.page_level + 1)); root = parent;}
            else{Pair pair = stack.pop(); parent = (PageNoLeaf) deSerializePage(pair.page_offset); prev= pair.page_pre_offset;}
            //分裂
            page.PageSplit(parent,prev);
            page = parent;           //page跳转至父页
        }
    }

    //插入一条新记录 默认顺序插入
    public void insertRec(Object... objects){
        LinkedHashMap<String,Object> valueMap = new LinkedHashMap<>();

        boolean flag = insertObjects(valueMap,objects);
        if(!flag) throw new RuntimeException("插入失败");

        insertRec(valueMap);
    }

    /****************************检查插入记录数据***************************/

    //根据字段集的属性检查每一个插入的字段行数据是否可行   默认顺序：无指定顺序  valueMap必须是空的
    private boolean insertObjects(LinkedHashMap<String,Object> valuesMap,Object... objects){
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
            if(!value.couldRepeated){/*TODO*/}

            //没问题
            valuesMap.put(map.getKey(),object);

            //下一个object
            index++;
        }
        return  true;
    }

    //根据字段集的属性检查每一个插入的字段行数据是否可行  给Frame外接
    public boolean insertRec(List<String> strings) throws RuntimeException{
        if(strings.size() != fields.size())  return false;
        //插入记录的LinkedHashMap
        LinkedHashMap<String,Object> valueMap = new LinkedHashMap<>();

        int index = 0;
        for (Map.Entry<String, TableColumn> map : fields.entrySet()) {
            String str = strings.get(index);
            TableColumn value = map.getValue();
            Object object;
            if (value.type == Integer.class){
                try {
                    object = Integer.parseInt(str); // 这会抛出异常
                } catch (NumberFormatException e) {
                    throw new RuntimeException("记录添加失败，有不符合字段的数据");
                }
            }else if(value.type == String.class){
                object = str;
            }else if(value.type == Float.class){
                try {
                    object = Float.parseFloat(str); // 这会抛出异常
                } catch (NumberFormatException e) {
                    throw new RuntimeException("记录添加失败，有不符合字段的数据");
                }
            }else if(value.type == Boolean.class){
                if(str.equalsIgnoreCase("TRUE")) object = true;
                else if(str.equalsIgnoreCase("FALSE")) object = false;
                else throw new RuntimeException("记录添加失败，有不符合字段的数据");
            }else throw new RuntimeException("记录添加失败，有不符合字段的数据");
            index++;
            valueMap.put(map.getKey(),object);
        }
        insertRec(valueMap);

        return true;

    }

    /****************************删除记录********************************/

    //根据主键删除记录
    public void delete(Object index_key){
        delete(index_key,index_key);
    }

    //根据主键删除记录   （带范围的）
    public void delete(Object index_key_begin,Object index_key_end){
        //stack1 和 stack2分别记录删除路径上的页 以及进入的节点的前节点
        Stack<Pair> stack1= new Stack<>();
        Stack<Pair> stack2= new Stack<>();
        Pair[] arr;         //本层的删除操作
        Page left = root;
        Page right = root;
        arr = left.delete(index_key_begin,index_key_end);  //delete操作出来的Pair是 下一层页 + 页前节点  而Stack要的是  本层页 + 页前节点
        //begin和end都进入同一页的情况
        while(arr.length == 2 && arr[0].page_offset == arr[1].page_offset){
            stack1.push(new Pair(left.page_offset,arr[0].page_pre_offset));   //将本层的删除操作记录后进入下一层
            stack2.push(new Pair(right.page_offset,arr[1].page_pre_offset));   //将本层的删除操作记录后进入下一层
            left = right = deSerializePage(arr[0].page_offset);
            arr = left.delete(index_key_begin,index_key_end);
        }
        //begin和end进入了两个不同页
        while(arr.length == 2 && arr[0] != null && arr[1] != null ){
            if(arr[0].page_offset == arr[1].page_offset)  throw new RuntimeException("逻辑错误");
            stack1.push(new Pair(left.page_offset,arr[0].page_pre_offset));   //将本层的删除操作记录后进入下一层
            stack2.push(new Pair(right.page_offset,arr[1].page_pre_offset));   //将本层的删除操作记录后进入下一层
            left = deSerializePage(arr[0].page_offset);
            right = deSerializePage(arr[1].page_offset);
            arr[0] = left.deleteOneSide(index_key_begin,1);
            arr[1] = right.deleteOneSide(index_key_end,2);
        }
        deSerializeAllPage();  /*TODO*/
        //至此删除完毕,下面考虑合并操作
        while(  (left.checkPageMerge() || right.checkPageMerge()) && !stack1.isEmpty() && !stack2.isEmpty() ){
            boolean flag = right == left;
            if(flag){
                pageMergeControl(left,stack1);
                left = right = deSerializePage(stack1.pop().page_offset);
                stack2.pop();
            }else {
                if(left.checkPageMerge()){
                    pageMergeControl(left,stack1);
                    left = deSerializePage(stack1.pop().page_offset);
                    deSerializeAllPage();  /*TODO*/
                }
                if(right.checkPageMerge()){
                    pageMergeControl(right,stack2);
                    right = deSerializePage(stack2.pop().page_offset);
                    deSerializeAllPage();
                }
            }
        }
    }

    //页合并的控制端
    private boolean pageMergeControl(Page page,Stack<Pair> stack){
        //PageNoLeaf ancestor;   int ancestor_offset;
        Pair pair = stack.peek();
        PageNoLeaf parent = (PageNoLeaf) deSerializePage(pair.page_offset);
        int prev = pair.page_pre_offset;                               //page页节点的前节点
        //prev的处理          在范围删除的时候，prev极有可能失效    (后续如果该情况触发较多，需重新思考其他方法)
        if(parent.isDelete(prev)){
            prev = parent.findPrevNode(page);
            pair.page_pre_offset = prev;        //栈中搞得prev也要更新
        }
        int next = parent.getNextOffset(parent.getNextOffset(prev));   //page页节点的后节点
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
        int prev = pair.page_pre_offset;                                            //父母页中当前页节点前节点
        PageNoLeaf ancestor = null;                                                 //极端情况肯定有祖先页
        int ancestor_offset = 0;                                                  //祖先节点
        Page neighbor = null;                                                       //邻居页
        //找祖先页
        while(prev == Page.MIN){
            if(s.isEmpty()) { break;}  //已经到根页了，操作页就是最极端的左页
            pair = s.pop();
            parent = (PageNoLeaf) deSerializePage(pair.page_offset);
            prev = pair.page_pre_offset;
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
        int next = parent.getNextOffset(parent.getNextOffset(pair.page_pre_offset));//父页中当前页节点后节点
        PageNoLeaf ancestor = null;                                                 //极端情况肯定有祖先页
        int ancestor_offset = 0;                                                  //祖先节点
        Page neighbor = null;                                                       //邻居页
        //找祖先页
        while(next == Page.MAX){
            if(s.isEmpty()) { break;}  //已经到根页了，操作页就是最极端的右页
            pair = s.pop();
            parent = (PageNoLeaf) deSerializePage(pair.page_offset);
            next = parent.getNextOffset(parent.getNextOffset(pair.page_pre_offset));
        }
        //找邻居   next != Page.MAX就说明找到祖先了
        if(next != Page.MAX){
            ancestor = parent;
            ancestor_offset = parent.getNextOffset(pair.page_pre_offset);
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
        int prev = pair.page_pre_offset;
        //特殊情况： 本页已无节点，父页直接删除本页节点即可
        if(model == 3){
            parent.offsetDelete(prev);
            return;
        }
        //第一步
        neighbor.merge(page,model);
        if(page.page_num != 0)  throw new RuntimeException("逻辑错误");
        int offset = parent.getNextOffset(prev);
        if(parent.getLeftPage(offset) != page.page_offset)  throw new RuntimeException("逻辑错误");
        //第二步
        parent.offsetDelete(prev);
        if(parent.page_num == 0 &&root == parent) root = neighbor;
        //第三步
        if(ancestor == null) return;
        else{
            PageNoLeaf ancestor_page = (PageNoLeaf) deSerializePage(ancestor.page_offset);
            ancestor_page.findIndex(ancestor_offset);
        }

    }



    /****************************页缓冲池*******************************/

    //在缓冲池中删除某一页
    public void deletePage(int page_offset){
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

    //测试逻辑  /*TODO*/
    public IndexRecord deSerializeIndexRecord(int page_offset) {
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

        if(page == null) return null;
        else{
            page.deSerializeIndexRecords();
            return page.Page_Head_Try;
        }
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
        for (Page page : page_cache.keySet()) {
            closePage(page);
        }
        closePage(root);
        BytesIO.writeDataOut(serializeTable(),0,table_name);
    }

    //测试逻辑专用 /*TODO*/
    public void deSerializeAllPage(){
        for (Page page : page_cache.keySet()) {
            page.deSerializeIndexRecords();
        }

    }


    /*
    //测试逻辑专用
    public void deSerializeAllPage(){
        root.deSerializeIndexRecords();
    }

     */

    /****************************逻辑操作*******************************/

    //计算在当前字段集下，一个记录实际占用的字节数
    private int[] calculateRecordLength(){
        int[] arr = new int[fields.size() + 1];
        int index = 0;
        arr[index++] = 0;
        for (Map.Entry<String, TableColumn> entry : fields.entrySet()) {
            TableColumn tableColumn = entry.getValue();
            arr[index] = arr[index - 1] + ByteTools.singleObjectLength(tableColumn.type,tableColumn.length);
        }
        return arr;
    }

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
        0-31:  表名
        32-63: 主键名字
        64-67: 根页
        68-71: 默认主键
        72-75: 最后一个可被插入页指针
        76-79: 行数据总占用的字节长度
        80-83: 行数据扩张预留的字节长度
        84-87: 页中槽分裂阈值
        512-1023: 字段数据
     */
    //序列化该表的信息
    public byte[] serializeTable(){
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        byte[] bytes;
        //写入表名
        buffer.position(0);
        bytes = table_name.getBytes(StandardCharsets.UTF_8);
        buffer.put((byte)bytes.length);   //字符串字节的长度
        buffer.put(bytes);                //字符串
        //写入主键名
        buffer.position(32);
        if(primaryKey == null)
            buffer.put((byte)0);
        else {
            bytes = primaryKey.getBytes(StandardCharsets.UTF_8);
            buffer.put((byte) bytes.length);
            buffer.put(bytes);
        }
        //写入根页
        buffer.position(64);
        if(root == null)  buffer.putInt(0);
        else buffer.putInt(root.page_offset);
        //写入默认主键
        buffer.position(68);
        buffer.putInt(default_key);
        //写入最后一个可被插入页指针
        buffer.position(72);
        buffer.putInt(last_offset);
        //写入行数据总占用的字节长度，包括（扩张的）
        buffer.position(76);
        buffer.putInt(record_maxLength);
        //可改变全局变量
        //写入行数据扩张预留的字节长度
        buffer.position(80);
        buffer.putInt(RecordLengthAdd);
        //写入页中槽分裂阈值
        buffer.position(84);
        buffer.putInt(SlotSplit);
        //写入字段数据
        buffer.position(512);
        for (String fieldName : getFieldNames()) {
            bytes = fieldName.getBytes(StandardCharsets.UTF_8);
            buffer.put((byte)bytes.length);
            buffer.put(bytes);
            //字段属性
            buffer.put(ByteTools.typeToByte(fields.get(fieldName).type)); //将字段类型转为相应的字节
            buffer.put((byte) fields.get(fieldName).length);              //将字段类型长度转为单字节
            buffer.put(fields.get(fieldName).getFieldBooleanByte());      //将字段的约束转为相应的字节
        }
        return buffer.array();
    }

    //反序列化创建一个新的表
    public static Table deSerializeTable(byte[] table_data){
        ByteBuffer buffer = ByteBuffer.wrap(table_data);
        Table table ;
        String table_name;   //表名
        String primaryKey;   //主键名
        int root_offset;      //根页地址
        int default_key;     //默认主键
        int last_offset;     //可被插入的页指针
        int record_maxLength; //行数据字节占用长度
        LinkedHashMap<String,TableColumn> fields = new LinkedHashMap<>(); //字段属性
        int str_length;    //字符串长度
        byte[] bytes;      //用来读取字符串的字节数组
        int RecordLengthAdd;   //行数据扩张预留的字节长度
        int SlotSpilt;

        //表名
        buffer.position(0);
        str_length = buffer.get();
        bytes = new byte[str_length];
        buffer.get(bytes);
        table_name = new String(bytes,StandardCharsets.UTF_8);
        //主键名
        buffer.position(32);
        str_length = buffer.get();
        if(str_length == 0) primaryKey = null;
        else{
            bytes = new byte[str_length];
            buffer.get(bytes);
            primaryKey = new String(bytes,StandardCharsets.UTF_8);
        }
        //根页
        buffer.position(64);
        root_offset = buffer.getInt();    //获得到根页的偏移量
        //默认主键
        buffer.position(68);
        default_key = buffer.getInt();
        //最后一个可被插入的页指针
        buffer.position(72);
        last_offset = buffer.getInt();
        //行数据扩张预留的字节长度
        buffer.position(76);
        record_maxLength= buffer.getInt();
        //行数据扩张预留的字节长度
        buffer.position(80);
        RecordLengthAdd = buffer.getInt();
        //页中槽分裂的阈值
        buffer.position(84);
        SlotSpilt = buffer.getInt();
        //字段属性
        buffer.position(512);
        str_length = buffer.get();
        while(str_length != 0)
        {
            String field_name;
            TableColumn tableColumn;
            Class<?> type;   //字段类型
            short length;      //字段类型长度
            byte constraint; //字段约束
            //字段名
            bytes = new byte[str_length];
            buffer.get(bytes);
            field_name = new String(bytes,StandardCharsets.UTF_8);
            //字段类型
            type = ByteTools.byteToType(buffer.get());
            //字段类型长度
            length =buffer.get();
            //字段约束
            constraint = buffer.get();
            tableColumn = new TableColumn(type,length,constraint);
            fields.put(field_name,tableColumn);
            //继续循环
            str_length = buffer.get();
        }
        table = new Table(table_name,fields,primaryKey,default_key,root_offset,last_offset,record_maxLength,RecordLengthAdd,SlotSpilt);
        return table;
    }

    //将该表的信息写入文件中
    public void writeTable(){BytesIO.writeDataOut(this.serializeTable(),0,this.table_name);}


    /**************************获取*********************************/

    //返回字段名字数组
    public String[] getFieldNamesArr(){
        return fields.keySet().toArray(new String[0]);
    }
    //返回字段名字列表的迭代器
    public Iterable<String> getFieldNames(){return this.fields.keySet();}
    //返回字段的集合迭代器
    public Set<Map.Entry<String,TableColumn>> getFieldEntry(){return fields.entrySet();}
    //返回字段的数据类型
    public Class<?> getFieldType (String name){
        return fields.get(name).type;
    }
    //返回字段的数据长度
    public short getFieldLength(String name){return fields.get(name).length;}
    //返回主键名字
    public String getPrimaryKey(){return this.primaryKey;}
    //返回默认主键 并且加一
    public int getDefault_key(int addNum){int nowKey = this.default_key; this.default_key += addNum; return nowKey;}
    //返回该表的根页
    public Page getRoot(){return this.root;}
    //修改根页
    public void setRoot(Page page){this.root = page;}
    //返回该表中的记录占用的总字节长度
    public int getRecord_maxLength(){return this.record_maxLength;}
    //返回该表的主键索引的字节长度
    public int getIndex_length(){return this.index_length;}
    //返回该表的数据位置头
    public int[] getValueOffset(){return valueOffset;}



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


