package MyTable;
import DataIO.BytesIO;
import Memory.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;


public class Table {

    //表信息
    public String table_name;
    private HashMap<String,TableColumn> fields;  //根据字段名字寻找字段信息   字段集
    private List<String> intToString;            //(工具数组)建立一个将字段名字顺序排序的数组，下标对应唯一的字段名，为了替代字段名难以储存的问题。
    private Page root;                           //该表的根页
    private String primaryKey;                   //主键的名字
    private int default_key;                     //如果没有主键，默认的主键
    //字段信息的内部类
    public static class TableColumn{
        public Class<?> type;           //字段类型
        public boolean couldNull;       //能否为null
        public boolean isPrimary;       //是否为主键
        public boolean couldRepeated;   //能否被重复


        //初始化 无约束
        public TableColumn(Class<?> type) {
            this.type = type; couldNull = true; isPrimary = false; couldRepeated = true;
        }
        //初始化 有约束
        public TableColumn(Class<?> type,boolean couldNull,boolean isPrimary,boolean isRepeated){
            this.type = type; this.couldNull = couldNull; this.isPrimary = isPrimary; this.couldRepeated = isRepeated;
        }
        //初始化 根据字节标识设置约束
        public TableColumn(Class<?> type,byte constraint) {
            couldRepeated = false; isPrimary = false; couldRepeated = false;
            int sum = (int)constraint;
            if(sum >= 4){couldRepeated = true; sum -= 4;}
            if(sum >= 2){isPrimary = true; sum -= 2;}
            if(sum >= 1){couldNull = true; sum -= 1;}
            this.type = type;
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

    //创建一张新表       //要求给出一个包含字段名字和字段信息的hashmap表
    public Table(String tableName,HashMap<String,TableColumn> fields,String primaryName){
        this.root = null;
        this.table_name = tableName;
        this.fields = fields;
        intToString = new ArrayList<>();
        //遍历该字段的名字集合，初始化字段名字数组
        for (String fieldName : getFieldNames()) {
            intToString.add(fieldName);
        }
        //初始化主键
        this.primaryKey = primaryName;
        //无主键
        default_key = 0;
    }

    public Table(String tableName,HashMap<String,TableColumn> fields,String primaryName,int default_key,Page root){
        this.root = root;
        this.default_key = default_key;
        this.table_name = tableName;
        this.fields = fields;
        intToString = new ArrayList<>();
        //遍历该字段的名字集合，初始化字段名字数组
        for (String fieldName : getFieldNames()) {
            intToString.add(fieldName);
        }
        //初始化主键
        this.primaryKey = primaryName;

    }

    //插入一条新记录
    public void insertRec(Memory.Record rec) {
        //现在在处理的页
        Page nowPage = root;
        while(nowPage instanceof PageNoLeaf)
        {

        }
    }
    //插入一条新记录
    public void insertRec(HashMap<String,Object> valuesMap)
    {
        this.insertRec(new Memory.Record(this,valuesMap));
    }

    //序列化该表的信息
    /*
        0-31:  表名
        32-63: 主键名字
        64-67: 根页
        68-71: 默认主键
        512-1023: 字段数据
     */
    //序列化该表的信息
    public byte[] serializeTable(){
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        byte[] bytes;
        //写入表名
        buffer.position(0);
        bytes = table_name.getBytes(StandardCharsets.UTF_8);
        buffer.put((byte)bytes.length);   //字符串长度
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
        else buffer.putInt(root.off_set);
        //写入默认主键
        buffer.position(68);
        buffer.putInt(default_key);
        //写入字段数据
        buffer.position(512);
        for (String fieldName : getFieldNames()) {
            bytes = fieldName.getBytes(StandardCharsets.UTF_8);
            buffer.put((byte)bytes.length);
            buffer.put(bytes);
            //字段属性
            buffer.put(ByteTools.typeToByte(fields.get(fieldName).type)); //将字段类型转为相应的字节
            buffer.put(fields.get(fieldName).getFieldBooleanByte());      //将字段的约束转为相应的字节
        }
        return buffer.array();
    }

    //反序列化创建一个新的表
    public static Table deSerializeTable(byte[] table_data){
        ByteBuffer buffer = ByteBuffer.wrap(table_data);
        String table_name;   //表名
        String primaryKey;   //主键名
        Page root = null;           //根页
        int default_key;     //默认主键
        HashMap<String,TableColumn> fields = new HashMap<>(); //字段属性
        List<String> intToString = new ArrayList<>();  //字段名的标志
        int str_length;    //字符串长度
        byte[] bytes;      //用来读取字符串的字节数组
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
        int off_set = buffer.getInt();    //获得到根页的偏移量
        if(off_set != 0)
        {/*TODO*/}
        //默认主键
        buffer.position(68);
        default_key = buffer.getInt();
        //字段属性
        buffer.position(512);
        str_length = buffer.get();
        while(str_length != 0)
        {
            String field_name;
            TableColumn tableColumn;
            Class<?> type;   //字段类型
            byte constraint; //字段约束
            //字段名
            bytes = new byte[str_length];
            buffer.get(bytes);
            field_name = new String(bytes,StandardCharsets.UTF_8);
            intToString.add(field_name);    //放入字段标注数组
            //字段类型
            type = ByteTools.byteToType(buffer.get());
            //字段约束
            constraint = buffer.get();
            tableColumn = new TableColumn(type,constraint);
            fields.put(field_name,tableColumn);
            //继续循环
            str_length = buffer.get();
        }
        return new Table(table_name,fields,primaryKey,default_key,root);
    }

    //将该表的信息写入文件中
    public void writeTable(){BytesIO.writeDataOut(this.serializeTable(),0,this.table_name);}


    /**************************************************************/

    //返回字段名字列表的迭代器
    public Iterable<String> getFieldNames(){
        return this.fields.keySet();
    }
    //返回字段的数据类型
    public Class<?> getFieldType (String name){
        return fields.get(name).type;
    }
    //返回字段的特定标识
    public int getMark(String name){return intToString.indexOf(name);}
    //返回特定标识中的字段
    public String getFieldName(int mark){return intToString.get(mark);}
    //返回主键
    public String getPrimaryKey(){return this.primaryKey;}
    //返回默认主键 并且加一
    public int getDefault_key(int addNum){int nowKey = this.default_key; this.default_key += addNum; return nowKey;}
    //返回该表的根页
    public Page getRoot(){return this.root;}

    /**********************************************************/

    public static void main(String[] args) {
        HashMap<String, Table.TableColumn> columnHashMap = new HashMap<>();
        columnHashMap.put("年龄", new Table.TableColumn(int.class,false,true,false));
        columnHashMap.put("是否参加医保", new Table.TableColumn(boolean.class));
        columnHashMap.put("支付额度", new Table.TableColumn(float.class));
        columnHashMap.put("姓名", new Table.TableColumn(String.class));

        Table t0 = new Table("t0",columnHashMap,null);
        //写入字节文件
        BytesIO.writeDataOut(t0.serializeTable(),0,t0.table_name);
        //读出字节文件
        byte[] bytes = BytesIO.readDataInto(1024,0,t0.table_name);
        Table t1 = Table.deSerializeTable(t0.serializeTable());

        System.out.println();
        System.out.println(t1.getPrimaryKey());
    }

}


