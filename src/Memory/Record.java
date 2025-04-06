package Memory;

import MyTable.Table;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

//叶子页中的行记录
public class Record extends IndexRecord{
    //常量
    public static final byte TYPE_NULL = 0x00;       //为空标志
    public static final byte TYPE_STRING = 0x01;     //字符串类型
    public static final byte TYPE_INT = 0x02;        //整数类型
    public static final byte TYPE_FLOAT = 0x03;      //小数类型
    public static final byte TYPE_BOOLEAN = 0x04;    //布尔类型
    //表数据
    public Table table;          //通过组合模式将表和行数据组合在一起，使用继承很不方便  (只有叶子记录需要)
    //行数据
    public HashMap<String, Object> valuesMap;  //建立一个根据字段名字查询数据的hashmap   (核心数据)

    //创建一条新的行记录  (普通叶子记录)
    public Record(Table table,HashMap<String, Object> valuesMap) {
        this.table = table;
        this.rec_type = 0x00;                   //普通叶子记录
        this.valuesMap = valuesMap;
        if(table.getPrimaryKey() == null) index_key = table.getDefault_key(1);
        else{ index_key = valuesMap.get(table.getPrimaryKey());}

    }

    public Record(byte rec_type, Object index_key) {
        super(rec_type, index_key);
    }

    //将本条记录序列化
    public byte[] serialize() {
        byte[] bytes = new byte[0];
        for (String str : table.getFieldNames()) {
            int fieldMark = table.getMark(str);                  //字段的标识
            Class<?> type = table.getFieldType(str);             //字段类型
            Object obj = valuesMap.get(str);                     //字段相应的数据
            byte[] b2 = serializeSingleObject(obj, type, fieldMark);//根据字段类型、字段标识、数据生成的二进制数据
            bytes = ByteTools.concatBytes(bytes, b2);             //将二进制数据都拼接起来
        }

        return bytes;
    }

    //将一个Object转化为字节数组   该Object只能是规定的几个数据类型
    private byte[] serializeSingleObject(Object obj, Class<?> field, int fieldMark)  //字段数据，字段数据类型，字段标识
    {
        //字段标识为0-126    过多抛出异常
        if (fieldMark > 126) throw new RuntimeException("字段过多");
        byte mark = (byte) fieldMark;
        //如果字段数据为null
        if (obj == null)
            return new byte[]{TYPE_NULL, mark};   //返回空标志
        //创建一个新字节数组
        byte[] bytes;
        ByteBuffer buffer;

        if (field == String.class)   //字符串
        {
            byte[] strBytes = ((String) obj).getBytes(StandardCharsets.UTF_8);
            buffer = ByteBuffer.allocate(1 + 1 + 4 + strBytes.length)
                    .put(TYPE_STRING)
                    .put(mark)
                    .putInt(strBytes.length)         //最大能记录2的31次方长度的字符串  //要限制
                    .put(strBytes);
        } else if (field == int.class) {
            buffer = ByteBuffer.allocate(1 + 1 + 4)
                    .put(TYPE_INT)
                    .put(mark)
                    .putInt((Integer) obj);
        } else if (field == float.class) {
            buffer = ByteBuffer.allocate(1 + 1 + 4)
                    .put(TYPE_FLOAT)
                    .put(mark)
                    .putFloat((Float) obj);
        } else if (field == boolean.class) {
            buffer = ByteBuffer.allocate(1 + 1 + 1)
                    .put(TYPE_BOOLEAN)
                    .put(mark)
                    .put((byte) (((boolean) obj) ? 1 : 0));
        } else {
            bytes = new byte[0];
            throw new RuntimeException("非法类型");
        }

        bytes = buffer.array();
        return bytes;
    }

    //反序列化   //静态可被外接
    public static HashMap<String, Object> deSerialize(byte[] data, Table table) {
        ByteBuffer buffer = ByteBuffer.wrap(data);  //小字端，模拟innodb的小字端
        HashMap<String, Object> values = new HashMap<>();
        while (buffer.hasRemaining()) {
            byte type = buffer.get();      //字段类型
            int mark = buffer.get();    //字段标识
            String fieldName = table.getFieldName(mark);
            switch (type) {
                case (TYPE_NULL):
                    values.put(fieldName, null);
                    break;
                case (TYPE_STRING):
                    int length = buffer.getInt();                          //字符串长度
                    byte[] strBytes = new byte[length];
                    buffer.get(strBytes);
                    values.put(fieldName, (new String(strBytes, StandardCharsets.UTF_8)));
                    break;
                case (TYPE_INT):
                    values.put(fieldName, buffer.getInt());
                    break;
                case (TYPE_FLOAT):
                    values.put(fieldName, buffer.getFloat());
                    break;
                case (TYPE_BOOLEAN):
                    values.put(fieldName, buffer.get() == 1);
            }
        }
        return values;

    }
}
