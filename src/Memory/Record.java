package Memory;

import java.nio.ByteBuffer;
import java.util.Collection;
/*
    default_no      : 根据table中default_key给出
    indexKey_off    : 创建新Record时默认为0
    indexKey        : 创建新Record时默认为null
*/

//叶子页中的行记录
public class Record extends IndexRecord{
    //行数据
    public Object[] values ;         //建立一个根据字段名字查询数据的hashmap   (核心数据)  /*TODO*/
    public int heap_no;                              //  创建新Record时表给出


    //创建一条新的行记录  (普通叶子记录) 由页分配本记录的偏移量、下一条记录的地址、以及该索引记录的类型  (完整创建)
    public Record(byte rec_type,Object[] values,int offset,IndexRecord next_record,Table table){
        super(rec_type,null,offset,next_record,table);

        this.heap_no = table.getDefault_key(1);
        this.values = values;
        //主键
        if(table.getPrimaryKey() == null) this.index_key = heap_no;
        else{ this.index_key = values[table.getFieldIndex(table.getPrimaryKey())]; }
    }

    //反序列化行记录
    public Record(byte rec_type,Object index_key,int offset,int next_record_offset,
                  byte delete_flag,byte n_owned,Table table,
                  Object[] values,int heap_no){
        super(rec_type,index_key,offset,next_record_offset,delete_flag,n_owned,table);
        this.values = values;
        this.heap_no = heap_no;
    }

    /**************************逻辑操作*****************************/

    /**************************序列*********************************/

    /*
    //反序列出下一条记录   (不建议单独使用)
    protected void deSerializeNextRecord(){
        if(this.next_record_offset == 0)  this.next_record = null; //没有下一条记录了
        if(this.next_record != null)  throw new RuntimeException("序列化下一条记录时，next_record已不为null");

        byte[] data = BytesIO.readDataInto(table.calculateRecordLength(),this.next_record_offset,table.table_name);
        next_record = IndexRecord.deSerialize(data,table);
    }

     */

    //将本条记录序列化  Record中的伪最大最小值都是11字节
    public byte[] serialize() {
        //行头序列化
        byte[] row_head = incompleteSerialize();   //未序列化主键值 和 行字节长度   11字节
        if(rec_type != 0x00)   //如果非普通记录
            return row_head;
        //索引序列化
        byte[] index_bytes;
        if(table.getPrimaryKey() == null)  //无主键
            index_bytes = ByteTools.serializeSingleObject(index_key,Integer.class,(short)0);  //字段标识-1用于检错
        else  //有主键
        {
            String primaryName = table.getPrimaryKey();
            Class<?> type = table.getFieldType(primaryName);
            short length = table.getFieldLength(primaryName);
            index_bytes = ByteTools.serializeSingleObject(index_key,type,length);
        }
        //序列化行数据
        byte[] values_bytes = new byte[0];
        int index = 0;
        for (Table.TableColumn tableColumn : table.getFieldsProperty()) {
            Class<?> type = tableColumn.type;
            short length = tableColumn.length;
            Object obj = values[index];                     //字段相应的数据
            byte[] b2 = ByteTools.serializeSingleObject(obj, type,length);//根据字段类型、数据生成的二进制数据
            values_bytes = ByteTools.concatBytes(values_bytes, b2);             //将二进制数据都拼接起来
            index++;
        }

        ByteBuffer buffer = ByteBuffer.allocate(HEAD_LENGTH + 4 + index_bytes.length + values_bytes.length + table.RecordLengthAdd);
        //记录头
        buffer.put(row_head);
        //记录独特序号
        buffer.putInt(heap_no);
        //索引值
        buffer.put(index_bytes);
        //行数据
        buffer.put(values_bytes);

        return buffer.array();
    }

    //部分反序列化记录数据
    public static Object[] deSerializeData(ByteBuffer buffer, Table table) {
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

    /**********************************************************/

    public Object[] getNode_All(){
        Object[] objects = new Object[6 + values.length];
        objects[0] = index_key;
        objects[1] = delete_flag;
        objects[2] = offset;
        objects[3] = rec_type;
        objects[4] = n_owned;

        int index = 6;
        for (Object value : values) {
            objects[index++] = value;
        }
        return objects;
    }

    /*************************************************/


}
