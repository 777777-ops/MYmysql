package Memory;

import java.nio.ByteBuffer;

//非叶子页中的索引键值
public class IndexKey extends IndexRecord{
    public Page leftPage;               //该索引的左页
    public int leftPage_offset;         //索引左页偏移量

    public IndexRecord Try;

    //创建一个新的索引节点：由页分配本索引的偏移量、下一条索引的地址、以及该索引记录的类型
    public IndexKey(byte rec_type,Object index_key,int offset,IndexRecord next_record,
                    Page leftPage,Table table){
        super(rec_type,index_key,offset,next_record,table);
        //左页
        this.leftPage = leftPage;
        if(this.leftPage == null) leftPage_offset = 0;
        else leftPage_offset = leftPage.page_offset;
    }

    //反序列创建一个新的索引节点
    public IndexKey(byte rec_type,Object index_key,int offset,int next_record_offset,
                       byte delete_flag,byte n_owned, int leftPage_offset,Table table) {
        super(rec_type,index_key,offset,next_record_offset,delete_flag,n_owned,table);
        this.leftPage_offset = leftPage_offset;



        /*TODO*/
        //this.Try = table.deSerializeIndexRecord(leftPage_offset);


        /*
        this.leftPage = table.deSerializePage(leftPage_offset);
        if(leftPage instanceof PageLeaf)
            leftPage = null;
        else
            leftPage.deSerializeIndexRecords();


         */




        /*
        this.leftPage = table.deSerializePage(leftPage_offset);
        leftPage.deSerializeIndexRecords();


         */







    }

    /*****************序列******************/

    //序列化
    public byte[] serialize(){
        byte[] row_head = incompleteSerialize();
        ByteBuffer buffer = ByteBuffer.allocate(HEAD_LENGTH + 4);
        //行头序列化
        buffer.put(row_head);
        //左页序列化
        if(leftPage == null) buffer.putInt(0);
        else buffer.putInt(leftPage.page_offset);
        //索引序列化
        byte[] index_bytes;
        if(table.getPrimaryKey() == null)  //无主键
            index_bytes = ByteTools.serializeSingleObject(index_key,int.class,(short)0);  //字段标识-1用于检错
        else  //有主键
        {
            String primaryName = table.getPrimaryKey();
            Class<?> type = table.getFieldType(primaryName);
            short length = table.getFieldLength(primaryName);
            index_bytes = ByteTools.serializeSingleObject(index_key,type,length);
        }
        buffer.put(index_bytes);

        return buffer.array();
    }

    /*****************************************/
    //返回所有数据
    public Object[] getNode_All(){
        return new Object[]{index_key,delete_flag,offset,rec_type,n_owned,leftPage_offset};
    }

}
