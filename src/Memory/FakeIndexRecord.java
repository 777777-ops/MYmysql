package Memory;

//页中伪最大和伪最小
public class FakeIndexRecord extends IndexRecord{

    //初始化:用于创建一个新的
    public FakeIndexRecord(byte rec_type,int offset,IndexRecord next_record,Table table){
        super(rec_type,null,offset,next_record,table);
    }

    //初始化:反序列
    public FakeIndexRecord(byte rec_type,byte delete_flag,int next_record_offset,int offset,byte n_owned,Table table){
        super(rec_type,null,offset,next_record_offset,delete_flag,n_owned,table);
    }


    @Override
    public byte[] serialize() {
        return incompleteSerialize();
    }

    //返回所有数据
    public Object[] getNode_All(){
        return new Object[]{index_key,delete_flag,offset,rec_type,n_owned,next_record_offset};
    }
}
