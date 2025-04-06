package Memory;

//非叶子页中的索引键值
public class IndexKey extends IndexRecord{
    public Page leftPage;               //该索引的左页
    //创建一个新的索引节点，不能称为‘记录’
    public IndexKey(byte rec_type,Object index_key,Page leftPage){
        super(rec_type,index_key);
        this.leftPage = leftPage;
    }
}
