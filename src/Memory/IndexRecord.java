package Memory;

//非叶子页中的索引和叶子页中的记录的超类
public class IndexRecord implements Comparable<IndexRecord>{
    //行数据
    public byte delete_flag;       //删除标志                                         // 1为删除 、0为存在  （1字节）
    public byte rec_type;          //记录类型                                         (1字节) 0x00为叶子记录 0x01是非叶子节点 02是伪最小 03是伪最大
    protected byte n_owned;        //该槽数所拥有的索引记录数量                         (1字节)
    public IndexRecord next_record;//下一条记录                                       (16位)
    public Object index_key;       //索引值

    //初始化，用于页中的伪最大和伪最小，和非叶子页中的索引初始化
    public IndexRecord(byte rec_type,Object index_key){
        this.delete_flag = 0x00;           //删除标志为0
        this.rec_type = 0x00;              //槽默认为0
        this.rec_type = rec_type;
        if(rec_type == 0x01)               //非叶子的普通索引节点
            this.index_key = index_key;
        else this.index_key = null;
    }

    public IndexRecord() {}

    //重写索引的比较大小方法
    @Override
    public int compareTo(IndexRecord o) {
        //伪最大最小记录
        if(o.rec_type == 0x02)  //比较对象为伪最小
            return 1;
        if(o.rec_type == 0x03)  //比较对象为伪最大
            return -1;
        //普通记录
        if(index_key instanceof Integer)
            return (int)this.index_key - (int)o.index_key;
        else if(index_key instanceof Float){
            float a = (float)this.index_key - (float)o.index_key;
            if(a > 0)     return 1;
            else if(a <0) return -1;
            else          return 0;
        }else if(index_key instanceof String)
            return this.index_key.toString().compareTo(o.index_key.toString());
        else{
            throw new RuntimeException("索引值类型在可检测范围之外");
        }
    }



    //测试点 -- 序列化 反序列化 数据部分

    /*
    public static void main(String[] args) {
        HashMap<String, Table.TableColumn> columnHashMap = new HashMap<>();
        columnHashMap.put("年龄", new Table.TableColumn(int.class));
        columnHashMap.put("是否参加医保", new Table.TableColumn(boolean.class));
        columnHashMap.put("支付额度", new Table.TableColumn(float.class));
        columnHashMap.put("姓名", new Table.TableColumn(String.class));
        Table t0 = new Table("t0", columnHashMap,null);

        HashMap<String, Object> values = new HashMap<>();
        values.put("年龄", 20);
        values.put("是否参加医保", true);
        values.put("支付额度", 30.45f);
        values.put("姓名", "成龙");
        IndexRecord row1 = new Record(t0,values);

        byte[] bytes = row1.serialize();
        System.out.println(bytes.length);
        HashMap<String, Object> devalues = IndexRecord.deSerialize(bytes, t0);
        for (Object value : devalues.values()) {
            System.out.println(value);
        }

    }
     */

}

