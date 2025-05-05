package Memory;

import DataIO.BytesIO;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.HashMap;

import static Memory.ByteTools.deSerializeSingleObject;

//非叶子页中的索引和叶子页中的记录的超类
public abstract class IndexRecord implements Comparable<IndexRecord>{
    /*
    行头：
        删除标志         (1字节)
        下一条记录偏移量  (4字节)
        本条记录的偏移量  (4字节)   (无用留空)
        记录类型         (1字节)
        槽中所拥有的数量  (1字节)
        索引值           (?)

        非叶子索引 = 行头 + 左页偏移量 + 左索引偏移量 + 索引值 = 11 + 4 + 4 + ?;   (确定)
        叶子行数据 =  行头 + 行记录独特标识 + 索引值 + 行数据表    =  11 + 4 + ? + ?
        非叶子索引最终长度   = 15 + ?
        叶子行数据最终长度   = 叶子行数据 + default_record_add(默认64B)          预计160B左右

        可以设定一个记录初始长度，用于不同的实际需求中，为什么记录的长度很重要。在不考虑长字符文本的情况下，假如没有给记录
      预留一定的空闲空间，那么所有的记录虽然可以紧凑地利用磁盘空间，但一旦涉及到记录数据的更变扩大，那么原来紧凑的空间就会被
      破坏，更新的记录数据被迫转移到其他位置，更不用提及如果是表新字段的插入，那么成片的数据更变这种带来的内存和io压力相对于
      重新插入所有数据。所以本程序会像mysql一样，在设置字段字符串类型时要求用户给出字符串的最大长度，以此来智能分配单条记录
      的总长度，预留50-100字节不等的空闲空间。
        为了预防字段插入带来的所有数据扩张问题，本程序会采用行链接的处理方式，避免大规模的数据移动。
        最后，会考虑实现一个低优先级的空间重组操作，使数据解决碎片化问题。
    */
    /*
        一个新的IndexRecord
            rec_type   :   创建新IndexRecord时页给出
            index_key  :   创建新IndexRecord时页给出
            table      :   创建新IndexRecord时页给出
            offset     :   创建新IndexRecord时页给出
            next_record:   创建新IndexRecord时页给出

            delete_flag         :   创建新IndexRecord时 默认为0x00
            n_owned             :   创建新IndexRecord时 默认为0x00
            next_record_offset  :   创建新IndexRecord时 由next_record得出

    */
    //表数据
    public Table table;          //通过组合模式将表和行数据组合在一起，使用继承很不方便
    //行数据
    protected byte delete_flag;       //删除标志                                         // 1为删除 、0为存在  （1字节）
    protected int offset;          //本记录的相对偏移量偏移量
    protected byte rec_type;          //记录类型    0x00为叶子记录 0x01是非叶子 02伪最小  03伪最大
    protected byte n_owned;        //该槽数所拥有的索引记录数量
    protected int next_record_offset; //下一条记录的偏移量
    protected IndexRecord next_record;//下一条记录
    protected Object index_key;       //索引值

    public static final int HEAD_LENGTH = 11;  //头长度

    //初始化，用于页中的伪最大和伪最小，和非叶子页中的索引初始化  （测试程序逻辑专用）
    public IndexRecord(byte rec_type,Object index_key,Table table){
        this.delete_flag = 0x00;           //删除标志为0
        this.n_owned = 0x00;               //槽数默认为0
        this.rec_type = rec_type;          //记录类型

        if(rec_type == 0x10 || rec_type == 0x00)               //叶子和非叶子的普通索引节点
            this.index_key = index_key;
        else this.index_key = null;

        this.table = table;
    }

    //初始化, 页创建索引记录     由页分配本记录的偏移量、下一条记录的地址、以及该索引记录的类型
    public IndexRecord(byte rec_type,Object index_key,int offset,IndexRecord next_record,Table table) {
        this(rec_type,index_key,table);
        this.offset = offset;
        this.next_record = next_record;
        if(next_record == null) next_record_offset = 0;
        else  {next_record_offset = next_record.offset;}
    }

    //初始化, 全变量用于反序列化 (不会反序列出next_record对象)
    public IndexRecord(byte rec_type,Object index_key,int offset,int next_record_offset,
                       byte delete_flag,byte n_owned,Table table) {
        this.delete_flag = delete_flag;
        this.offset = offset;
        this.rec_type = rec_type;
        this.n_owned = n_owned;
        this.next_record_offset = next_record_offset;
        this.index_key = index_key;
        this.table = table;
    }

    /***************************序列************************************/

    //完全序列化，抽象给子类实现
    public abstract byte[] serialize();

    //部分序列化 所有索引或者记录都必须要序列化的部分     ！未序列化索引值
    public byte[] incompleteSerialize(){
        ByteBuffer buffer = ByteBuffer.allocate(HEAD_LENGTH );
        //删除标志
        buffer.put(delete_flag);
        //下一条记录的偏移量
        if(next_record == null)  next_record_offset = 0;
        else next_record_offset = next_record.offset;
        buffer.putInt(next_record_offset);
        //本条记录的偏移量
        buffer.putInt(offset);
        //记录类型
        buffer.put(rec_type);
        //槽中所有的记录的数量
        buffer.put(n_owned);
        return buffer.array();
    }

    //完全反序列化，实例出Record或IndexKey对象   //需要给出与记录最大长度等长的数组  (不会完整反序列化next_record)
    public static IndexRecord deSerialize(byte[] bytes,Table table){
        IndexRecord instance;  //instance要用Record 或 IndexKey实现
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        //变量设置
        byte delete_flag;
        int offset;  byte rec_type;  byte n_owned;
        int next_record_offset;
        //删除标志
        delete_flag = buffer.get();
        //下一条记录的偏移量  下一条记录对象默认null
        next_record_offset = buffer.getInt();
        //本记录的偏移量
        offset = buffer.getInt();
        //记录类型
        rec_type = buffer.get();
        //槽中所拥有的索引记录数量
        n_owned = buffer.get();

        //检查是IndexKey类还是Record
        if(rec_type == 0x01 || rec_type == 0x11)         //0x11特殊的IndexKey
        {
            int leftPage_offset;       //左页的偏移量
            Object index_key;          //索引值
            //反序列左页偏移量
            leftPage_offset = buffer.getInt();
            //索引值反序列化
            byte type = buffer.get();
            short length = table.getFieldLength(table.getPrimaryKey());
            index_key = deSerializeSingleObject(buffer,type,length);
            //实例化
            instance = new IndexKey(rec_type,index_key,offset,next_record_offset,
                    delete_flag,n_owned,leftPage_offset,table);

        }//IndexKey类
        else if(rec_type == 0x00)
        {
            int heap_no = buffer.getInt();           //记录独特的序号标志
            Object index_key;                        //索引值
            HashMap<String,Object> values;  //数据
            //索引值反序列化
            byte type = buffer.get();
            short length = table.getFieldLength(table.getPrimaryKey());
            index_key = deSerializeSingleObject(buffer,type,length);
            //记录数据反序列化
            int[] valueOffset = table.getValueOffset();
            byte[] record_bytes = new byte[valueOffset[valueOffset.length - 1]];   //实际长度减去现已读取的字节长度
            buffer.get(record_bytes);
            values = Record.deSerializeData(record_bytes,table);
            //实例化
            instance = new Record(rec_type,index_key,offset,next_record_offset,delete_flag,
                    n_owned,table,values,heap_no);
        }  //Record类
        else if(rec_type == 0x02 || rec_type == 0x03)
        {
            instance = new FakeIndexRecord(rec_type,delete_flag,next_record_offset,offset,n_owned,table);
        }    //伪最大最小
        else{throw new RuntimeException("rec_type超出限定范围");}



        return instance;

    }


    /*********************************************************************/

    //重写索引的比较大小方法
    public int compareTo(IndexRecord o) {
        //伪最大最小记录
        if(o.rec_type == 0x02)  //比较对象为伪最小
            return 1;
        if(o.rec_type == 0x03 || o.rec_type == 0x11)  //比较对象为伪最大  或为特殊的索引值
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

    /**********************************************************************/
    //返回下一个实例节点
    public IndexRecord getNext_record(){return next_record;}
    //返回所有数据
    public abstract Object[] getNode_All();



}

