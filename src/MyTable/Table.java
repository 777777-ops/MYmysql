package MyTable;

import Memory.RowRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class Table {
    public String tableName;
    private HashMap<String,TableColumn> fields;  //根据字段名字寻找字段信息
    private List<String> intToString;          //建立一个将字段名字顺序排序的数组，下标对应唯一的字段名，为了替代字段名难以储存的问题。


    //字段信息的子类
    public static class TableColumn{
        public Class<?> type;     //字段类型
        //public int length;        //字段长度
        //约束
    }

    //创建一张新表       //要求给出一个包含字段名字和字段信息的hash表
    public Table(String tableName,HashMap<String,TableColumn> fields){
        this.tableName = tableName;
        this.fields = fields;
        intToString = new ArrayList<>();
        for (String fieldName : getFieldNames()) {
            intToString.add(fieldName);
        }
    }

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


}


