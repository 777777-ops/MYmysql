package ConditionPage;

import Memory.Table;

public class ColumnBuilder {
    static final String regex = "^String\\[\\d+\\]$";   //字符串格式

    public static void ColumnInserter(String str, Table table){
        String[] parts = str.trim().split("\\s+");

        String name;                      //名字
        Class<?> type; short length = 0; boolean couldNull = true; boolean isPrimary = false; boolean couldRepeated = true;  //属性
        //parts太少
        if(parts.length < 2)  throw new RuntimeException("格式错误");
        name = parts[0];

        //数据类型
        if(parts[1].matches(regex)){ type = String.class; length = (short) subToInt(parts[1]);}
        else if(parts[1].equalsIgnoreCase("INT")) type = Integer.class;
        else if(parts[1].equalsIgnoreCase("FLOAT")) type = Float.class;
        else if(parts[1].equalsIgnoreCase("BOOLEAN")) type = Boolean.class;
        else throw new RuntimeException("无法检测的数据类型");

        int i = 2;
        while(i < parts.length){
            if(parts[i].equalsIgnoreCase("UNIQUE")) { i++; couldRepeated = false;}
            else if(parts[i].equalsIgnoreCase("NOT") && i + 1 < parts.length &&
                    parts[i+1].equalsIgnoreCase("NULL")) { i += 2; couldNull = false;}
            else if(parts[i].equalsIgnoreCase("PRIMARY") && i + 1 < parts.length &&
                    parts[i+1].equalsIgnoreCase("KEY")){i += 2; isPrimary = true;}
            else throw new RuntimeException("无法检测的约束");
        }
        //插入
        table.insertColumn(name,new Table.TableColumn(type,length,couldNull,isPrimary,couldRepeated));
    }

    //从字符串的格式中提取出长度
    private static int subToInt(String str){
        int begin = 0 ;   int end;
        while(str.charAt(begin) != '[') begin++;
        begin ++;
        end = begin;
        while(str.charAt(end) != ']') end++;
        return Integer.parseInt(str.substring(begin,end));
    }
}
