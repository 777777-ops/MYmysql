package ConditionPage;


import Memory.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//单个条件
public class SimpleCondition extends Condition{

    private static final List<String> VALID_OPERATORS = Arrays.asList(">=", "<=", "==");

    String field;                           /*TODO*/
    String operator;
    Object data;

    public SimpleCondition (String conditionStr, Table table) throws IllegalArgumentException{
        this.table = table;
        parseConditions(conditionStr);
    }

    //解析条件字符串
    @Override
    protected void parseConditions(String input) throws IllegalArgumentException {
        if (input == null) {
            throw new IllegalArgumentException("输入不能为null");
        }
        // 使用正则表达式分割任意空白字符
        String[] parts = input.trim().split("\\s+");

        // 检查分割后的部分数量
        if (parts.length != 3) {
            throw new IllegalArgumentException(
                    "输入必须包含三个部分，用空格分隔。实际得到: " + parts.length + " 部分");
        }

        // 检查第二个部分是否为合法运算符
        operator = parts[1];
        if (!VALID_OPERATORS.contains(operator)) {
            throw new IllegalArgumentException(
                    "第二部分必须是>=, <= 或 ==。实际得到: '" + operator + "'");
        }

        field = parts[0];
        String dataStr = parts[2];

        //检查第一个部分是否在字段集中
        String [] fieldNames = table.getFieldNamesArr();
        int i;
        for (i = 0 ; i < fieldNames.length; i++)if(fieldNames[i].equals(field)) break;
        if(i == fieldNames.length) throw new IllegalArgumentException("表中无该字段名");

        //第三个部分是否满足字段类型
        data = table.checkObject(field,dataStr);

        //如果是boolean类型只允许使用 == 运算符
        if(table.getFieldType(field) == Boolean.class && !operator.equals("=="))
            throw new IllegalArgumentException("布尔类型数据只允许使用 == 运算符");

    }

    //返回该条件下需要使用到的索引查询字段
    @Override
    protected List<Condition> getIndexKeys(){
        List<Condition> indexs = new ArrayList<>();
        if(table.getIndexSet().contains(field))indexs.add(this);
        return indexs;
    }

    //给出一个数据数组，要求根据当前的条件返回数据是否符合条件
    @Override
    protected boolean isEligible(Object[] values){
        Object value = values[table.getFieldIndex(field)];
        Class<?> type = table.getFieldType(field);
        if(operator.equals("=="))
            return compare(value,data,type) == 0;
        else if(operator.equals(">="))
            return compare(value,data,type) >= 0;
        else if(operator.equals("<="))
            return compare(value,data,type) <= 0;
        else throw new RuntimeException("错误的operator运算符");
    }

    //比较两个数据大小
    private int compare(Object a,Object b,Class<?> type){
        if(type == Integer.class) return (int)a - (int)b;
        else if(type == Float.class){
            float c = (float) a - (float) b;
            if(c > 0) return 1;else if(c == 0) return 0; else return -1;
        }
        else if(type == String.class) return a.toString().compareTo(b.toString());
        else if(type == Boolean.class) {
            if((boolean)a == (boolean)b) return 0; else return 1;
        }
        else throw new RuntimeException("无法识别的字段类型");
    }

    //重写输出 //逻辑测试
    @Override
    public String toString(){return field + " " + operator + " " + data.toString();}
}
