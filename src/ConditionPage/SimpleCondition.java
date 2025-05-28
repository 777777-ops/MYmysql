package ConditionPage;


import Memory.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//单个条件
public class SimpleCondition extends Condition{

    private static final List<String> VALID_OPERATORS = Arrays.asList(">=", "<=", "==","to");

    public String field;
    public String operator;
    public Object[] data;

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


        //检查第一个部分是否在字段集中
        field = parts[0];
        String [] fieldNames = table.getFieldNamesArr();
        int i;
        for (i = 0 ; i < fieldNames.length; i++)if(fieldNames[i].equals(field)) break;
        if(i == fieldNames.length) throw new IllegalArgumentException("表中无该字段名");

        // 分割后的字符有两种  3个字符串的
        if (parts.length == 3) {
            operator = parts[1];
            String dataStr = parts[2];

            // 检查是否为合法运算符
            if (!VALID_OPERATORS.contains(operator)) {
                throw new IllegalArgumentException(
                        "第二部分必须是>=, <= 或 ==。实际得到: '" + operator + "'");
            }

            //数据
            if(!operator.equals("in")){
                data = new Object[1];
                data[0] = table.checkObject(field,dataStr);
            }

            //如果是boolean类型只允许使用 == 运算符
            if(table.getFieldType(field) == Boolean.class && !operator.equals("=="))
                throw new IllegalArgumentException("布尔类型数据只允许使用 == 运算符");
        }
        //5个字符串的  只能是between and  该程序使用 between to
        else if(parts.length == 5){
            String operator_head = parts[1];
            String begin = parts[2];
            operator = parts[3];
            String end = parts[4];

            //运算符
            if(!(operator_head.equalsIgnoreCase("BETWEEN") && operator.equalsIgnoreCase("TO")))
                throw new IllegalArgumentException("语法错误,正确的语法是 ... BETWEEN ... TO ....");
            operator = "to";

            //数据
            data = new Object[2];
            data[0] = table.checkObject(field,begin);
            data[1] = table.checkObject(field,end);
        }
        else throw new IllegalArgumentException("未知指令");

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
        if(value == null) return false;    /*TODO*/   //暂不支持null操作
        return switch (operator) {
            case "==" -> compare(value, data[0], type) == 0;
            case ">=" -> compare(value, data[0], type) >= 0;
            case "<=" -> compare(value, data[0], type) <= 0;
            case "to" -> compare(value, data[0], type) >= 0 && compare(value,data[1], type) <= 0;
            default -> throw new RuntimeException("错误的operator运算符");
        };
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
    public String toString(){return field + " " + operator + " " + data[0].toString();}
}
