package ConditionPage;

import Memory.Table;
import UI.TableFrame;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//复杂条件
public class NestedCondition extends Condition{
    private final List<String> separator = new ArrayList<>();          //分隔符
    private final List<Condition> conditions = new ArrayList<>();      //条件集
    public boolean ALL;                                               //全部查询的标号


    public NestedCondition(String conditionStr,Table table) throws IllegalArgumentException{
        ALL = false;
        this.table = table;
        //解析
        parseConditions(conditionStr);
    }


    //解析条件字符串
    @Override
    protected void parseConditions(String input) throws IllegalArgumentException {
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("条件语句为空");
        }
        // 预处理：去除首尾空格
        input = input.trim();
        // 预处理：去除首尾括号
        if(input.charAt(0) == '('){input = input.substring(1, input.length() - 1);}
        if(input.trim().isEmpty()) throw new IllegalArgumentException("括号内有空体");
        if(input.equalsIgnoreCase("ALL")) {ALL = true; return;}  //全部搜索
        // 1. 先校验括号位置是否正确
        validateParentheses(input);
        // 2. 正则表达式匹配逻辑分隔符或括号内容
        Pattern pattern = Pattern.compile("(\\(.*?\\)|(?:^|\\s)(?i)(and|or)(?:\\s|$))");
        Matcher matcher = pattern.matcher(input);

        int lastEnd = 0;
        while (matcher.find()) {
            // 添加分隔符前的内容
            if (matcher.start() > lastEnd) {
                String before = input.substring(lastEnd, matcher.start()).trim();
                if (!before.isEmpty()) {
                    conditions.add(new SimpleCondition(before,table));
                }
            }

            // 处理匹配到的分割符AND、OR 也有可能是()
            String matched = matcher.group().trim();
            if (matched.startsWith("(")) {
                conditions.add(new NestedCondition(matched,table));
            } else {
                String operator = matched.toLowerCase();
                separator.add(operator);
            }

            lastEnd = matcher.end();
        }

        // 添加最后一部分
        if (lastEnd < input.length()) {
            String remaining = input.substring(lastEnd).trim();
            if (!remaining.isEmpty()) {
                conditions.add(new SimpleCondition(remaining,table));
            }
        }
    }

    //返回该条件下可以使用到的索引查询字段
    @Override
    public List<Condition> getIndexKeys(){

        List<Condition> indexs = new ArrayList<>();   //维护一个动态改变的字段索引集
        for (int i = 0; i < conditions.size(); i++) {
            //第一个Condition直接放入即可
            if(i == 0){indexs.addAll(conditions.get(i).getIndexKeys());continue;}

            boolean sep = separator.get(i - 1).equals("and");            //sep即separator的单个操作符 and是true  or是false
            Condition condition = conditions.get(i);                     //当前处理的条件
            List<Condition> conditionIndexKeys = condition.getIndexKeys();  //当前处理的条件下可能得字段索引

            //条件一：原先存在可以使用的索引 且or后新增的条件也 存在可以使用的索引   结果一：原先索引和新索引合并
            if(!indexs.isEmpty() && !conditionIndexKeys.isEmpty() && !sep){
                indexs.addAll(conditionIndexKeys);
            }
            //条件二：只要 or后新增的条件不存在索引                               结果二：清空索引集
            else if (!sep && conditionIndexKeys.isEmpty()) indexs.clear();
            //条件三：and后新增的条件存在索引，原先没有索引                        结果三：采用新索引集
            else if (sep && !conditionIndexKeys.isEmpty() && indexs.isEmpty()) indexs = conditionIndexKeys;
            //条件四：and后新增的条件存在索引，原先也有索引集                      结果四：选择较短的索引集作为新索引集
            else if (sep && !conditionIndexKeys.isEmpty())
                if(conditionIndexKeys.size() > indexs.size()) indexs = conditionIndexKeys;
        }
        return indexs;
    }

    //给出一个数据数组，要求根据当前的条件返回数据是否符合条件
    @Override
    public boolean isEligible(Object[] values){
        boolean result = conditions.get(0).isEligible(values);
        for (int i = 1; i < conditions.size(); i++) {
             boolean sep = separator.get(i - 1).equals("and");       //sep是true时说明是and
             if(!result && !sep) result = conditions.get(i).isEligible(values);
             if(result && sep) result = conditions.get(i).isEligible(values);
        }
        return result;
    }

    /************检查格式**************/

    //检查格式
    private void validateParentheses(String input) throws IllegalArgumentException {
        // 检查括号是否成对
        int balance = 0;
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '(') {
                balance++;
                // 检查左括号前是否是AND/OR或字符串开头
                if (i > 0 && !isValidBeforeParenthesis(input, i)) {
                    throw new IllegalArgumentException("左括号前必须是AND/OR或字符串开头");
                }
            } else if (c == ')') {
                balance--;
                // 检查右括号后是否是AND/OR或字符串结尾
                if (i < input.length() - 1 && !isValidAfterParenthesis(input, i)) {
                    throw new IllegalArgumentException("右括号后必须是AND/OR或字符串结尾");
                }
                if (balance < 0) {
                    throw new IllegalArgumentException("括号不匹配，多余的右括号");
                }
            }
        }

        if (balance > 0) {
            throw new IllegalArgumentException("括号不匹配，缺少右括号");
        }
    }

    // 获取括号前的非空字符
    private boolean isValidBeforeParenthesis(String input, int pos) {
        int i = pos - 1;
        while (i >= 0 && Character.isWhitespace(input.charAt(i))) {
            i--;
        }
        if (i < 0) return true; // 字符串开头

        //定位
        int start = i;
        while (start >= 0 && !Character.isWhitespace(input.charAt(start))) {
            start--;
        }
        start++;
        // 检查是否是AND或OR
        String before = input.substring(start, i + 1).trim().toLowerCase();
        return before.equals("and") || before.equals("or");
    }

    // 获取括号后的非空字符
    private boolean isValidAfterParenthesis(String input, int pos) {
        int i = pos + 1;
        while (i < input.length() && Character.isWhitespace(input.charAt(i))) {
            i++;
        }
        if (i >= input.length()) return true; // 字符串结尾

        // 定位
        int end = i;
        while (end < input.length() && !Character.isWhitespace(input.charAt(end))) {
            end++;
        }
        end--;
        // 检查是否是AND或OR
        String after = input.substring(i, end + 1).trim().toLowerCase();
        return after.equals("and") || after.equals("or");
    }

    /*********************************/

    public static void main1(String[] args) {
        LinkedHashMap<String, Table.TableColumn> columnHashMap = new LinkedHashMap<>();
        columnHashMap.put("编号", new Table.TableColumn(Integer.class,(short)0,false,true,false));
        columnHashMap.put("名字", new Table.TableColumn(String.class,(short) 5,false,false,false));
        columnHashMap.put("年龄", new Table.TableColumn(Integer.class,(short) 0,false,false,false));
        columnHashMap.put("工资", new Table.TableColumn(Float.class,(short) 0,false,false,false));
        Table t0 = new Table("t0",columnHashMap,"编号");


        t0.insert("3", "'李明'", "25", "4218.7");

        t0.insert("7", "'张伟'", "22", "3895.2");
        t0.insert("5", "'王芳'", "19", "2763.9");
        t0.insert("9", "'赵燕'", "28", "5321.6");
        t0.insert("2", "'陈晨'", "23", "3147.8");
        t0.insert("8", "'刘洋'", "21", "4672.3");
        t0.insert("4", "'孙莉'", "26", "5984.1");
        t0.insert("6", "'周杰'", "24", "3829.5");
        t0.insert("10", "'吴强'", "27", "4276.0");
        t0.insert("11", "'郑爽'", "20", "3542.9");

        new TableFrame(t0);




    }

    public static void main(String[] args) {
        LinkedHashMap<String, Table.TableColumn> columnHashMap = new LinkedHashMap<>();
        columnHashMap.put("编号", new Table.TableColumn(Integer.class,(short)0,false,false,false));
        columnHashMap.put("名字", new Table.TableColumn(String.class,(short) 5,false,false,false));
        columnHashMap.put("年龄", new Table.TableColumn(Integer.class,(short) 0,false,true,false));
        columnHashMap.put("工资", new Table.TableColumn(Float.class,(short) 0,false,false,false));
        Table t0 = new Table("t0",columnHashMap,"编号");

        new TableFrame(t0);
        t0.insert("68", "'伟'", "105", "890.12");
        t0.insert("69", "'秀英'", "48", "901.23");
        t0.insert("70", "'娜磊'", "9", "1234.56");
        t0.insert("71", "'敏洋'", "114", "2345.67");
        t0.insert("72", "'丽小明'", "57", "3456.78");
        t0.insert("1", "'王伟'", "45", "3824.15");
        t0.insert("2", "'李敏'", "78", "5678.92");
        t0.insert("3", "'张芳'", "23", "4218.7");
        t0.insert("19", "'小明'", "7", "1357.91");
        t0.insert("85", "'王磊'", "72", "789.01");
        t0.insert("86", "'李洋'", "15", "890.12");
        t0.insert("87", "'张小明'", "99", "901.23");
        t0.insert("88", "'刘小红'", "42", "1234.56");
        t0.insert("24", "'志强'", "98", "6802.46");
        t0.insert("25", "'海燕'", "42", "7913.57");
        t0.insert("20", "'小红'", "116", "2468.1");
        t0.insert("21", "'建国'", "49", "5791.35");
        t0.insert("22", "'建军'", "72", "8024.68");
        t0.insert("61", "'陈建军'", "30", "123.45");
        t0.insert("43", "'敏洋'", "42", "123.45");
        t0.insert("44", "'丽小明'", "75", "234.56");
        t0.insert("45", "'磊小红'", "18", "345.67");
        t0.insert("46", "'洋建国'", "93", "456.78");
        t0.insert("23", "'桂花'", "15", "3579.13");
        t0.insert("76", "'小红桂花'", "21", "7890.12");
        t0.insert("77", "'建国志强'", "66", "8901.23");
        t0.insert("81", "'海燕'", "96", "345.67");
        t0.insert("4", "'刘静'", "56", "7321.43");
        t0.insert("5", "'陈洋'", "12", "1987.65");
        t0.insert("12", "'伟'", "54", "7890.12");
        t0.insert("13", "'秀英'", "18", "3456.78");
        t0.insert("14", "'娜'", "93", "5678.9");
        t0.insert("82", "'大勇建国'", "39", "456.78");
        t0.insert("83", "'美丽'", "84", "567.89");
        t0.insert("84", "'建国'", "17", "678.9");
        t0.insert("26", "'大勇'", "105", "9135.79");
        t0.insert("27", "'美丽'", "63", "2468.02");
        t0.insert("28", "'建国'", "21", "4680.24");
        t0.insert("29", "'王磊'", "87", "5791.35");
        t0.insert("30", "'李洋'", "36", "6802.46");
        t0.insert("31", "'张小明'", "3", "7913.57");
        t0.insert("32", "'刘小红'", "114", "8024.68");
        t0.insert("78", "'建军海燕'", "3", "9012.34");
        t0.insert("79", "'桂花大勇'", "108", "123.45");
        t0.insert("80", "'志强美丽'", "51", "234.56");
        t0.insert("39", "'芳美丽'", "33", "6789.01");
        t0.insert("40", "'伟建国'", "66", "7890.12");
        t0.insert("41", "'秀英'", "9", "8901.23");
        t0.insert("50", "'建军海燕'", "108", "890.12");
        t0.insert("15", "'敏静'", "27", "4321.09");
        t0.insert("16", "'丽强'", "61", "8765.43");
        t0.insert("17", "'磊'", "39", "6543.21");
        t0.insert("18", "'洋'", "84", "9876.54");
        t0.insert("51", "'桂花大勇'", "51", "901.23");
        t0.insert("52", "'志强美丽'", "84", "1234.56");
        t0.insert("33", "'陈建国'", "57", "9135.79");
        t0.insert("34", "'杨建军'", "90", "1234.56");
        t0.insert("42", "'娜磊'", "111", "9012.34");
        t0.insert("54", "'大勇建国'", "72", "3456.78");
        t0.insert("55", "'美丽'", "39", "4567.89");
        t0.insert("56", "'建国'", "96", "5678.9");
        t0.insert("47", "'小明建军'", "27", "567.89");
        t0.insert("48", "'小红桂花'", "60", "678.9");
        t0.insert("49", "'建国志强'", "3", "789.01");
        t0.insert("35", "'赵桂花'", "24", "2345.67");
        t0.insert("36", "'黄志强'", "81", "3456.78");
        t0.insert("37", "'周海燕'", "48", "4567.89");
        t0.insert("38", "'吴大勇'", "99", "5678.9");
        t0.insert("53", "'海燕'", "17", "2345.67");
        t0.insert("62", "'杨桂花'", "81", "234.56");
        t0.insert("63", "'赵志强'", "24", "345.67");
        t0.insert("64", "'黄海燕'", "69", "456.78");
        t0.insert("65", "'周大勇'", "12", "567.89");
        t0.insert("57", "'王洋'", "21", "6789.01");
        t0.insert("58", "'李小明'", "63", "7890.12");
        t0.insert("59", "'张小红'", "6", "8901.23");
        t0.insert("60", "'刘建国'", "117", "9012.34");
        t0.insert("66", "'吴美丽'", "93", "678.9");
        t0.insert("67", "'芳'", "36", "789.01");;
        t0.insert("73", "'磊小红'", "90", "4567.89");
        t0.insert("74", "'洋建国'", "33", "5678.9");
        t0.insert("75", "'小明建军'", "78", "6789.01");
        t0.insert("89", "'陈建国'", "87", "2345.67");
        t0.insert("90", "'杨建军'", "30", "3456.78");
        t0.insert("91", "'赵桂花'", "111", "4567.89");
        t0.insert("92", "'黄志强'", "54", "5678.9");
        t0.insert("93", "'周海燕'", "9", "6789.01");
        t0.insert("94", "'吴大勇'", "120", "7890.12");
        t0.insert("6", "'杨大勇'", "89", "8543.21");
        t0.insert("7", "'赵美丽'", "34", "3456.78");
        t0.insert("8", "'黄建国'", "67", "6789.32");
        t0.insert("9", "'周桂花'", "9", "1234.56");
        t0.insert("10", "'吴磊'", "102", "4567.89");
        t0.insert("11", "'芳'", "31", "2345.67");
        t0.insert("95", "'芳美丽'", "63", "8901.23");
        t0.insert("96", "'伟建国'", "6", "9012.34");
        t0.insert("97", "'秀英'", "93", "123.45");
        t0.insert("98", "'娜磊'", "36", "234.56");
        t0.insert("99", "'敏洋'", "105", "345.67");
        t0.insert("100", "'丽小明'", "48", "456.78");
        //897777777788888888888888789789
        //t0.allLeafPageMergeCheck();
        //new TableFrame(t0);
    }
}
