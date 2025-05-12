package ConditionPage;

import Memory.Record;
import Memory.Table;
import UI.MainFrame;
import UI.RootFrame;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//复杂条件
public class NestedCondition extends Condition{
    private final List<String> separator = new ArrayList<>();          //分隔符
    private final List<Condition> conditions = new ArrayList<>();      //条件集
    private boolean ALL;                                               //全部查询的标号


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
    protected List<Condition> getIndexKeys(){

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
    protected boolean isEligible(Object[] values){
        boolean result = conditions.get(0).isEligible(values);
        for (int i = 1; i < conditions.size(); i++) {
             boolean sep = separator.get(i - 1).equals("and");       //sep是true时说明是and
             if(!result && !sep) result = conditions.get(i).isEligible(values);
             if(result && sep) result = conditions.get(i).isEligible(values);
        }
        return result;
    }

    //筛选出符合条件的所有SearchResult
    private Collection<Table.SearchResult> filter(){
        List<Condition> indexs = getIndexKeys();
        List<Table.SearchResult> first = new ArrayList<>();
        //第一步，先从indexs中利用索引过滤出第一批结果
        if(indexs.isEmpty())        //没有索引可以利用  直接顺序
            first = table.searchLinked();
        else                        //有索引可以利用 但要区别主键索引和其他索引
        {
            for (Condition index : indexs) {
                SimpleCondition sc = (SimpleCondition) index;
                //主键索引
                if(sc.field.equals(table.getPrimaryKey()))
                    first.addAll(table.searchPrimary(sc.data, sc.operator));
                    //其他索引
                else {
                    /*TODO*/
                }
            }
        }
        //第二步，从first中过滤掉不符合条件的  重复的
        LinkedHashMap<Integer,Table.SearchResult> map = new LinkedHashMap<>();
        for (Table.SearchResult searchResult : first) {
            Integer key = searchResult.node_prev + searchResult.page_offset;
            //符合条件 并且 在map中没有重复
            if(ALL || (isEligible(searchResult.values) && !map.containsKey(key))){
                map.put(key,searchResult);
            }
        }
        return map.values();

    }

    //查询接口
    public Object[][] runSearch(){
        Collection<Table.SearchResult> filter = filter();
        Object[][] result = new Object[filter.size()][];
        int index = 0;
        for (Table.SearchResult sr : filter) {
            result[index++] = sr.values;
        }
        return result;
    }

    //删除接口
    public void runDelete(){

        /*TODO*/  //要实现主键删除
        Collection<Table.SearchResult> filter = filter();
        for (Table.SearchResult sr : filter)
            table.delete(sr);
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

        new MainFrame(t0);
    }

    public static void main(String[] args) {
        LinkedHashMap<String, Table.TableColumn> columnHashMap = new LinkedHashMap<>();
        columnHashMap.put("编号", new Table.TableColumn(Integer.class,(short)0,false,true,false));
        columnHashMap.put("名字", new Table.TableColumn(String.class,(short) 5,false,false,false));
        columnHashMap.put("年龄", new Table.TableColumn(Integer.class,(short) 0,false,false,false));
        columnHashMap.put("工资", new Table.TableColumn(Float.class,(short) 0,false,false,false));
        Table t0 = new Table("t0",columnHashMap,"编号");
        // 1-20
        t0.insert("1", "'王芳'", "23", "4218.7");
        t0.insert("2", "'李明'", "28", "3895.2");
        t0.insert("3", "'张伟'", "25", "2763.9");
        t0.insert("4", "'赵燕'", "30", "5321.6");
        t0.insert("5", "'陈晨'", "22", "3147.8");
        t0.insert("6", "'刘洋'", "26", "4672.3");
        t0.insert("7", "'孙莉'", "24", "5984.1");
        t0.insert("8", "'周杰'", "27", "3829.5");
        t0.insert("9", "'吴强'", "29", "4276.0");
        t0.insert("10", "'郑爽'", "21", "3542.9");
        t0.insert("11", "'林涛'", "31", "4837.2");
        t0.insert("12", "'杨光'", "20", "3956.4");
        t0.insert("13", "'朱琳'", "32", "5128.3");
        t0.insert("14", "'秦朗'", "19", "3267.5");
        t0.insert("15", "'许静'", "33", "4569.8");
        t0.insert("16", "'何军'", "18", "2873.1");
        t0.insert("17", "'黄薇'", "34", "4982.7");
        t0.insert("18", "'马超'", "17", "3675.9");
        t0.insert("19", "'谢娜'", "35", "5432.6");
        t0.insert("20", "'董健'", "16", "4123.4");

// 21-40
        t0.insert("21", "'萧雨'", "36", "4765.3");
        t0.insert("22", "'冯刚'", "15", "3245.7");
        t0.insert("23", "'韩梅'", "37", "5876.2");
        t0.insert("24", "'邓辉'", "14", "2987.4");
        t0.insert("25", "'曹阳'", "38", "5123.9");
        t0.insert("26", "'彭丽'", "13", "3567.8");
        t0.insert("27", "'曾敏'", "39", "4987.1");
        t0.insert("28", "'苏强'", "12", "4123.5");
        t0.insert("29", "'潘婷'", "40", "5342.6");
        t0.insert("30", "'葛军'", "11", "3765.4");
        t0.insert("31", "'范伟'", "41", "4876.3");
        t0.insert("32", "'任静'", "10", "3245.9");
        t0.insert("33", "'袁芳'", "42", "5123.7");
        t0.insert("34", "'于洋'", "9", "3987.2");
        t0.insert("35", "'蒋明'", "43", "4765.8");
        t0.insert("36", "'蔡琳'", "8", "3456.1");
        t0.insert("37", "'余辉'", "44", "5234.9");
        t0.insert("38", "'杜刚'", "7", "3876.3");
        t0.insert("39", "'叶婷'", "45", "4987.5");
        t0.insert("40", "'程强'", "6", "4123.6");

// 41-60
        t0.insert("41", "'魏薇'", "46", "5342.8");
        t0.insert("42", "'吕明'", "5", "3567.9");
        t0.insert("43", "'丁静'", "47", "4876.4");
        t0.insert("44", "'崔健'", "4", "3987.1");
        t0.insert("45", "'钟华'", "48", "5123.5");
        t0.insert("46", "'谭芳'", "3", "4123.7");
        t0.insert("47", "'姜涛'", "49", "5342.9");
        t0.insert("48", "'毛敏'", "2", "3765.2");
        t0.insert("49", "'江明'", "50", "4987.3");
        t0.insert("50", "'史强'", "1", "4123.8");
        t0.insert("51", "'顾婷'", "51", "5234.7");
        t0.insert("52", "'侯杰'", "52", "4567.9");
        t0.insert("53", "'邵静'", "53", "4876.5");
        t0.insert("54", "'龙伟'", "54", "5123.4");
        t0.insert("55", "'万芳'", "55", "5342.3");
        t0.insert("56", "'段刚'", "56", "4765.7");
        t0.insert("57", "'雷明'", "57", "4987.6");
        t0.insert("58", "'钱静'", "58", "5123.2");
        t0.insert("59", "'汤强'", "59", "5342.1");
        t0.insert("60", "'尹薇'", "60", "4876.8");

// 61-80
        t0.insert("61", "'黎华'", "61", "5123.9");
        t0.insert("62", "'易明'", "62", "4765.6");
        t0.insert("63", "'常静'", "63", "4987.7");
        t0.insert("64", "'武刚'", "64", "5234.5");
        t0.insert("65", "'乔芳'", "65", "5342.4");
        t0.insert("66", "'赖伟'", "66", "4876.9");
        t0.insert("67", "'龚敏'", "67", "5123.1");
        t0.insert("68", "'文强'", "68", "4765.8");
        t0.insert("69", "'欧阳'", "69", "4987.2");
        t0.insert("70", "'司马'", "70", "5234.3");
        t0.insert("71", "'上官'", "71", "5342.7");
        t0.insert("72", "'诸葛'", "72", "4876.1");
        t0.insert("73", "'东方'", "73", "5123.8");
        t0.insert("74", "'尉迟'", "74", "4765.9");
        t0.insert("75", "'令狐'", "75", "4987.4");
        t0.insert("76", "'慕容'", "76", "5234.2");
        t0.insert("77", "'宇文'", "77", "5342.5");
        t0.insert("78", "'端木'", "78", "4876.7");
        t0.insert("79", "'皇甫'", "79", "5123.6");
        t0.insert("80", "'独孤'", "80", "4765.3");

// 81-100
        t0.insert("81", "'公孙'", "81", "4987.9");
        t0.insert("82", "'仲孙'", "82", "5234.8");
        t0.insert("83", "'轩辕'", "83", "5342.2");
        t0.insert("84", "'长孙'", "84", "4876.4");
        t0.insert("85", "'司徒'", "85", "5123.3");
        t0.insert("86", "'司空'", "86", "4765.1");
        t0.insert("87", "'鲜于'", "87", "4987.8");
        t0.insert("88", "'闾丘'", "88", "5234.6");
        t0.insert("89", "'子车'", "89", "5342.9");
        t0.insert("90", "'亓官'", "90", "4876.2");
        t0.insert("91", "'司寇'", "91", "5123.7");
        t0.insert("92", "'巫马'", "92", "4765.4");
        t0.insert("93", "'公西'", "93", "4987.1");
        t0.insert("94", "'漆雕'", "94", "5234.9");
        t0.insert("95", "'乐正'", "95", "5342.8");
        t0.insert("96", "'壤驷'", "96", "4876.3");
        t0.insert("97", "'公良'", "97", "5123.4");
        t0.insert("98", "'拓跋'", "98", "4765.5");
        t0.insert("99", "'夹谷'", "99", "4987.6");
        t0.insert("100", "'宰父'", "100", "5234.1");
        //897777777788888888888888789789
        new MainFrame(t0);
        //t0.allLeafPageMergeCheck();
        //new MainFrame(t0);
    }
}
