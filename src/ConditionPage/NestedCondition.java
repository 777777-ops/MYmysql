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

    //外接
    public List<Condition> IndexKeys(){return getIndexKeys();}

    //查询接口
    public Object[][] runSearch(){

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
        //第二部，从first中过滤掉不符合条件的  重复的
        HashMap<Integer,Object[]> map = new HashMap<>();
        for (Table.SearchResult searchResult : first) {
            Integer key = searchResult.node_prev + searchResult.page_offset;
            Object[] value = searchResult.values;
            //符合条件 并且 在map中没有重复
            if(ALL || (isEligible(value) && !map.containsKey(key))){
                map.put(key,value);
            }
        }

        return map.values().toArray(new Object[0][]);
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

    public static void main(String[] args) {
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
}
