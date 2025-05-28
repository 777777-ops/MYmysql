package ConditionPage;

import Memory.Table;

import java.util.List;


//目的是能否找出可以使用索引查找的条件！

public abstract class Condition {
    public Table table;

    //将字符串解析程序友好型的变量
    protected abstract void parseConditions(String input);

    //返回该条件下需要使用到的索引查询字段
    /*
        为空说明只能使用顺序查找，若太多说明都要查找，查找出后的结果一定要重新检查
    */
    protected abstract List<Condition> getIndexKeys();

    //给出一个数据数组，要求根据当前的条件返回数据是否符合条件
    protected abstract boolean isEligible(Object[] values);
}
