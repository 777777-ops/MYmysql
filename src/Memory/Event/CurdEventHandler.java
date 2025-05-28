package Memory.Event;


import ConditionPage.Condition;
import ConditionPage.NestedCondition;
import ConditionPage.SimpleCondition;
import Memory.Page;
import Memory.PageLeaf;
import Memory.PageNoLeaf;
import Memory.Table;

import java.util.*;

public abstract class CurdEventHandler<T extends CurdEvent> implements EventHandler<T> {
    protected Table table;
    public CurdEventHandler(Table table){this.table = table;}

    //添加操作
    public static class insertHandler extends CurdEventHandler<CurdEvent.insertEvent>{
        public insertHandler(Table table){super(table);}
        @Override
        public void handle(CurdEvent.insertEvent event) {
            insertRec(event.getData());
        }

        //插入一条新记录
        private void insertRec(Object[] values){

            //从table中获取独特标识
            int heap_no = table.getDefault_key(1);
            //获取主键
            Object index_key;
            if(table.getPrimaryKey() == null) index_key = heap_no;
            else index_key = values[table.getFieldIndex(table.getPrimaryKey())];
            //定位到叶子页 (给出叶子页的路径)
            CurdEvent.selectWayEvent wayEvent = new CurdEvent.selectWayEvent(index_key);
            table.eventBus.execute(wayEvent);                           //调用查询线程
            Stack<Table.Pair> stack = wayEvent.getResult();
            PageLeaf leaf = (PageLeaf) table.getPage(stack.peek().page_offset);
            //page现在是叶子页了
            if(leaf.contain(index_key))  throw new RuntimeException("该表中已有该主键");
            //插入
            leaf.insert(values,heap_no,index_key);
            //需要页分裂
            if(leaf.checkPageSplit()){
                PageManagerEvent.SpiltEvent event = new PageManagerEvent.SpiltEvent(stack);
                table.eventBus.execute(event);
            }

        }

        @Override
        public Class<CurdEvent.insertEvent> getEventType() {return CurdEvent.insertEvent.class;}

    }

    //删除操作
    public static class deleteHandler extends CurdEventHandler<CurdEvent.deleteEvent>{
        NestedCondition ns;
        public deleteHandler(Table table){super(table);}

        /*根据索引删除可以优化！*/
        @Override
        public void handle(CurdEvent.deleteEvent event) {
            ns = new NestedCondition(event.getConditions(),table);
            //观察是否可以直接利用主键
            List<Condition> indexs = ns.getIndexKeys();
            if(indexs.size() == 1 && indexs.get(0) instanceof SimpleCondition sc){
                if(sc.field.equals(table.getPrimaryKey())){
                    deleteByPrimary(sc.data,sc.operator);
                }
                //非聚集索引/*TODO*/
            }
            //无法使用单索引
            else deleteByLinked();
        }

        //根据主键删除记录    总控制端
        private void deleteByPrimary(Object[] index_key,String operator){
            switch (operator) {
                case ">=" -> deleteOneSide(index_key[0],1);
                case "<=" -> deleteOneSide(index_key[0],2);
                case "==" -> deleteDoubleSide(index_key[0],index_key[0]);
                default -> deleteDoubleSide(index_key[0], index_key[1]);
            }

        }

        //根据主键删除记录   （带范围的）  between and
        private void deleteDoubleSide(Object index_key_begin,Object index_key_end){
            int[] arr;         //本层的删除操作
            Page left = table.getRoot();
            Page right;
            arr = left.delete(index_key_begin,index_key_end);
            while(arr.length != 0 && arr[0] != 0){
                //到下一层
                left = table.getPage(arr[0]);
                right = table.getPage(arr[1]);
                //缓存主键  并且更改前后页
                if(left.getPage_level() == 0) {
                    table.deleteMap.putIfAbsent(arr[0], index_key_begin);
                    table.deleteMap.putIfAbsent(arr[1], index_key_end);
                    if(left != right && ((PageLeaf)left).page_next_offset != right.getPage_offset()){
                        ((PageLeaf)left).page_next_offset = right.getPage_offset();
                        ((PageLeaf)right).page_prev_offset = left.getPage_offset();
                    }
                }

                if(left == right)
                    arr = left.delete(index_key_begin,index_key_begin);
                else {
                    arr[0] = left.deleteOneSide(index_key_begin,1);
                    arr[1] = right.deleteOneSide(index_key_end,2);
                }
            }
        }

        //根据主键删除记录    >=  <=
        private void deleteOneSide(Object index_key,int model){
            Page page = table.getRoot();
            //开始遍历
            int down_page = page.deleteOneSide(index_key,model);
            while(down_page != 0){
                if(page.getPage_level() == 1)
                    table.deleteMap.putIfAbsent(down_page, index_key);
                page = table.getPage(down_page);
                down_page = page.deleteOneSide(index_key,model);
            }
            if(model == 1) ((PageLeaf)page).page_next_offset = 0;
            if(model == 2) ((PageLeaf)page).page_prev_offset = 0;
        }

        //根据条件遍历删除
        public void deleteByLinked(){
            //获取第一页
            CurdEvent.selectWayEvent event = new CurdEvent.selectWayEvent(null);
            table.eventBus.execute(event);
            PageLeaf leaf = (PageLeaf)table.getPage(event.getResult().pop().page_offset);
            //遍历
            while(true){
                int prt = leaf.getNextOffset(Page.MIN);
                while(prt != Page.MAX){
                    int next = leaf.getNextOffset(prt);   //提前获取下一个节点的偏移量
                    //符合条件的就删除
                    Object[] values = leaf.getValues(prt);
                    Object index_key = leaf.getIndex_key(prt);
                    if(ns.isEligible(values)){
                        leaf.offsetDelete(prt,2);
                        table.deleteMap.putIfAbsent(leaf.getPage_offset(),index_key);   //缓存池(页合并)
                    }
                    prt = next;
                }
                if(leaf.page_next_offset == 0)
                    break;
                leaf = (PageLeaf) table.getPage(leaf.page_next_offset);
            }
        }

        @Override
        public Class<CurdEvent.deleteEvent> getEventType() {
            return CurdEvent.deleteEvent.class;
        }
    }

    //查询操作
    public static class selectHandler extends CurdEventHandler<CurdEvent.selectEvent>{
        NestedCondition ns;
        public selectHandler(Table table){super(table);}

        @Override
        public void handle(CurdEvent.selectEvent event) {
            ns = new NestedCondition(event.getConditions(),table);
            event.complete(selectFilter());
        }

        //查询（双层过滤）
        private Collection<Table.SearchResult> selectFilter(){
            List<Condition> indexs = ns.getIndexKeys();
            List<Table.SearchResult> first = new ArrayList<>();
            //第一步，先从indexs中利用索引过滤出第一批结果
            if(indexs.isEmpty())        //没有索引可以利用  直接顺序
                first = searchLinked();
            else                        //有索引可以利用 但要区别主键索引和其他索引
            {
                for (Condition index : indexs) {
                    SimpleCondition sc = (SimpleCondition) index;
                    //主键索引
                    if(sc.field.equals(table.getPrimaryKey()))
                        first.addAll(searchPrimary(sc.data, sc.operator));
                        //其他索引
                    else {
                        /*TODO*/
                    }
                }
            }
            //第二步，从first中过滤掉不符合条件的  重复的
            LinkedHashMap<Integer,Table.SearchResult> map = new LinkedHashMap<>();
            for (Table.SearchResult searchResult : first) {
                Integer key = searchResult.offset + searchResult.page_offset;
                //符合条件 并且 在map中没有重复
                if(ns.ALL || (ns.isEligible(searchResult.values) && !map.containsKey(key))){
                    map.put(key,searchResult);
                }
            }
            return map.values();

        }

        //根据主键查询某一记录的信息   （B+）
        private List<Table.SearchResult> searchPrimary(Object[] index_key, String operator){
            Page page = table.getRoot();
            while(page instanceof PageNoLeaf p)
            {
                int prev = p.Search(index_key[0]);     //所要进入页的前节点
                int next = p.getNextOffset(prev);   //所要进入页的后节点
                //如果next节点等于index_key  prev和next全部后移
                if(p.compare(index_key[0],next) == 0){prev = next ;next = p.getNextOffset(next);}
                page = table.getPage(p.getLeftPage(next));
            }
            PageLeaf leaf = ((PageLeaf)page);
            Table.SearchResult head = leaf.searchValues(index_key[0]);      //头节点
            List<Table.SearchResult> result = new ArrayList<>();         //结果
            switch (operator){
                case "==": {
                    int prt = head.offset;
                    if (leaf.compare(index_key[0], prt) == 0) result.add(head);
                    return result;
                }
                case ">=": {
                    int page_offset = head.page_offset;
                    while (page_offset != 0) {
                        leaf = (PageLeaf) table.getPage(page_offset);
                        int prt;
                        if(page_offset == head.page_offset) prt = head.offset;
                        else prt = leaf.getNextOffset(Page.MIN);
                        while (prt != Page.MAX) {
                            result.add(new Table.SearchResult(page_offset,prt,leaf.getValues(prt)));
                            prt = leaf.getNextOffset(prt);
                        }
                        page_offset = leaf.page_next_offset;
                    }
                    return result;
                }
                case "<=":{
                    CurdEvent.selectWayEvent event = new CurdEvent.selectWayEvent(null);
                    table.eventBus.execute(event);
                    int page_offset = event.getResult().pop().page_offset;
                    while (page_offset != head.page_offset){
                        leaf = (PageLeaf) table.getPage(page_offset);
                        int prt = leaf.getNextOffset(Page.MIN);
                        while (prt != Page.MAX) {
                            result.add(new Table.SearchResult(page_offset,prt,leaf.getValues(prt)));
                            prt = leaf.getNextOffset(prt);
                        }
                        page_offset = leaf.page_next_offset;
                    }
                    //此时的page_offset到了head页
                    leaf = (PageLeaf) table.getPage(page_offset);
                    int prt = leaf.getNextOffset(Page.MIN);
                    while(prt != head.offset){
                        result.add(new Table.SearchResult(page_offset,prt,leaf.getValues(prt)));
                        prt = leaf.getNextOffset(prt);
                    }
                    //此时prt就是目标节点 需判断是否 ==
                    if (leaf.compare(index_key, prt) == 0) result.add(head);
                    return result;
                }
                case "to":{
                    int page_offset = head.page_offset;
                    int prt = head.offset;
                    while(leaf.compare(index_key[1],prt) >= 0){
                        result.add(new Table.SearchResult(page_offset,prt,leaf.getValues(prt)));
                        prt = leaf.getNextOffset(prt);
                        if(prt == Page.MAX){
                            page_offset = leaf.page_next_offset;  if(page_offset == 0) break;
                            leaf = (PageLeaf) table.getPage(page_offset);
                            prt = leaf.getNextOffset(Page.MIN);
                        }
                    }
                    return  result;
                }
                default: throw new RuntimeException("不可能到达的语句");
            }

        }

        //所有的行数据全部都呈现出来  （线性）
        private List<Table.SearchResult> searchLinked(){
            List<Table.SearchResult> result = new ArrayList<>();
            CurdEvent.selectWayEvent event = new CurdEvent.selectWayEvent(null);
            table.eventBus.execute(event);
            int page_offset = event.getResult().pop().page_offset;
            while(page_offset != 0){
                PageLeaf leaf = (PageLeaf) table.getPage(page_offset);
                int prt = leaf.getNextOffset(Page.MIN);
                while(prt != Page.MAX){
                    result.add(new Table.SearchResult(page_offset,prt,leaf.getValues(prt)));
                    prt = leaf.getNextOffset(prt);
                }
                page_offset = leaf.page_next_offset;
            }
            return result;
        }

        @Override
        public Class<CurdEvent.selectEvent> getEventType() {return CurdEvent.selectEvent.class;}
    }

    //查询路径操作
    public static class selectWayHandler extends CurdEventHandler<CurdEvent.selectWayEvent>{
        public selectWayHandler(Table table){super(table);}

        @Override
        public void handle(CurdEvent.selectWayEvent event) {
            int model = event.getModel();
            switch (model){
                case 1 -> event.complete(indexKeyFindWay(event.getIndex_key()));
                case 2 -> event.complete(getFirstPageOffset());
                case 3 -> event.complete(pageOffsetIndexFindWay(event.getIndex_key(),event.getPage_offset()));
            }
        }

        //根据主键找路径
        private Stack<Table.Pair> indexKeyFindWay(Object index_key){
            Stack<Table.Pair> stack = new Stack<>();
            Page page = table.getRoot();
            while (page instanceof PageNoLeaf p){
                //将该层的页保存
                int prev = p.Search(index_key);
                int prt = p.getNextOffset(prev);
                if(p.compare(index_key,prt) == 0)  prt = p.getNextOffset(prt);
                stack.push(new Table.Pair(page.getPage_offset(),prt));
                //进入下一层
                int page_offset = p.getLeftPage(prt);
                page = table.getPage(page_offset);
            }
            stack.push(new Table.Pair(page.getPage_offset(),0));
            return stack;
        }

        //给出主键 和 页地址 根据B+树结构找到该主键所在页的路径
        private Stack<Table.Pair> pageOffsetIndexFindWay(Object index_key, int target_offset){
            Stack<Table.Pair> stack = indexKeyFindWay(index_key);
            Page page = table.getPage(stack.peek().page_offset);
            //出现了偏差
            if(page.getPage_offset() != target_offset){
                Page target = table.getPage(target_offset);
                //如果目标页没有节点   寻求左页帮忙
                if(target.getPage_num() == 0){
                    stack = pageOffsetFindWay(target_offset);
                }else{
                    stack = indexKeyFindWay(target.getIndex_key(target.getNextOffset(Page.MIN)));
                }
            }
            return stack;
        }

        //----辅助方法---以下-----//

        //仅给出叶子页地址  返回目标路径  (左)   //调用该方法的前提是该页确定无节点
        private Stack<Table.Pair> pageOffsetFindWay(int target_offset){
            PageLeaf target = (PageLeaf) table.getPage(target_offset);
            //从左页找起
            int prev = target.page_prev_offset;
            while(prev != 0){
                target = (PageLeaf) table.getPage(prev);
                if(target.getPage_offset() > 0) break;
                //遍历
                prev = target.page_prev_offset;
            }
            //找不到匹配的左页
            if(prev == 0) {
                //组一个第一页的路径
                Page page = table.getRoot();
                Stack<Table.Pair> stack = new Stack<>(); int prt = page.getNextOffset(Page.MIN);
                while(page instanceof PageNoLeaf p){
                    stack.push(new Table.Pair(page.getPage_offset(),prt));
                    page = table.getPage(p.getLeftPage(prt));
                    prt = page.getNextOffset(Page.MIN);
                }
                stack.push(new Table.Pair(page.getPage_offset(),0));
                return pageOffsetLeftFindWay(stack,target_offset);
            }
            else {
                Object index_key = target.getIndex_key(target.getNextOffset(Page.MIN));
                return pageOffsetLeftFindWay(indexKeyFindWay(index_key),target_offset);
            }

        }

        //给出一条完整的页路径  向右遍历找出目标路径
        private Stack<Table.Pair> pageOffsetLeftFindWay(Stack<Table.Pair> stack, int target_offset){
            Page page = table.getPage(stack.pop().page_offset);
            int prt = Page.MIN;
            while (true){
                //到达叶子层
                while(page instanceof PageNoLeaf p){
                    stack.push(new Table.Pair(page.getPage_offset(),prt));
                    page = table.getPage(p.getLeftPage(prt));
                    prt = page.getNextOffset(Page.MIN);
                }
                prt = Page.MAX;
                //判断是否相等
                if(page.getPage_offset() == target_offset) {
                    stack.push(new Table.Pair(page.getPage_offset(),0));
                    return stack;
                }
                while(prt == Page.MAX && !stack.isEmpty()){
                    Table.Pair pair = stack.pop();
                    page = table.getPage(pair.page_offset);
                    prt = pair.node_offset;
                    prt = page.getNextOffset(prt);   //下一个节点
                }
                if(prt == Page.MAX && stack.isEmpty())
                    break;
            }
            throw new RuntimeException("无法找到相应的叶子页");
        }

        //获取一张表中在顺序逻辑上的第一页
        private Stack<Table.Pair> getFirstPageOffset(){
            Page page = table.getRoot();
            Stack<Table.Pair> result = new Stack<>();
            while(page instanceof PageNoLeaf p){
                int prt = p.getNextOffset(Page.MIN);
                result.push(new Table.Pair(page.getPage_offset(),prt));
                int page_offset = p.getLeftPage(prt);
                page = table.getPage(page_offset);
            }
            result.push(new Table.Pair(page.getPage_offset(),0));
            return result;
        }

        //----辅助方法---以上-----//
        @Override
        public Class<CurdEvent.selectWayEvent> getEventType() {return CurdEvent.selectWayEvent.class;}
    }

}
