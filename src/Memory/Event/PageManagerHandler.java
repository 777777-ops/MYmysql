package Memory.Event;

import Memory.*;

import java.util.List;
import java.util.Map;
import java.util.Stack;

public abstract class PageManagerHandler<T extends PageManagerEvent> implements EventHandler<T>{
    protected Table table;
    public PageManagerHandler(Table table){this.table = table;}

    //分裂操作
    public static class SpiltHandler extends PageManagerHandler<PageManagerEvent.SpiltEvent>{
        public SpiltHandler(Table table){super(table);}


        @Override
        public void handle(PageManagerEvent.SpiltEvent event) {
            SplitTheWay(event.getWay());
        }

        //整条路径上分裂
        private void SplitTheWay(Stack<Table.Pair> stack){
            Page page = table.getPage(stack.pop().page_offset);
            while(page.checkPageSplit()){
                PageNoLeaf parent;   int prt = Page.PAGE_HEAD;
                if(stack.isEmpty()) {
                    parent = (PageNoLeaf) table.insertPage((byte)(page.getPage_level() + 1));
                    table.setRoot(parent);
                }
                else{
                    Table.Pair pair = stack.pop(); parent = (PageNoLeaf)table.getPage(pair.page_offset);
                    prt= pair.node_offset;
                }
                //分裂
                Page rightPage = table.insertPage(page.getPage_level());
                SplitPage(parent,prt,page,rightPage);
                page = parent;           //page跳转至父页
            }
        }

        //分裂
        private void SplitPage(PageNoLeaf parent, int parent_offset,Page page,Page rightPage){
            //对半分裂
            int n = page.getPage_num()/2;
            int prt = page.getNextOffset(Page.MIN);    //本页的第一个节点
            byte[] this_page_bytes = new byte[0];
            //拼接所有的本页的前一半节点
            while(n-- > 0){
                this_page_bytes = ByteTools.concatBytes(this_page_bytes,page.getBytes(prt));
                prt = page.getNextOffset(prt);
            } //现在的prt还未拼接

            //向父页中插入新节点
            parent.insert(parent_offset,page.getIndex_key_bytes(prt),page.getPage_offset(),rightPage.getPage_offset());
            //检查本页是否为非叶子页
            if(page instanceof PageNoLeaf){
                page.setType(prt,(byte)0x11);    //将中间节点转化为辅助节点插入本页新更新的缓冲数组中
                this_page_bytes = ByteTools.concatBytes(this_page_bytes,page.getBytes(prt));
                prt = page.getNextOffset(prt);
            }

            //拼接本页的后一半节点
            byte[] new_page_bytes = new byte[0];
            while(prt != Page.MAX){
                new_page_bytes = ByteTools.concatBytes(new_page_bytes,page.getBytes(prt));          //将本节点的字节数组连接
                prt = page.getNextOffset(prt);
            }
            //将本页中移出的数据插入新页中
            rightPage.resetAllBuffer(new_page_bytes);
            //需要更新原叶子页后面的那个页
            if(page instanceof PageLeaf leaf){
                int right = rightPage.getPage_offset();   //右页偏移量
                int offset = leaf.getPage_offset();       //本页偏移量
                //需要更新原叶子页后面的那个页
                if(leaf.page_next_offset != 0){
                    PageLeaf next = (PageLeaf) table.getPage(leaf.page_next_offset);
                    next.page_prev_offset = right;
                    ((PageLeaf)rightPage).page_next_offset = next.getPage_offset();
                }
                leaf.page_next_offset = right;
                ((PageLeaf)rightPage).page_prev_offset = offset;
            }
            //如果本页是表的根页  那么将根页转交给父页
            if(table.getRoot() == page) table.setRoot(parent);
            //更新本页缓冲数组
            page.resetAllBuffer(this_page_bytes);
        }

        @Override
        public Class<PageManagerEvent.SpiltEvent> getEventType() {
            return PageManagerEvent.SpiltEvent.class;
        }
    }

    //合并操作
    public static class MergeHandler extends PageManagerHandler<PageManagerEvent.MergeEvent>{

        public MergeHandler(Table table){super(table);}

        @Override
        public void handle(PageManagerEvent.MergeEvent event) {
            allLeafPageMergeCheck();
        }

        //根据删除日志，进行页合并
        private void allLeafPageMergeCheck(){
            Stack<Table.Pair> stack;
            Page page;
            //遍历每一个已经删除的主键
            for (Map.Entry<Integer, Object> map : table.deleteMap.entrySet()) {
                if(table.getPage(map.getKey()).getPage_level() < 0) continue; /*TODO*/  //page_level < 0代表已被删除

                Object index_key = map.getValue();
                //根据被删除的主键查找页路径  可能会有偏差需要方法调整
                CurdEvent.selectWayEvent event = new CurdEvent.selectWayEvent(index_key,map.getKey());
                table.eventBus.execute(event);
                stack = event.getResult();
                page = table.getPage(stack.pop().page_offset);
                if(page.getPage_offset() != map.getKey()){throw new RuntimeException("...");}
                //从叶子页层层递归上去检查是否有需要合并的页
                while (page.checkPageMerge()) {
                    pageMergeControl(page,stack);
                    page = table.getPage(stack.pop().page_offset);
                }

                stack.clear();
            }
            while(table.getRoot().getPage_num() == 1 && table.getRoot() instanceof PageNoLeaf p){
                table.setRoot(table.getPage(p.getLeftPage(p.getNextOffset(Page.MIN))));
            }
            //清空
            table.deleteMap.clear();
        }

        //---辅助方法---以下--//

        //页合并的控制端
        private boolean pageMergeControl(Page page,Stack<Table.Pair> stack){
            //PageNoLeaf ancestor;   int ancestor_offset;
            Table.Pair pair = stack.peek();
            PageNoLeaf parent = (PageNoLeaf) table.getPage(pair.page_offset);
            int prev = parent.getPrevOffset(pair.node_offset);   //page页节点的前节点
            int next = parent.getNextOffset(pair.node_offset);   //page页节点的后节点
            Page neighbor = null;                                                 //page页的邻居页
            //开始找邻居页
            if(page.getPage_num() == 0) {pageMerge(page,pair,null,3,null,0); return true;} //无需寻找邻居页，直接删除
            //父母页中只有一个工具节点的情况  寻找左邻居和右邻居都比较麻烦
            if(prev == Page.MIN && next == Page.MAX){
                return extremeLeftNeighbor(page,stack) || extremeRightNeighbor(page,stack);
            }
            //左极端情况  寻找左邻居麻烦
            else if(prev == Page.MIN){
                neighbor = table.getPage(parent.getLeftPage(next));
                if(neighbor.getPage_num() + page.getPage_num() < 5){
                    pageMerge(page,pair,neighbor,2,null,0);  return true;
                }
                //右邻居失败 找左邻居
                return extremeLeftNeighbor(page,stack);
            }
            //右极端情况  寻找右邻居麻烦
            else if(next == Page.MAX){
                neighbor = table.getPage(parent.getLeftPage(prev));
                if(neighbor.getPage_num() + page.getPage_num() < 5){
                    pageMerge(page,pair,neighbor,1,parent,prev);  return true;
                }
                //左邻居失败 找右邻居
                return extremeRightNeighbor(page,stack);
            }
            //页节点在中间的情况，最为普遍,处理最简单
            else{
                neighbor = table.getPage(parent.getLeftPage(next));
                if(neighbor.getPage_num() + page.getPage_num() < 5){
                    pageMerge(page,pair,neighbor,2,null,0);  return true;
                }
                neighbor = table.getPage(parent.getLeftPage(prev));
                if(neighbor.getPage_num() + page.getPage_num() < 5){
                    pageMerge(page,pair,neighbor,1,parent,prev);  return true;
                }
                return false;
            }
        }

        //处理 找邻居页中的极端左情况    是pageMerge的辅助方法
        private boolean extremeLeftNeighbor(Page page,Stack<Table.Pair> stack){
            Stack<Table.Pair> s = new Stack<>();  s.addAll(stack);         //克隆一个stack栈来使用
            Table.Pair pair = s.pop();
            PageNoLeaf parent = (PageNoLeaf) table.getPage(pair.page_offset);         //page的父母页
            int prt = pair.node_offset;                                            //父母页中当前页节点前节点
            int prev = parent.getPrevOffset(prt);
            PageNoLeaf ancestor = null;                                                 //极端情况肯定有祖先页
            int ancestor_offset = 0;                                                  //祖先节点
            Page neighbor = null;                                                       //邻居页
            //找祖先页
            while(prev == Page.MIN){
                if(s.isEmpty()) { break;}  //已经到根页了，操作页就是最极端的左页
                pair = s.pop();
                parent = (PageNoLeaf) table.getPage(pair.page_offset);
                prev = parent.getPrevOffset(pair.node_offset);
            }
            //找邻居   prev != Page.MIN就说明找到祖先了
            if(prev != Page.MIN){
                ancestor = parent;
                ancestor_offset = prev;
                neighbor = table.getPage(parent.getLeftPage(prev));
                while(neighbor.getPage_level() != page.getPage_level()){
                    neighbor = table.getPage(
                            ((PageNoLeaf)neighbor).getLeftPage
                                    (neighbor.getPage_slots_offset().
                                            get(neighbor.getPage_slots_offset().size() - 1)));
                }
            }
            //邻居存在  且可以合并
            if(neighbor != null && neighbor.getPage_num() + page.getPage_num() < 5){
                pageMerge(page,stack.peek(),neighbor,1,ancestor,ancestor_offset);
                return true;
            }
            else return false;
        }

        //处理 找邻居页中的极端右情况    是pageMerge的辅助方法
        private boolean extremeRightNeighbor(Page page,Stack<Table.Pair> stack){
            Stack<Table.Pair> s = new Stack<>();  s.addAll(stack);         //克隆一个stack栈来使用
            Table.Pair pair = s.pop();
            PageNoLeaf parent = (PageNoLeaf) table.getPage(pair.page_offset);         //page的父母页
            int next = parent.getNextOffset(pair.node_offset);//父页中当前页节点后节点
            PageNoLeaf ancestor = null;                                                 //极端情况肯定有祖先页
            int ancestor_offset = 0;                                                  //祖先节点
            Page neighbor = null;                                                       //邻居页
            //找祖先页
            while(next == Page.MAX){
                if(s.isEmpty()) { break;}  //已经到根页了，操作页就是最极端的右页
                pair = s.pop();
                parent = (PageNoLeaf) table.getPage(pair.page_offset);
                next = parent.getNextOffset(pair.node_offset);
            }
            //找邻居   next != Page.MAX就说明找到祖先了
            if(next != Page.MAX){
                ancestor = parent;
                ancestor_offset = next;
                neighbor = table.getPage(parent.getLeftPage(next));
                while(neighbor.getPage_level() != page.getPage_level()){
                    neighbor = table.getPage(
                            ((PageNoLeaf)neighbor).getLeftPage
                                    (neighbor.getNextOffset(Page.MIN)));
                }
            }
            //邻居存在  且可以合并
            if(neighbor != null && neighbor.getPage_num() + page.getPage_num() < 5){
                pageMerge(page,stack.peek(),neighbor,2,ancestor,ancestor_offset);
                return true;
            }
            else return false;
        }

        //---辅助方法---以上--//

        //页合并（三步）  第一步:将两页合并  第二步：父页删除节点 第三步：祖先页更新索引键
        private void pageMerge(Page page, Table.Pair pair, Page neighbor, int model, Page ancestor, int ancestor_offset){
            PageNoLeaf parent = (PageNoLeaf) table.getPage(pair.page_offset);
            int offset = pair.node_offset;
            //特殊情况： 本页已无节点，父页直接删除本页节点即可
            if(model == 3){
                parent.offsetDelete(offset,1);
            }else {
                //第一步
                merge(page,neighbor,model);
                if (page.getPage_num() != 0) throw new RuntimeException("逻辑错误");
                if (parent.getLeftPage(offset) != page.getPage_offset()) throw new RuntimeException("逻辑错误");
                //第二步
                parent.offsetDelete(offset,1);
                if (parent.getPage_num() == 0 && table.getRoot() == parent) table.setRoot(neighbor);
                //第三步
                if (ancestor != null) {
                    PageNoLeaf ancestor_page = (PageNoLeaf) table.getPage(ancestor.getPage_offset());
                    findIndex(ancestor_page.getPage_offset(),ancestor_offset);
                }
            }
        }

        //---辅助方法---以下--//

        //两页合并
        private void merge(Page outer,Page iner,int model){
            if(iner.getPage_level() != outer.getPage_level())  throw new RuntimeException("逻辑错误");
            if(outer.getPage_num() == 0)  return;      //page页的节点数量为0，不做任何处理

            //叶子页的情况
            if(outer instanceof PageLeaf out && iner instanceof PageLeaf in){
                int out_prt = out.getNextOffset(Page.MIN);
                while(out_prt != Page.MAX)
                {
                    int heap_no = out.getHeapNo(out_prt);
                    Object[] values = out.getValues(out_prt);
                    Object index_key = out.getIndex_key(out_prt);
                    //插入
                    in.insert(values,heap_no,index_key);
                    //遍历
                    out_prt = out.getNextOffset(out_prt);
                }
            }
            //非叶子页的情况
            if(outer instanceof PageNoLeaf out && iner instanceof PageNoLeaf in){
                int out_prt = out.getNextOffset(Page.MIN);
                int in_prev;   int in_last;
                //准备工作 ，找到插入的前一个节点
                if(model == 1){
                    in_prev = in.getPrevOffset(Page.MAX);
                    //原本的最后一个节点
                    in_last = in_prev;
                    in.setType(in_last,(byte)0x01); in.setOwned(in_last,(byte)1);
                }else {in_prev = Page.MIN; in_last = Page.MIN;}
                //插入
                while(out_prt != Page.MAX)
                {
                    byte[] node_bytes = out.getBytes(out_prt);
                    in.insert(in_prev,node_bytes);
                    in.addOwned(in_last);
                    //遍历
                    out_prt = out.getNextOffset(out_prt);
                    in_prev = in.getNextOffset(in_prev);
                }
                //后续要将空白的索引节点补上
                if(model == 2){in_last = in_prev;}
                findIndex(in.getPage_offset(),in_last);
                if(model == 1){
                    in.setOwned(in_last, (byte) (in.getOwned(in_last) - 1));
                    //in_prev现在是最后一个节点   即工具节点
                    in.setType(in_prev,(byte) 0x11);   in.setOwned(in_prev,(byte)2);
                    List<Integer> slots = in.getPage_slots_offset();
                    slots.add(in_prev);
                }
                //槽没有动态平衡!  /*TODO*/
            }

            outer.setPage_num(0);
        }

        //查找某一非叶子节点的可用索引键值
        private void findIndex(int page_offset,int prt){
            PageNoLeaf page = (PageNoLeaf) table.getPage(page_offset);
            int next = page.getNextOffset(prt);
            //如果下一个节点就是伪最大节点  那么说明本节点就是工具节点
            if(next == Page.MAX) {
                page.setType(prt,(byte)0x11);
                page.setIndex_key_bytes(prt,new byte[]{0,0,0,0});
                return;
            }
            Page rightPage = table.getPage(page.getLeftPage(next));

            Stack<byte[]> stack = new Stack<>();   //栈中储存着可能利用到的索引
            //只要右页不是叶子页 就一直递推到叶子页
            while(true){
                //储存本层可能利用的索引
                int first = rightPage.getNextOffset(Page.MIN);
                if(first == Page.MAX) break;
                if(rightPage.getType(first) != (byte)0x11) {
                    stack.push(rightPage.getIndex_key_bytes(first));
                }
                //如果到叶子层 不用下层
                if(rightPage.getPage_level() == (byte)0x00) break;
                //到下一层
                PageNoLeaf pagee = (PageNoLeaf)rightPage;
                rightPage = table.getPage(pagee.getLeftPage(first));
            }
            //为空说明该节点的右页没有一层可以利用索引
            if(stack.isEmpty()){
                if(page.getType(next) == (byte)0x11)  {
                    page.setType(prt,(byte) 0x11);  //为了不破坏查找功能 只要键值为空的都是0x11
                    page.setIndex_key_bytes(prt,new byte[]{0,0,0,0});
                }
                else page.setIndex_key_bytes(prt,page.getIndex_key_bytes(next));

            }else page.setIndex_key_bytes(prt,stack.pop());

        /*
            1.offset 删除掉next  会破坏掉页地址检查机制
            2.启动重检页地址判断
        */
        }

        //---辅助方法---以上--//


        @Override
        public Class<PageManagerEvent.MergeEvent> getEventType() {return PageManagerEvent.MergeEvent.class;}
    }
}
