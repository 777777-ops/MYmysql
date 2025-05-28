package Memory.Event;

import Memory.Table;

import java.util.Collection;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;

public abstract class CurdEvent extends Event{

    //插入事件
    public static class insertEvent extends CurdEvent{
        Object[] data;
        public insertEvent(Object[] data){
            this.data = data;
            this.priority = 1;   /*TODO*/
        }
        public Object[] getData() {return data;}
    }

    //删除事件
    public static class deleteEvent extends CurdEvent{
        String conditions;
        public deleteEvent(String conditions){this.conditions = conditions;this.priority = 2;}
        public String getConditions(){return this.conditions;}
    }

    //查询事件
    public static class selectEvent extends CurdEvent{
        private final CompletableFuture<Collection<Table.SearchResult>> future = new CompletableFuture<>();
        String conditions;

        //完成
        public void complete(Collection<Table.SearchResult> data){ future.complete(data); }

        //获取结果
        public Collection<Table.SearchResult> getResult(){
            try {
                return future.get();
            } catch (Exception e) {
                throw new RuntimeException("Event processing failed", e);
            }
        }

        public selectEvent(String conditions){this.conditions = conditions;}
        public String getConditions(){return this.conditions;}
    }

    //查询某节点的路径
    public static class selectWayEvent extends CurdEvent {
        //目前用不上
        private final CompletableFuture<Stack<Table.Pair>> future = new CompletableFuture<>();
        Object index_key;
        int page_offset;
        /*
            model :
                1 -> 索引查询
                2 -> 查询第一页
                3 -> 索引 + 页偏移量
        */
        int model;


        public selectWayEvent(Object index_key) {
            this.index_key = index_key;
            this.page_offset = 0;
            if(index_key != null) this.model = 1;
            else this.model = 2;
            this.priority = 1; /*TODO*/
        }

        public selectWayEvent(Object index_key,int page_offset){
            this.index_key = index_key;
            this.page_offset = page_offset;
            this.model = 3;
            this.priority = 1;
        }

        public void complete(Stack<Table.Pair> result) {
            future.complete(result);
        }

        public Stack<Table.Pair> getResult(){
            try {
                return future.get();
            } catch (Exception e) {
            throw new RuntimeException("Event processing failed", e);
            }
        }
        public Object getIndex_key() {return index_key;}
        public int getModel(){return model;}
        public int getPage_offset(){return this.page_offset;}
    }
}
