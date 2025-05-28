package Memory.Cache;

import Memory.Page;
import Memory.Table;

import java.util.HashMap;
import java.util.Map;

/*TODO*/  //实现最大空间为内存大小
//最大空间为Page的个数
public class LRUCacheStrategy extends CacheStrategy{


    private final int capacity;                                             //最大容量
    private class PageCountPair{   //页和页使用次数
        Page page;int count;
        public PageCountPair(Page page,int count){
            this.page = page;  this.count = count;
        }
    }
    private final Map<Integer, PageCountPair> cache = new HashMap<>();   //缓存池

    public LRUCacheStrategy(int capacity, Table table){
        this.table = table;
        this.capacity = capacity;
    }

    @Override
    public Page getPage(int page_offset) {
        PageCountPair pair = cache.get(page_offset);
        if(pair == null){
            Page page = deSerializePageInto(page_offset);
            pair = new PageCountPair(page,0);
            pushPage(page);
        }else{
            pair.count++;    //调用值加一
        }
        return pair.page;
    }

    @Override
    public void pushPage(Page page) {
        //缓冲池满了
        if(isCacheFull()){
            PageCountPair minPair = catchMinCount();   //找出最小调用值
            popPage(minPair.page.getPage_offset());
        }
        cache.put(page.getPage_offset(),new PageCountPair(page,0));
    }

    @Override
    public void popPage(int page_offset) {
        Page page = cache.remove(page_offset).page;  //缓存删除
        page.serializePageOut();  //写入磁盘
    }

    @Override
    public boolean isCacheFull() {
        /*TODO*/
        return cache.size() >= capacity;
    }

    @Override
    public void close() {
        for (PageCountPair pair : cache.values()) {
            pair.page.serializePageOut();
        }
        cache.clear();
    }

    //找出当前缓冲池中调用值最小的页
    private PageCountPair catchMinCount(){
        PageCountPair minPair = null;
        int min = 99999;
        for (PageCountPair pair : cache.values())
            if(pair.count < min) {min = pair.count; minPair = pair; }

        return minPair;
    }
}