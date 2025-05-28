package Memory.Cache;

import DataIO.BytesIO;
import Memory.Page;
import Memory.Table;

public abstract class CacheStrategy {
    Table table;
    abstract public Page getPage(int page_offset);
    abstract public void pushPage(Page page);
    abstract public void popPage(int page_offset);
    abstract boolean isCacheFull();

    abstract public void close();


    //从磁盘中读取出Page
    protected Page deSerializePageInto(int page_offset){
        byte[] data = BytesIO.readDataInto(Page.PAGE_HEAD,page_offset,table.table_name);
        return Page.deSerialize(data,table);
    }
}

