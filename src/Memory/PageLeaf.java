package Memory;

import java.util.ArrayList;

//叶子页
public class PageLeaf extends Page{
    public Page page_prev;                //上一页                                              (4字节)
    public Page page_next;                //下一页                                              (4字节)

    //初始化叶子页
    public PageLeaf(){
        super((byte)0x00);
        page_next = null;
        page_prev = null;
        page_slots = new ArrayList<>();
        page_slots.add(new Record((byte)0x02,null));  //伪最小槽
        page_slots.add(new Record((byte)0x03,null));  //伪最大槽。伪最大槽只有一条记录（索引）
        page_slots.get(0).next_record = page_slots.get(1);     //将伪最小值的下一个指针指向伪最大值
    }
}
