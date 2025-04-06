package Memory;

import java.util.ArrayList;
import java.util.List;
/*
    打算叶子页为16kb         非叶子页为2kb
*/

//索引页
public abstract class Page{    //一张表文件最多储存1MB  1**20位，页的偏移量要为4字节

    protected byte page_level;                     //该页的层级,判断是否为叶子页 0x00是叶子页，0xff是根页   (1字节)
    protected List<IndexRecord> page_slots;        //该页的页目录槽                                      (40字节)预留20个槽
    protected static int default_slot_split = 5;   //槽分裂的默认阈值
    public int off_set;                            //该页的偏移量

    //初始化
    public Page(byte isLeaf){this.page_level = isLeaf;}

    //二分搜索槽  返回刚好比值rec小一点的那个槽位
    protected int slotsSearch(IndexRecord rec){
        int min = 0,max = page_slots.size()-1;
        int mid = (min + max)/2;
        while(min + 1 != max) {
            if(rec.compareTo(page_slots.get(mid))>0)
                min = mid;
            if(rec.compareTo(page_slots.get(mid))<0)
                max = mid;
            mid = (min + max)/2;
        }
        return mid;
    }

    //对二分搜索进一步补充，线性定位到索引所要插入位置的前一个索引
    protected IndexRecord Search(IndexRecord rec,IndexRecord slot_head) {
        IndexRecord prt = slot_head;
        IndexRecord prev = prt;
        while(rec.compareTo(prt) > 0)              //循环直到prt索引比rec大
        {
            prev = prt;                                 //prev指针一直在prt指针的前一位
            prt = prt.next_record;
        }
        return prev;
    }

    //将索引记录插入槽中       //返回所插入节点的上一个节点
    protected void insert (IndexRecord rec){
        int slot_head_num = slotsSearch(rec);       //二分找出槽头
        IndexRecord slot_head = this.page_slots.get(slot_head_num);
        slot_head.n_owned++;                     //槽头自增一
        IndexRecord prt = Search(rec,slot_head);    //所要插入的节点
        rec.next_record = prt.next_record;
        prt.next_record = rec;                      //插入
        check_slot_split(slot_head_num);                //检查槽是否需要分裂
        /*TODO*/                                    //流更新
    }

    //检查槽内数量是否达到分裂阈值
    private void check_slot_split(int slot_head_num){
        IndexRecord slot_head = this.page_slots.get(slot_head_num);
        if(slot_head.n_owned < default_slot_split)
            return;
        int mid = slot_head.n_owned/2;
        int mid_n = mid;
        IndexRecord prt = slot_head;
        while(mid_n-- > 0) {prt = prt.next_record;}
        //prt指向要分裂的节点
        page_slots.add(slot_head_num+1,prt);          //插入一个新的槽头
        prt.n_owned = (byte)(slot_head.n_owned - mid);      //新槽头的数量更新
        slot_head.n_owned = (byte)mid;                      //旧槽头的数量更新

    }
}

