package Memory;

import java.util.ArrayList;
import java.util.Set;

//非叶子页节点
public class PageNoLeaf extends Page{

    //初始化非叶子页，创建一个新页时一定会有一个新的索引  和一个伴随的右页
    public PageNoLeaf(byte level, IndexKey indexKey,Page rightPage){
        super(level);
        page_slots = new ArrayList<>();
        page_slots.add(new IndexKey((byte)0x02,null,null));  //伪最小槽
        page_slots.add(new IndexKey((byte)0x03,null,null));  //伪最大槽。伪最大槽只有一条记录（索引）
        page_slots.get(0).next_record = page_slots.get(1);                    //将伪最小值的下一个指针指向伪最大值
        page_slots.get(0).n_owned++;  page_slots.get(1).n_owned++;
        insertIndex(indexKey,rightPage);
    }

    //插入一个新的索引节点(索引节点自带左页，而不自带右页)   该方法还要实现写入流操作
    public void insertIndex(IndexRecord indexKey,Page rightPage) {
        insert(indexKey);                                                  //插入成功
        IndexRecordToIndexKey(indexKey.next_record).leftPage = rightPage;  //取出索引节点的下一个节点

    }

    //找到rec这条记录所要进入的那个页的右索引
    public IndexKey findEnterPage(IndexRecord rec) {
        IndexRecord indexKey = this.page_slots.get(slotsSearch(rec));  //槽头
        indexKey = Search(rec,indexKey);            //返回刚好比rec记录小的索引
        indexKey = indexKey.next_record;            //定位到下一个索引

        return IndexRecordToIndexKey(indexKey);
    }

    //强转检查
    private IndexKey IndexRecordToIndexKey(IndexRecord rec) {
        if(rec instanceof IndexKey) return (IndexKey)rec;
        else{
            throw new RuntimeException("非叶子页中节点不为索引键值");
        }
    }


    /******************************************************************************/

    //测试点   //测试插入索引记录，以及槽分组。
    public static void main(String[] args) {
        PageNoLeaf p1 = new PageNoLeaf((byte)0x01,new IndexKey((byte)0x01,5,null),null);
        //new IndexKey((byte)0x01,2,null),null
        p1.insertIndex(new IndexKey((byte)0x01,2,null),null);
        p1.insertIndex(new IndexKey((byte)0x01,11,null),null);
        p1.insertIndex(new IndexKey((byte)0x01,16,null),null);
        p1.insertIndex(new IndexKey((byte)0x01,7,null),null);
        p1.insertIndex(new IndexKey((byte)0x01,9,null),null);

        System.out.println(p1.page_slots.get(0).next_record.index_key);
    }
}
