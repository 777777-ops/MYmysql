package Bplus;

import java.util.*;

public class BpTree<Index extends Comparable<Index>>{
    private BpNode<Index> root; //根节点
    private int order;           //阶                   B+数的阶数要>=3否则就成了二叉树了

    //创建一个包含BpNode节点地址 和 进入的是该节点中的第几阶 的内部类
    private class EnterNode{
        BpNode<Index> node;
        int EnterIndex;
        EnterNode(BpNode<Index> ThisNode,int InsertIndex){
            node = ThisNode;
            EnterIndex = InsertIndex;
        }
    }


    public BpTree(int order)
    {
        this.order = order;
        root = new BpNode<Index>(true,order);  //根节点默认是叶子节点
    }
    //插入
    public void insert(Index index)
    {
        BpNode<Index> NowNode = root; //现在正在处理的Node
        Stack<EnterNode> stack = new Stack<>();
        while(!NowNode.getIsLeaf()){   //递推到NowNode是叶子节点为止
            int EnterIndex = NowNode.getEnterOrder(index);        //所进入的阶
            stack.push(new EnterNode (NowNode,EnterIndex) );  //将现在处理的节点和所进入的阶 打包 送入栈
            NowNode = NowNode.getChildren().get(EnterIndex);  //将NowNode替换成所进入阶中的节点
        }
        //现在NowNode是叶子结点了

        NowNode.insert(index);                 //不管是否满了都要插入
        boolean IsSpill = NowNode.isSpill();        //如果没溢出，该函数到此结束
        while(IsSpill)
        {
            BpNode<Index> parent = null;
            if(NowNode.getIsLeaf())   //是叶子
                parent = SpiltLeafNode(NowNode);   //分裂后的父母节点
            else                   //不是叶子
                parent = SpiltNode(NowNode);

            if(stack.isEmpty())  //如果栈空，就说明该父母节点为根节点
                root = parent;
            else{
                EnterNode enterNode = stack.pop();   //弹栈
                NowNode = enterNode.node;
                MergeNode(NowNode,parent,enterNode.EnterIndex);  //拼接

            }

            IsSpill = NowNode.isSpill();  //现在的节点溢出了吗
        }
    }
    //分裂叶子节点     并返回父母节点
    private BpNode<Index> SpiltLeafNode(BpNode<Index> Node) {   //分裂叶子节点
        List<Index> Indexs = Node.getIndexs();
        int Mid = Indexs.size() / 2;  //中分
        BpNode<Index> parent = new BpNode<>(false,this.order);  //非叶子节点
        BpNode<Index> Right = new BpNode<>(true,this.order,new ArrayList<>(Indexs.subList(Mid,Indexs.size())),null);    //右叶子节点

        parent.insert(Indexs.get(Mid));      //将中分位置插入父母节点
        parent.getChildren().add(Node);     //左节点
        parent.getChildren().add(Right);    //右节点

        Indexs.subList(Mid,Indexs.size()).clear();//将中分后的节点删除

        return parent;

    }

    //分裂非叶子节点
    private BpNode<Index> SpiltNode(BpNode<Index> Node){
        List<Index> Indexs = Node.getIndexs();
        List<BpNode<Index>> children = Node.getChildren();
        int Mid = Indexs.size() / 2;  //中分
        BpNode<Index> parent = new BpNode<>(false,this.order);  //非叶子节点
        BpNode<Index> Right = new BpNode<>(false,this.order,
                new ArrayList<>(Indexs.subList(Mid+1,Indexs.size())),new ArrayList<>(children.subList(Mid+1,children.size())));    //右叶子节点

        parent.insert(Indexs.get(Mid));      //将中分位置插入父母节点
        parent.getChildren().add(Node);     //左节点
        parent.getChildren().add(Right);    //右节点

        Indexs.subList(Mid,Indexs.size()).clear();   //删除分裂的索引节点
        children.subList(Mid+1,children.size()).clear();  //删除分裂的子节点

        return parent;
    }
    //将单索引和多索引的节点拼接
    private  void MergeNode(BpNode<Index> More,BpNode<Index> One,int EnterIndex)  //外加所要插入阶
    {
        if(One.getIndexs().size()!=1)
            throw new RuntimeException("父母节点的索引大小不为1");

        Index index = One.getIndexs().get(0);
        List<BpNode<Index>> MoreChildren = More.getChildren();    //More节点的子节点列表
        More.getIndexs().add(EnterIndex, index);      //索引拼接

        MoreChildren.remove(EnterIndex);
        MoreChildren.add(EnterIndex,One.getChildren().get(1));   //阶拼接
        MoreChildren.add(EnterIndex,One.getChildren().get(0));

    }


    ////////////////////////////////////


    //测试类
    public static void main(String[] args) {

        BpTree<String> bpTree = new BpTree<>(4);

        bpTree.insert("11");
        bpTree.insert("12");
        bpTree.insert("13");
        bpTree.insert("14");
        bpTree.insert("15");
        bpTree.insert("16");
        bpTree.insert("17");
        bpTree.insert("18");
        bpTree.insert("19");
        bpTree.insert("21");
        bpTree.insert("22");
        bpTree.insert("23");

    }

}
class BpNode<Index extends Comparable<Index>>{  //索引(Index)    //接口Comparable实现比较
    /*
        节点中的操作仅仅局限于节点
    */


    private static final int DEFAULT_ORDER = 4;  //默认阶数
    private int order; // 树的阶数
    private List<Index> indexs; // 节点中的索引

    private List<BpNode<Index>> children; // 子节点
    private boolean isLeaf; // 是否为叶子节点
    public BpNode<Index> nexNode; // 指向下一个叶子节点
    public BpNode<Index> preNode; //指向上一个叶子节点

    public BpNode(boolean isLeaf,int order)
    {
        this.order = order;
        indexs = new ArrayList<>(); //初始化索引表
        this.isLeaf = isLeaf;
        nexNode = null;  preNode = null; children = null;   //初始化都为null
        if(!isLeaf)
            children = new ArrayList<>();
    }
    //带有索引列表的构造函数
    public BpNode(boolean isLeaf,int order,List<Index> indexs,List<BpNode<Index>> children)
    {
        this.order = order;
        this.indexs = indexs; //初始化索引表
        this.isLeaf = isLeaf;
        nexNode = null;  preNode = null; this.children = null;   //初始化都为null
        if(!isLeaf)        //不是叶子节点
            this.children = children;
    }

    //节点内插入
    public void insert(Index index)
    {
        //如果在达到上限仍要插入                  //size() <= order - 1
        if(this.indexs.size() == order )        //例如order为4  最大上限也只能为4，理论为3
            try {
                throw new Exception("B树节点已经满") ;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        //索引列表为空
        if(this.indexs.isEmpty()) {
            indexs.add(index);
            return;
        }

        int newIndex=0;  //新索引插入的位置
        while(newIndex < indexs.size())
        {
            if(index.compareTo(indexs.get(newIndex))<0)
                break;
            newIndex++;
        }    //确定好了新索引该插入的位置
        indexs.add(newIndex,index);
    }
    //查看索引是否包含在该节点中
    public boolean contain(Index index)
    {for(Index i : indexs)  if(i.equals(index)) return true;
        return false;}

    //比较索引列表，一旦有比输入索引大的就返回    比如索引0比输入索引大，返回0
    public int getEnterOrder(Index index){
        int i = 0;
        while(i<indexs.size() && index.compareTo(indexs.get(i))>0)
            i++;
        return i;
    }
    
    //返回当前节点是否满了
    public boolean isFull(){return this.order == this.indexs.size()+1;}
    //返回当前节点是否溢出了
    public boolean isSpill(){return this.order == this.indexs.size();}
    //返回阶
    public int getOrder(){return this.order;}
    //返回是否为叶子节点
    public boolean getIsLeaf(){return this.isLeaf;}
    //返回子节点们
    public List<BpNode<Index>> getChildren(){return this.children;}
    //返回索引列表
    public List<Index> getIndexs(){return this.indexs;}



}