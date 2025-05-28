package Memory.Event;

import Memory.Table;
import java.util.Stack;

public abstract class PageManagerEvent extends Event{


    public static class SpiltEvent extends PageManagerEvent{
        Stack<Table.Pair> way;
        public SpiltEvent(Stack<Table.Pair> way){this.way = way; this.priority = 3;}
        public Stack<Table.Pair> getWay() {return way;}
    }

    public static class MergeEvent extends PageManagerEvent{
        public MergeEvent(){this.priority = 3;}
    }
}
