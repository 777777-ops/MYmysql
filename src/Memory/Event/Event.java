package Memory.Event;

public class Event implements Comparable<Event>{
    int priority;
    @Override
    public int compareTo(Event o) {
        return priority - o.priority;
    }
}
