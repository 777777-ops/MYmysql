package CustomThread;

import Memory.Event.Event;

public class PrioritizedTask implements Runnable, Comparable<PrioritizedTask> {
    private final Runnable task;
    private final Event event;

    public PrioritizedTask(Runnable task, Event event) {
        this.task = task;
        this.event = event;
    }

    @Override
    public void run() {
        task.run();
    }

    @Override
    public int compareTo(PrioritizedTask other) {
        return event.compareTo(other.event);
    }
}