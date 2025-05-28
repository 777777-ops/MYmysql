package Memory.Event;

public interface EventHandler<T extends Event> {
    void handle(T event);

    // 支持处理的事件类型
    Class<T> getEventType();
}
