package Memory.Event;

import CustomThread.CustomThreadPool;
import CustomThread.PrioritizedTask;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

public class EventBus {
    private final Map<Class<? extends Event>, EventHandler<?>> handlers = new ConcurrentHashMap<>();
    private final Executor executor;

    public EventBus() {
        this(new CustomThreadPool());
    }

    public EventBus(Executor executor) {
        this.executor = executor;
    }

    // 同一中操作都集中在同一处理器上！！！   /*TODO*/
    // 尚未引入多线程，故一个事件都只对应一个处理器  /*TODO*/
    // 注册事件处理器
    public <T extends Event> void registerHandler(EventHandler<T> handler) {
        Class<T> eventType = handler.getEventType();
        handlers.put(eventType,handler);
    }

    // 发布事件  (多线程)
    public <T extends Event> void publish(T event) {
        // 获取事件的实际类
        Class<?> eventClass = event.getClass();

        // 查找匹配的处理器
        EventHandler<?> eventHandlers = handlers.get(eventClass);
        if (eventHandlers != null) {
            // 执行处理器
            executor.execute(
                    new PrioritizedTask(() -> {
                @SuppressWarnings("unchecked")
                EventHandler<T> castedHandler = (EventHandler<T>) eventHandlers;
                castedHandler.handle(event);
            },event));

        }

    }

    // 执行事件  (单线程)
    public <T extends Event> void execute(T event){
        // 获取事件的实际类
        Class<?> eventClass = event.getClass();

        // 查找匹配的处理器
        EventHandler<?> eventHandlers = handlers.get(eventClass);
        @SuppressWarnings("unchecked")
        EventHandler<T> castedHandler = (EventHandler<T>) eventHandlers;
        castedHandler.handle(event);
    }
}
