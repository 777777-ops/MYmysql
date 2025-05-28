package CustomThread;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class CustomThreadPool extends ThreadPoolExecutor {

    //是优先级多线程   目前默认单线程
    /*TODO*/
    public CustomThreadPool() {
        super(2, 2, 60, TimeUnit.SECONDS, new PriorityBlockingQueue<>(100));
    }
}
