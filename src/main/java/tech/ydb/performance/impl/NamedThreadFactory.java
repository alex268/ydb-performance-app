package tech.ydb.performance.impl;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class NamedThreadFactory implements ThreadFactory {
    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger(1);

    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    NamedThreadFactory(String prefix) {
        namePrefix = prefix + INSTANCE_COUNT.getAndIncrement() + "-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(r, namePrefix + threadNumber.getAndIncrement());
        t.setDaemon(true);
        return t;
    }

}
