package tech.ydb.performance.api;

import java.util.concurrent.CompletableFuture;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbSession {
    interface Pool {

        public CompletableFuture<YdbSession> createSession();
        public void shutdown();
    }

    public CompletableFuture<AppRecord> readRecord(String uuid);
    public void close();
}
