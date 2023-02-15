package tech.ydb.performance.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbRuntime extends AutoCloseable {
    public interface YdbSession {
        public CompletableFuture<AppRecord> read(String uuid);
        public CompletableFuture<Boolean> bulkUpsert(List<AppRecord> records);

        public void close();
    }

    public void createTable();

    public CompletableFuture<YdbSession> createSession();

    @Override
    public void close();
}
