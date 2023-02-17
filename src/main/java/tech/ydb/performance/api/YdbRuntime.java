package tech.ydb.performance.api;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbRuntime extends AutoCloseable {
    public interface YdbSession extends AutoCloseable  {
        public CompletableFuture<AppRecord> read(String uuid);

        @Override
        public void close();
    }

    public void createTable();

    public CompletableFuture<YdbSession> createSession();

    public CompletableFuture<Boolean> bulkUpsert(List<AppRecord> records);

    @Override
    public void close();
}
