package tech.ydb.performance.runtime;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.yandex.ydb.auth.iam.CloudAuthHelper;
import com.yandex.ydb.core.Result;
import com.yandex.ydb.core.Status;
import com.yandex.ydb.core.grpc.GrpcTransport;
import com.yandex.ydb.table.Session;
import com.yandex.ydb.table.SessionRetryContext;
import com.yandex.ydb.table.TableClient;
import com.yandex.ydb.table.description.TableDescription;
import com.yandex.ydb.table.query.DataQueryResult;
import com.yandex.ydb.table.query.Params;
import com.yandex.ydb.table.result.ResultSetReader;
import com.yandex.ydb.table.rpc.grpc.GrpcTableRpc;
import com.yandex.ydb.table.settings.BulkUpsertSettings;
import com.yandex.ydb.table.transaction.TxControl;
import com.yandex.ydb.table.values.ListType;
import com.yandex.ydb.table.values.PrimitiveType;
import com.yandex.ydb.table.values.PrimitiveValue;
import com.yandex.ydb.table.values.StructType;
import com.yandex.ydb.table.values.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.performance.AppConfig;
import tech.ydb.performance.api.AppRecord;
import tech.ydb.performance.api.YdbRuntime;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbRuntimeV1 implements YdbRuntime {
    private final static Logger logger = LoggerFactory.getLogger(YdbRuntimeV1.class);

    private final String tableName;
    private final String tablePath;
    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;

    public YdbRuntimeV1(AppConfig config) {
        this.tableName = config.tableName();
        this.transport = GrpcTransport.forConnectionString(config.endpoint())
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .build();

        this.tableClient = TableClient.newClient(GrpcTableRpc.useTransport(transport))
                .sessionPoolSize(0, Math.max(2, config.threadsCount()))
                .build();
        this.retryCtx = SessionRetryContext.create(tableClient).build();
        this.tablePath = transport.getDatabase() + "/" + tableName;
    }

    @Override
    public void createTable() {
        TableDescription description = TableDescription.newBuilder()
                .addNullableColumn("uuid", PrimitiveType.utf8())
                .addNullableColumn("payload", PrimitiveType.string())
                .setPrimaryKey("uuid")
                .build();
        retryCtx.supplyStatus(session -> session.createTable(tablePath, description))
                .join().expect("Can't create table");
    }

    @Override
    public CompletableFuture<YdbSession> createSession() {
        return tableClient.getOrCreateSession(Duration.ofSeconds(5))
                .thenApply(r -> r.map(SessionImpl::new)
                .expect("can't create session"));
    }

    @Override
    public void close() {
        tableClient.close();
        transport.close();
    }

    @Override
    public CompletableFuture<Boolean> bulkUpsert(List<AppRecord> records) {
        StructType type = StructType.of(
                "uuid", PrimitiveType.utf8(),
                "payload", PrimitiveType.string()
        );

        List<Value> values = records.stream().map(r -> type.newValue(
                "uuid", PrimitiveValue.utf8(r.uuid()),
                "payload", PrimitiveValue.string(r.payload())
        )).collect(Collectors.toList());

        BulkUpsertSettings settings = new BulkUpsertSettings();

        return retryCtx
                .supplyStatus(s -> s.executeBulkUpsert(tablePath, ListType.of(type).newValue(values), settings))
                .thenApply(Status::isSuccess);
    }

    private class SessionImpl implements YdbSession {
        private final Session session;

        public SessionImpl(Session session) {
            this.session = session;
        }

        @Override
        public CompletableFuture<AppRecord> read(String uuid) {
            String query = "DECLARE $uuid as Text; SELECT uuid, payload FROM " + tableName + " WHERE uuid = $uuid;";
            Params params = Params.of("$uuid", PrimitiveValue.utf8(uuid));

            return session.executeDataQuery(query, TxControl.serializableRw(), params)
                    .thenApply(this::readRecord);
        }

        private AppRecord readRecord(Result<DataQueryResult> result) {
            if (result == null) {
                logger.warn("got null data query result");
                return null;
            }

            if (!result.isSuccess()) {
                logger.warn("got {} status ", result.getCode());
                return null;
            }

            if (result.expect("").getResultSetCount() == 0) {
                logger.warn("got empty result set");
                return null;
            }

            ResultSetReader rs = result.expect("").getResultSet(0);
            if (!rs.next()) {
                logger.warn("got result set without rows");
                return null;
            }

            String uuid = rs.getColumn("uuid").getUtf8();
            byte[] payload = rs.getColumn("payload").getString();
            return new AppRecord(uuid, payload);
        }

        @Override
        public void close() {
            session.release();
        }
    }

}
