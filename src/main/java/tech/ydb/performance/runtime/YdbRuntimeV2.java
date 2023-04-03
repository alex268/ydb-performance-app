package tech.ydb.performance.runtime;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.Result;
import tech.ydb.core.Status;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.performance.AppConfig;
import tech.ydb.performance.api.AppRecord;
import tech.ydb.performance.api.YdbRuntime;
import tech.ydb.table.Session;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.query.Params;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.transaction.TxControl;
import tech.ydb.table.values.ListType;
import tech.ydb.table.values.PrimitiveType;
import tech.ydb.table.values.PrimitiveValue;
import tech.ydb.table.values.StructType;
import tech.ydb.table.values.Value;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbRuntimeV2 implements YdbRuntime {
    private final static Logger logger = LoggerFactory.getLogger(YdbRuntimeV2.class);

    private final String tableName;
    private final String tablePath;
    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;

    public YdbRuntimeV2(AppConfig config) {
        this.tableName = config.tableName();
        this.transport = GrpcTransport.forConnectionString(config.endpoint())
                .withAuthProvider(CloudAuthHelper.getAuthProviderFromEnviron())
                .build();

        this.tableClient = TableClient.newClient(transport)
                .sessionPoolSize(0, Math.max(2, config.threadsCount()))
                .build();
        this.retryCtx = SessionRetryContext.create(tableClient).build();
        this.tablePath = transport.getDatabase() + "/" + tableName;
    }

    @Override
    public void createTable() {
        TableDescription description = TableDescription.newBuilder()
                .addNullableColumn("uuid", PrimitiveType.Text)
                .addNullableColumn("payload", PrimitiveType.Bytes)
                .setPrimaryKey("uuid")
                .build();
        retryCtx.supplyStatus(session -> session.createTable(tablePath, description))
                .join().expectSuccess("Can't create table");
    }

    @Override
    public CompletableFuture<YdbSession> createSession() {
        return tableClient.createSession(Duration.ofSeconds(5))
                .thenApply(r -> r.map(SessionImpl::new)
                .getValue());
    }

    @Override
    public CompletableFuture<Boolean> bulkUpsert(List<AppRecord> records) {
        StructType type = StructType.of(
                "uuid", PrimitiveType.Text,
                "payload", PrimitiveType.Bytes
        );

        List<Value<?>> values = records.stream().map(r -> type.newValue(
                "uuid", PrimitiveValue.newText(r.uuid()),
                "payload", PrimitiveValue.newBytes(r.payload())
        )).collect(Collectors.toList());

        return retryCtx.supplyStatus(s -> s.executeBulkUpsert(tablePath, ListType.of(type).newValue(values)))
                .thenApply(Status::isSuccess);
    }

    @Override
    public void close() {
        tableClient.close();
        transport.close();
    }

    private class SessionImpl implements YdbSession {
        private final Session session;

        public SessionImpl(Session session) {
            this.session = session;
        }

        @Override
        public CompletableFuture<AppRecord> read(String uuid) {
            String query = "DECLARE $uuid as Text; SELECT uuid, payload FROM " + tableName + " WHERE uuid = $uuid;";
            Params params = Params.of("$uuid", PrimitiveValue.newText(uuid));

            return session.executeDataQuery(query, TxControl.serializableRw(), params)
                    .thenApply(this::readRecord);
        }

        private AppRecord readRecord(Result<DataQueryResult> result) {
            if (result == null) {
                logger.warn("got null data query result");
                return null;
            }

            if (!result.isSuccess()) {
                logger.warn("got {} status ", result.getStatus().getCode());
                return null;
            }

            if (result.getValue().getResultSetCount() == 0) {
                logger.warn("got empty result set");
                return null;
            }

            ResultSetReader rs = result.getValue().getResultSet(0);
            if (!rs.next()) {
                logger.warn("got result set without rows");
                return null;
            }

            String uuid = rs.getColumn("uuid").getText();
            byte[] payload = rs.getColumn("payload").getBytes();
            return new AppRecord(uuid, payload);
        }

        @Override
        public void close() {
            session.close();
        }
    }
}
