package tech.ydb.performance.impl;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import tech.ydb.auth.iam.CloudAuthHelper;
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

        this.tableClient = TableClient.newClient(transport).build();
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
        return tableClient.createSession(Duration.ofSeconds(1))
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
                    .thenApply(r -> r.map(this::readRecord).getValue());
        }

        private AppRecord readRecord(DataQueryResult result) {
            if (result.isEmpty()) {
                return null;
            }

            ResultSetReader rs = result.getResultSet(0);
            if (!rs.next()) {
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
