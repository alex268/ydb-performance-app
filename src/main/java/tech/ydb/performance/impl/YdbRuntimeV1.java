package tech.ydb.performance.impl;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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

import tech.ydb.performance.AppConfig;
import tech.ydb.performance.api.AppRecord;
import tech.ydb.performance.api.YdbRuntime;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbRuntimeV1 implements YdbRuntime {
    private final String tableName;
    private final String tablePath;
    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;

    public YdbRuntimeV1(AppConfig config) {
        this.tableName = config.tableName();
        this.transport = GrpcTransport.forConnectionString(config.endpoint())
                .build();

        this.tableClient = TableClient.newClient(GrpcTableRpc.useTransport(transport)).build();
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
        return tableClient.getOrCreateSession(Duration.ofSeconds(1))
                .thenApply(r -> r.map(SessionImpl::new)
                .expect("can't create session"));
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
            Params params = Params.of("$uuid", PrimitiveValue.utf8(uuid));

            return session.executeDataQuery(query, TxControl.serializableRw(), params)
                    .thenApply(r -> r.map(this::readRecord).expect("can't execute query"));
        }

        private AppRecord readRecord(DataQueryResult result) {
            if (result.isEmpty()) {
                return null;
            }

            ResultSetReader rs = result.getResultSet(0);
            if (!rs.next()) {
                return null;
            }

            String uuid = rs.getColumn("uuid").getUtf8();
            byte[] payload = rs.getColumn("payload").getString();
            return new AppRecord(uuid, payload);
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

        @Override
        public void close() {
            session.close();
        }
    }

}
