package tech.ydb.performance.api;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class AppRecord {
    private final String uuid;
    private final byte[] payload;

    public AppRecord(String uuid, byte[] payload) {
        this.uuid = uuid;
        this.payload = payload;
    }
}
