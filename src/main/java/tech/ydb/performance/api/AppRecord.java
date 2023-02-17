package tech.ydb.performance.api;

import java.util.Random;
import java.util.UUID;

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

    public String uuid() {
        return this.uuid;
    }

    public byte[] payload() {
        return this.payload;
    }

    public static AppRecord createByIndex(long index, int recordSize) {
        Random rnd = new Random(index * 31 + 21);

        byte[] payload = new byte[recordSize];
        rnd.nextBytes(payload);
        UUID uuid = UUID.nameUUIDFromBytes(payload);

        return new AppRecord(uuid.toString(), payload);
    }
}
