package org.db.io;

import org.db.utility.LSMEntry;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public final class EntrySerializer {

    private EntrySerializer() {
    }

    /**
     * Binary layout (little endian):
     * [8 bytes] timestamp
     * [1 byte]  command (0=PUT, 1=DELETE)
     * [4 bytes] key length
     * [n bytes] key (UTF-8)
     * [4 bytes] value length (PUT only)
     * [n bytes] value        (PUT only)
     */
    public static byte[] marshall(LSMEntry entry) throws IOException {
        var baos = new ByteArrayOutputStream();
        var writer = new DataOutputStream(baos);

        switch (entry) {
            case LSMEntry.Put(String key, byte[] value, long timestamp) -> {
                writeLongLE(writer, timestamp);
                writer.writeByte(0);
                writeStringLE(writer, key);
                writeInt32LE(writer, value.length);
                writer.write(value);
            }
            case LSMEntry.Tombstone(String key, long timestamp) -> {
                writeLongLE(writer, timestamp);
                writer.writeByte(1);
                writeStringLE(writer, key);
            }
        }

        writer.flush();
        return baos.toByteArray();
    }

    public static LSMEntry unmarshall(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        long timestamp = buf.getLong();
        byte command = buf.get();
        int keyLen = buf.getInt();
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        return switch (command) {
            case 0 -> {
                int valueLen = buf.getInt();
                byte[] value = new byte[valueLen];
                buf.get(value);
                yield new LSMEntry.Put(key, value, timestamp);
            }
            case 1 -> new LSMEntry.Tombstone(key, timestamp);
            default -> throw new IllegalArgumentException(
                    "Unknown command: " + command + " for key: " + key
            );
        };
    }

    // ── Write helpers ─────────────────────────────────────────────────────────

    public static void writeLongLE(DataOutputStream w, long v) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(v);
        w.write(buf.array());
    }

    public static void writeInt32LE(DataOutputStream w, int v) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(v);
        w.write(buf.array());
    }

    public static void writeStringLE(DataOutputStream w, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeInt32LE(w, bytes.length);
        w.write(bytes);
    }
}