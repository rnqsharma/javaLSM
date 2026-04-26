package org.db.io;

import org.db.core.wal.WALEntry;
import org.db.dto.Command;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public final class WALEntrySerializer implements EntrySerializer<WALEntry> {

    public static final WALEntrySerializer INSTANCE = new WALEntrySerializer();
    private static final byte CMD_PUT = 0;
    private static final byte CMD_TOMBSTONE = 1;
    private static final byte CMD_WRITE_SST = 2;
    private WALEntrySerializer() {
    }

    private static byte commandToByte(Command command) {
        return switch (command) {
            case PUT -> CMD_PUT;
            case TOMBSTONE -> CMD_TOMBSTONE;
            case WRITE_SST -> CMD_WRITE_SST;
        };
    }

    private static Command byteToCommand(byte b) {
        return switch (b) {
            case CMD_PUT -> Command.PUT;
            case CMD_TOMBSTONE -> Command.TOMBSTONE;
            case CMD_WRITE_SST -> Command.WRITE_SST;
            default -> throw new IllegalArgumentException("Unknown command byte: " + b);
        };
    }

    // ── Command mapping ───────────────────────────────────────────────────────

    /**
     * Binary layout (little endian):
     * [8 bytes] timestamp
     * [1 byte]  command
     * [4 bytes] key length
     * [n bytes] key (UTF-8)
     * [4 bytes] value length (0 if null)
     * [n bytes] value
     * <p>
     * IOException declared by interface but never thrown here —
     * ByteBuffer operations do not throw checked exceptions.
     */
    @Override
    public byte[] marshall(WALEntry entry) {
        byte[] keyBytes = entry.key().getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = entry.value() != null ? entry.value() : new byte[0];

        int totalSize = Long.BYTES
                + 1
                + Integer.BYTES
                + keyBytes.length
                + Integer.BYTES
                + valueBytes.length;

        ByteBuffer buf = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);

        buf.putLong(entry.timestamp());
        buf.put(commandToByte(entry.command()));
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putInt(valueBytes.length);
        if (valueBytes.length > 0) {
            buf.put(valueBytes);
        }

        return buf.array();
    }

    @Override
    public WALEntry unmarshall(byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        long timestamp = buf.getLong();
        Command command = byteToCommand(buf.get());
        int keyLen = buf.getInt();
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        int valueLen = buf.getInt();
        byte[] value = null;
        if (valueLen > 0) {
            value = new byte[valueLen];
            buf.get(value);
        }

        return new WALEntry(key, value, command, timestamp);
    }
}