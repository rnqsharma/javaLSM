package org.db.utility;

import org.db.core.SSTable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class SSTableIterable {

    private final SSTable ssTable;
    private final Path path;
    private LSMEntry entry;
    private final FileChannel channel;

    public SSTableIterable(SSTable ssTable, Path path, LSMEntry entry) {
        this.ssTable = ssTable;
        this.path = path;
        this.entry = entry;

        // Initializing FileChannel to avoid reopening and bad reads
        try (FileChannel fc = FileChannel.open(path, StandardOpenOption.READ)) {
            this.channel = fc;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public SSTable getSsTable() {
        return ssTable;
    }

    public Path getPath() {
        return path;
    }

    public LSMEntry getEntry() {
        return entry;
    }

    public void setEntry(LSMEntry entry) {
        this.entry = entry;
    }

    public boolean hasNext() {
        return channel.isOpen() && channelHasRemainingBytes();
    }

    public SSTableIterable next() {
        try {
            long size = readDataSize(this.channel);
            byte[] data = readEntryDataFromFile(this.channel, size);
            setEntry(unmarshall(data));

            return this;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private long readDataSize(FileChannel channel) throws IOException {
        // int64 requires 8 bytes
        ByteBuffer buffer = ByteBuffer.allocate(8);

        // Explicitly set Little Endian to match Go's binary.LittleEndian
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read exactly 8 bytes from the current file position
        while (buffer.hasRemaining()) {
            if (channel.read(buffer) == -1) {
                throw new IOException("Unexpected EOF while reading EntrySize");
            }
        }

        // Prepare the buffer for reading the long value
        buffer.flip();
        return buffer.getLong();
    }

    private byte[] readEntryDataFromFile(FileChannel fc, long size) throws IOException {
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Entry size exceeds maximum Java array capacity (2GB)");
        }

        byte[] data = new byte[(int) size];
        ByteBuffer buffer = ByteBuffer.wrap(data);

        while (buffer.hasRemaining()) {
            int bytesRead = fc.read(buffer);
            if (bytesRead == -1) {
                throw new IOException("Unexpected end of file while reading entry data");
            }
        }

        return data;
    }

    public static LSMEntry unmarshall(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // 1. Read Timestamp (int64 → long)
        long timestamp = buffer.getLong();

        // 2. Read Command (byte)
        // Mirrors Go's Command enum: 0 = PUT, 1 = DELETE
        byte command = buffer.get();

        // 3. Read Key
        int keyLen = buffer.getInt();
        byte[] keyBytes = new byte[keyLen];
        buffer.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        // 4. Branch on command — mirrors Go's Command_PUT / Command_DELETE
        return switch (command) {
            case 0 -> {
                // PUT — read value bytes
                int valueLen = buffer.getInt();
                byte[] value = new byte[valueLen];
                buffer.get(value);
                yield new LSMEntry.Put(key, value, timestamp);
            }
            case 1 -> {
                // DELETE — no value, just a tombstone marker
                yield new LSMEntry.Tombstone(key, timestamp);
            }
            default -> throw new IllegalArgumentException(
                    "Unknown command byte: " + command + " for key: " + key
            );
        };
    }

    private boolean channelHasRemainingBytes() {
        try {
            return channel.position() < channel.size();
        } catch (IOException e) {
            return false;
        }
    }
}
