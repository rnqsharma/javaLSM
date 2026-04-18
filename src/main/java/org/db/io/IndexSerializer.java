package org.db.io;

import org.db.dto.IndexEntry;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class IndexSerializer {

    private IndexSerializer() {
    }

    public static byte[] serialise(List<IndexEntry> entries) {
        int totalSize = Integer.BYTES;
        for (IndexEntry entry : entries) {
            totalSize += Integer.BYTES
                    + entry.key().getBytes(StandardCharsets.UTF_8).length
                    + Long.BYTES;
        }

        ByteBuffer buf = ByteBuffer
                .allocate(totalSize)
                .order(ByteOrder.LITTLE_ENDIAN);

        buf.putInt(entries.size());
        for (IndexEntry entry : entries) {
            byte[] keyBytes = entry.key().getBytes(StandardCharsets.UTF_8);
            buf.putInt(keyBytes.length);
            buf.put(keyBytes);
            buf.putLong(entry.offset());
        }

        return buf.array();
    }

    public static List<IndexEntry> deserialise(byte[] data) {
        ByteBuffer buf = ByteBuffer
                .wrap(data)
                .order(ByteOrder.LITTLE_ENDIAN);

        int numEntries = buf.getInt();
        var entries = new ArrayList<IndexEntry>(numEntries);

        for (int i = 0; i < numEntries; i++) {
            int keyLen = buf.getInt();
            byte[] keyBytes = new byte[keyLen];
            buf.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);
            long offset = buf.getLong();
            entries.add(new IndexEntry(key, offset));
        }

        return entries;
    }
}