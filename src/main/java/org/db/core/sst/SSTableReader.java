package org.db.core.sst;

import org.db.core.BloomFilter;
import org.db.io.ChannelIO;
import org.db.io.LSMEntrySerializer;
import org.db.io.IndexSerializer;
import org.db.dto.IndexEntry;
import org.db.dto.SSTableMetadata;
import org.db.dto.LSMEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Optional;

public final class SSTableReader {

    private SSTableReader() {
    }

    public static SSTableMetadata readMetadata(FileChannel fc) throws IOException {
        long dataOffset = 0;

        long bloomSize = ChannelIO.readInt64LE(fc);
        dataOffset += Long.BYTES;

        byte[] bloomData = ChannelIO.readBytes(fc, bloomSize);
        dataOffset += bloomData.length;

        long indexSize = ChannelIO.readInt64LE(fc);
        dataOffset += Long.BYTES;

        byte[] indexData = ChannelIO.readBytes(fc, indexSize);
        dataOffset += indexData.length;

        BloomFilter bloomFilter = BloomFilter.deSerialise(bloomData);
        List<IndexEntry> index = IndexSerializer.INSTANCE.unmarshall(indexData);

        return new SSTableMetadata(bloomFilter, index, dataOffset);
    }

    /**
     * Scans forward from `offset` in the entries section looking for `key`.
     * Uses positional reads — thread-safe for concurrent get() calls.
     *
     * @param fc         open FileChannel — shared across threads
     * @param dataOffset byte position where entries section begins
     * @param offset     relative offset within entries section (from index)
     * @param key        key to find
     */
    public static Optional<LSMEntry> scanFromOffset(FileChannel fc,
                                                    long dataOffset,
                                                    long offset,
                                                    String key) throws IOException {
        long pos = dataOffset + offset;
        long fileSize = fc.size();
        long[] sizeResult = new long[1];
        byte[][] dataResult = new byte[1][];

        while (pos < fileSize) {
            pos = ChannelIO.readInt64LEAt(fc, pos, sizeResult);
            long entrySize = sizeResult[0];

            if (entrySize <= 0 || entrySize > Integer.MAX_VALUE) {
                throw new IOException(
                        "Corrupt entry size: " + entrySize + " at position: "
                                + (pos - Long.BYTES)
                );
            }

            pos = ChannelIO.readBytesAt(fc, pos, entrySize, dataResult);

            LSMEntry entry = LSMEntrySerializer.INSTANCE.unmarshall(dataResult[0]);
            int cmp = entry.key().compareTo(key);

            if (cmp == 0) return Optional.of(entry);
            if (cmp > 0) return Optional.empty(); // past target key
            // cmp < 0 — keep scanning forward
        }

        return Optional.empty();
    }
}