package org.db.core;

import org.db.dto.IndexEntry;
import org.db.dto.MetadataAndBuffer;
import org.db.utility.LSMEntry;
import org.db.utility.SSTableIterable;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class SSTable {

    private final Path directory;
    private final FileChannel fileChannel;
    private final BloomFilter bloomFilter;
    private final List<IndexEntry> indexEntries;
    private final long dataOffset;

    public SSTable(Path directory, FileChannel fileChannel,
                   BloomFilter bloomFilter, List<IndexEntry> indexEntries,
                   long dataOffset) {
        this.directory = directory;
        this.fileChannel = fileChannel;
        this.bloomFilter = bloomFilter;
        this.indexEntries = indexEntries;
        this.dataOffset = dataOffset;
    }

    public Path getDirectory() {
        return directory;
    }

    public static SSTable open(Path directory) throws IOException {
        var fileChannel = FileChannel.open(directory, StandardOpenOption.READ);
        var ssTableMetadata = readSSTableMetadata(fileChannel);
        return new SSTable(
                directory,
                fileChannel,
                ssTableMetadata.bloomFilter,
                ssTableMetadata.index,
                ssTableMetadata.dataOffset
        );
    }

    public void close() throws IOException {
        fileChannel.close();
    }

    public long getDataOffset() {
        return dataOffset;
    }

    public Optional<LSMEntry> get(String key) {
        if(!bloomFilter.mightContain(key)) {
            return Optional.empty();
        }

        long offset = binarySearchIndex(key);
        if(offset < 0) {
            return Optional.empty();
        }

        return scanFromOffset(key, offset);
    }

    private long binarySearchIndex(String key) {
        int lo = 0;
        int hi = indexEntries.size() - 1;
        long closestOffset = -1;

        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            int cmp = indexEntries.get(mid).key().compareTo(key);

            if (cmp == 0) {
                return indexEntries.get(mid).offset(); // exact match
            } else if (cmp < 0) {
                closestOffset = indexEntries.get(mid).offset(); // best candidate so far
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }

        return closestOffset; // scan forward from here
    }

    private Optional<LSMEntry> scanFromOffset(String key, long offset) {
        try {
            long position = dataOffset + offset;
            while(position < fileChannel.size()) {
                long entrySize = readInt64LE(fileChannel, position);

                byte[] data = readBytes(fileChannel, position, entrySize);

                LSMEntry entry = SSTableIterable.unmarshall(data);

                int cmp = entry.key().compareTo(key);

                if(cmp == 0) {
                    return Optional.of(entry);
                }

                if(cmp > 0) {
                    return Optional.empty();
                }
            }

            return Optional.empty();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static SSTable write(Path directory, List<LSMEntry> entries) throws IOException {
        // ── Phase 1: build metadata and entries buffer ─────────────────────────
        var result = buildMetadataAndEntriesBuffer(entries);

        // ── Phase 2: write to disk ─────────────────────────────────────────────
        long dataOffset = writeSSTableFile(directory, result);

        // ── Phase 3: open file handle and return SSTable ───────────────────────
        FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ);
        return new SSTable(directory, channel, result.bloomFilter(), result.indexEntries(), dataOffset);
    }

    private static long writeSSTableFile(Path directory, MetadataAndBuffer result) throws IOException {
        byte[] bloomFilterData = result.bloomFilter().serialize();
        byte[] indexData = serialiseIndex(result.indexEntries());

        try (FileChannel fc = FileChannel.open(
                directory,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            long dataOffset = 0;

            // ── Write bloom filter size (int64 LE) ────────────────────────────
            dataOffset += writeInt64LEToChannel(fc, bloomFilterData.length);

            // ── Write bloom filter data ───────────────────────────────────────
            dataOffset += writeToChannel(fc, bloomFilterData);

            // ── Write index size (int64 LE) ───────────────────────────────────
            dataOffset += writeInt64LEToChannel(fc, indexData.length);

            // ── Write index data ──────────────────────────────────────────────
            dataOffset += writeToChannel(fc, indexData);

            // ── Write entries ─────────────────────────────────────────────────
            writeToChannel(fc, result.entriesBuffer());

            // fsync — ensure data is on disk before returning
            fc.force(true);
            System.out.println("Wrote to SSTable");

            return dataOffset;
        }
    }

    private static MetadataAndBuffer buildMetadataAndEntriesBuffer(List<LSMEntry> entries) throws IOException {
        var bloomFilter = new BloomFilter(1_000_000);
        var indexEntries = new ArrayList<IndexEntry>();
        var entriesBuffer = new ByteArrayOutputStream();
        var entriesWriter = new DataOutputStream(entriesBuffer);

        long currentOffSet = 0;

        for (LSMEntry entry : entries) {
            byte[] data = marshall(entry);
            long entrySize = data.length;

            indexEntries.add(new IndexEntry(entry.key(), currentOffSet));

            bloomFilter.add(entry.key());

            writeInt64LE(entriesWriter, entrySize);

            entriesWriter.write(data);

            currentOffSet += Long.BYTES + entrySize;
        }
        entriesWriter.flush();
        return new MetadataAndBuffer(bloomFilter, indexEntries, entriesBuffer.toByteArray());
    }

    // ── Binary write helpers (all little endian to match Go) ──────────────────────

    private static void writeLongLE(DataOutputStream w, long v) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(v);
        w.write(buf.array());
    }

    private static void writeInt32LE(DataOutputStream w, int v) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buf.putInt(v);
        w.write(buf.array());
    }

    private static void writeStringLE(DataOutputStream w, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        writeInt32LE(w, bytes.length);
        w.write(bytes);
    }

    private static void writeInt64LE(DataOutputStream w, long v) throws IOException {
        writeLongLE(w, v);
    }

    private static long writeInt64LEToChannel(FileChannel fc, long v) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(v);
        buf.flip();
        while (buf.hasRemaining()) {
            fc.write(buf);
        }
        return Long.BYTES;  // TODO: Why??
    }

    private static long writeToChannel(FileChannel fc, byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            fc.write(buf);
        }
        return data.length;
    }

    // Mirrors Go's mustMarshal for a single LSMEntry
// Binary layout (little endian):
//   [8 bytes] timestamp
//   [1 byte]  command (0=PUT, 1=DELETE)
//   [4 bytes] key length
//   [n bytes] key (UTF-8)
//   [4 bytes] value length  (PUT only)
//   [n bytes] value         (PUT only)
    public static byte[] marshall(LSMEntry kv) throws IOException {
        var baos   = new ByteArrayOutputStream();
        var writer = new DataOutputStream(baos);

        // Must match unmarshall() read order exactly
        switch (kv) {
            case LSMEntry.Put(String key, byte[] value, long timestamp) when value != null -> {
                writeLongLE(writer, timestamp);
                writer.writeByte(0);                            // command = PUT
                writeStringLE(writer, key);                     // key
                writeInt32LE(writer, value.length);             // value length
                writer.write(value);                            // value bytes
            }
            case LSMEntry.Tombstone(String key, long timestamp) -> {
                writeLongLE(writer, timestamp);                 // timestamp
                writer.writeByte(1);                            // command = DELETE
                writeStringLE(writer, key);                     // key
                // no value for tombstone
            }
            default -> throw new IllegalStateException("Unexpected value: " + kv);
        }

        writer.flush();
        return baos.toByteArray();
    }

    private static byte[] serialiseIndex(List<IndexEntry> entries) {
        int totalSize = Integer.BYTES;

        // ── Phase 1: calculate total buffer size upfront ───────────────────────
        // Avoids ByteArrayOutputStream resizing — important for memory efficiency
        for (IndexEntry entry : entries) {
            byte[] keyBytes = entry.key().getBytes(StandardCharsets.UTF_8);
            totalSize += Integer.BYTES   // key length prefix
                    +  keyBytes.length // key bytes
                    +  Long.BYTES;     // offset
        }

        // ── Phase 2: write into pre-allocated buffer ───────────────────────────
        java.nio.ByteBuffer buf = java.nio.ByteBuffer
                .allocate(totalSize)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN);

        // Write number of entries
        buf.putInt(entries.size());

        // Write each entry
        for (IndexEntry entry : entries) {
            byte[] keyBytes = entry.key().getBytes(StandardCharsets.UTF_8);

            buf.putInt(keyBytes.length);    // key length
            buf.put(keyBytes);              // key bytes
            buf.putLong(entry.offset());    // offset into entries section
        }

        return buf.array();
    }

    /**
     * Mirrors Go's mustUnmarshal(bytes, index).
     * Restores the list of IndexEntry objects from bytes read off disk.
     */
    private static List<IndexEntry> deserializeIndex(byte[] data) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer
                .wrap(data)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN);

        int numEntries = buf.getInt();
        var entries = new ArrayList<IndexEntry>(numEntries);

        for (int i = 0; i < numEntries; i++) {
            // Read key
            int keyLen     = buf.getInt();
            byte[] keyBytes = new byte[keyLen];
            buf.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            // Read offset
            long offset = buf.getLong();

            entries.add(new IndexEntry(key, offset));
        }

        return entries;
    }

    private static SSTableMetadata readSSTableMetadata(FileChannel fc) throws IOException {
        long dataOffset = 0;

        long bloomFilterSize = readInt64LE(fc);
        dataOffset += Long.BYTES;

        byte[] bloomFilterData = readBytes(fc, bloomFilterSize);
        dataOffset += bloomFilterData.length;

        long indexSize = readInt64LE(fc);
        dataOffset += Long.BYTES;

        byte[] indexData = readBytes(fc, indexSize);
        dataOffset += indexData.length;

        BloomFilter bloomFilter = BloomFilter.deSerialise(bloomFilterData);
        List<IndexEntry> index = deserializeIndex(indexData);

        return new SSTableMetadata(bloomFilter, index, dataOffset);
    }

    // ── Binary read helpers ───────────────────────────────────────────────────────

    /**
     * Sequential read — advances fc.position().
     * Only safe when called from a single thread (e.g. SSTable.open).
     */
    private static long readInt64LE(FileChannel channel) throws IOException {
        ByteBuffer buf = ByteBuffer
                .allocate(Long.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);

        while (buf.hasRemaining()) {
            if (channel.read(buf) == -1) {
                throw new IOException("Unexpected EOF reading int64");
            }
        }
        buf.flip();
        return buf.getLong();
    }

    private static byte[] readBytes(FileChannel channel, long size) throws IOException {
        if (size > Integer.MAX_VALUE || size < 0) {
            throw new IllegalArgumentException("Invalid size: " + size);
        }
        byte[]     data = new byte[(int) size];
        ByteBuffer buf  = ByteBuffer.wrap(data);
        while (buf.hasRemaining()) {
            if (channel.read(buf) == -1) {
                throw new IOException("Unexpected EOF reading bytes, expected: " + size);
            }
        }
        return data;
    }

    /**
     * Reads exactly 8 bytes from the channel and returns them as a long.
     */
    private static long readInt64LE(FileChannel channel, long position) throws IOException {
        ByteBuffer buf = ByteBuffer
                .allocate(Long.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);

        long pos = position;
        while (buf.hasRemaining()) {
            int bytesRead = channel.read(buf, pos);
            if (bytesRead == -1) {
                throw new IOException("Unexpected EOF while reading size prefix");
            }
            pos += bytesRead;
        }

        buf.flip();
        return buf.getLong();
    }

    /**
     * Reads exactly `size` bytes from the channel.
     * Mirrors Go's: make([]byte, size) followed by file.Read(data)
     */
    private static byte[] readBytes(FileChannel channel, long position, long size) throws IOException {
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Section size %d exceeds max Java array capacity".formatted(size)
            );
        }

        byte[]     data = new byte[(int) size];
        ByteBuffer buf  = ByteBuffer.wrap(data);
        long pos  = position;

        while (buf.hasRemaining()) {
            int bytesRead = channel.read(buf, pos);
            if (bytesRead == -1) {
                throw new IOException(
                        "Unexpected EOF — expected %d bytes, got %d".formatted(size, buf.position())
                );
            }
            pos += bytesRead;
        }

        return data;
    }

// ── Supporting record ─────────────────────────────────────────────────────────

    /**
     * Holds the three values returned by readSSTableMetadata.
     * Mirrors Go's three return values:
     * (*BloomFilter, *Index, EntrySize, error)
     */
    private record SSTableMetadata(
            BloomFilter      bloomFilter,
            List<IndexEntry> index,
            long             dataOffset
    ) {}

}