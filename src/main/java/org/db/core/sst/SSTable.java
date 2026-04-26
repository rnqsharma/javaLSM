package org.db.core.sst;

import org.db.core.BloomFilter;
import org.db.dto.IndexEntry;
import org.db.dto.SSTableMetadata;
import org.db.dto.LSMEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;

/**
 * Facade — coordinates SSTableReader and SSTableWriter.
 * Owns only: file lifecycle (open/close) and the get() entry point.
 *
 * All heavy lifting delegated to:
 *   SSTableWriter  — write path
 *   SSTableReader  — read path
 *   EntrySerializer — entry marshalling
 *   IndexSerializer — index marshalling
 *   ChannelIO       — raw binary I/O
 */
public final class SSTable implements Closeable {

    private final Path             path;
    private final FileChannel      fileChannel;
    private final BloomFilter      bloomFilter;
    private final List<IndexEntry> indexEntries;
    private final long             dataOffset;

    public SSTable(Path path, FileChannel fileChannel,
                   BloomFilter bloomFilter, List<IndexEntry> indexEntries,
                   long dataOffset) {
        this.path         = path;
        this.fileChannel  = fileChannel;
        this.bloomFilter  = bloomFilter;
        this.indexEntries = indexEntries;
        this.dataOffset   = dataOffset;
    }

    // ── Factory methods ───────────────────────────────────────────────────────

    public static SSTable open(Path path) throws IOException {
        FileChannel      fc       = FileChannel.open(path, StandardOpenOption.READ);
        SSTableMetadata metadata = SSTableReader.readMetadata(fc);
        return new SSTable(path, fc,
                metadata.bloomFilter(), metadata.index(), metadata.dataOffset());
    }

    public static SSTable write(Path path, List<LSMEntry> entries) throws IOException {
        var result  = SSTableWriter.write(path, entries);
        FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
        return new SSTable(path, fc,
                result.bloomFilter(), result.indexEntries(), result.dataOffset());
    }

    // ── Read path ─────────────────────────────────────────────────────────────

    public Optional<LSMEntry> get(String key) {
        if (!bloomFilter.mightContain(key)) {
            return Optional.empty();
        }

        long offset = binarySearchIndex(key);
        if (offset < 0) {
            return Optional.empty();
        }

        try {
            return SSTableReader.scanFromOffset(fileChannel, dataOffset, offset, key);
        } catch (IOException e) {
            throw new RuntimeException("get() failed for key: " + key, e);
        }
    }

    // ── Index lookup ──────────────────────────────────────────────────────────

    private long binarySearchIndex(String key) {
        int  lo            = 0;
        int  hi            = indexEntries.size() - 1;
        long closestOffset = -1;

        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            int cmp = indexEntries.get(mid).key().compareTo(key);

            if (cmp == 0)       return indexEntries.get(mid).offset();
            else if (cmp < 0) { closestOffset = indexEntries.get(mid).offset(); lo = mid + 1; }
            else                hi = mid - 1;
        }

        return closestOffset;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }

    public Path getDirectory()  { return path; }

    public long getDataOffset() { return dataOffset; }
}