package org.db.core.sst;

import org.db.core.BloomFilter;
import org.db.io.ChannelIO;
import org.db.io.EntrySerializer;
import org.db.io.IndexSerializer;
import org.db.dto.IndexEntry;
import org.db.dto.MetadataAndBuffer;
import org.db.utility.LSMEntry;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public final class SSTableWriter {

    private SSTableWriter() {}

    public static SSTableWriteResult write(Path path,
                                           List<LSMEntry> entries) throws IOException {
        var built = buildBuffers(entries);

        byte[] bloomData = built.bloomFilter().serialize();
        byte[] indexData = IndexSerializer.serialise(built.indexEntries());

        long dataOffset = writeToDisk(path, bloomData, indexData, built.entriesBuffer());

        return new SSTableWriteResult(
            built.bloomFilter(),
            built.indexEntries(),
            dataOffset
        );
    }

    private static MetadataAndBuffer buildBuffers(List<LSMEntry> entries)
            throws IOException {

        var bloomFilter   = new BloomFilter(1_000_000);
        var indexEntries  = new ArrayList<IndexEntry>();
        var entriesBuffer = new ByteArrayOutputStream();
        var writer        = new DataOutputStream(entriesBuffer);

        long currentOffset = 0;

        for (LSMEntry entry : entries) {
            byte[] data      = EntrySerializer.marshall(entry);
            long   entrySize = data.length;

            indexEntries.add(new IndexEntry(entry.key(), currentOffset));
            bloomFilter.add(entry.key());

            EntrySerializer.writeLongLE(writer, entrySize); // size prefix
            writer.write(data);

            currentOffset += Long.BYTES + entrySize;
        }

        writer.flush();
        return new MetadataAndBuffer(bloomFilter, indexEntries,
                                     entriesBuffer.toByteArray());
    }

    private static long writeToDisk(Path path, byte[] bloomData,
                                     byte[] indexData,
                                     byte[] entriesBuffer) throws IOException {
        try (FileChannel fc = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            long dataOffset = 0;
            dataOffset += ChannelIO.writeInt64LE(fc, bloomData.length);
            dataOffset += ChannelIO.writeBytes(fc, bloomData);
            dataOffset += ChannelIO.writeInt64LE(fc, indexData.length);
            dataOffset += ChannelIO.writeBytes(fc, indexData);
            ChannelIO.writeBytes(fc, entriesBuffer);

            fc.force(true); // fsync before returning
            return dataOffset;
        }
    }

    public record SSTableWriteResult(
        BloomFilter      bloomFilter,
        List<IndexEntry> indexEntries,
        long             dataOffset
    ) {}
}