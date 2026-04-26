package org.db.core.wal;

import org.db.io.WALEntrySerializer;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32;

public class WriteAheadLog implements WriteAheadLogger, Closeable {

    private static final String SEGMENT_PREFIX = "segment-";
    private static final long SYNC_INTERVAL_MS = 200;
    private final Path directory;
    private final boolean shouldFsync;
    private final long maxFileSize;
    private final int maxSegments;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicLong lastSequenceNo = new AtomicLong(0);
    private final ScheduledExecutorService syncExecutor;
    private FileChannel currentSegment;
    private BufferedOutputStream bufWriter;
    private FileOutputStream fileOutputStream;
    private int currentSegmentIndex;
    private volatile boolean closed = false;

    private WriteAheadLog(Path directory,
                          boolean shouldFsync,
                          long maxFileSize,
                          int maxSegments,
                          FileChannel currentSegment,
                          BufferedOutputStream bufWriter,
                          FileOutputStream fileOutputStream,
                          int currentSegmentIndex,
                          long lastSeq) {
        this.directory = directory;
        this.shouldFsync = shouldFsync;
        this.maxFileSize = maxFileSize;
        this.maxSegments = maxSegments;
        this.currentSegment = currentSegment;
        this.bufWriter = bufWriter;
        this.fileOutputStream = fileOutputStream;
        this.currentSegmentIndex = currentSegmentIndex;
        this.lastSequenceNo.set(lastSeq);

        this.syncExecutor = Executors.newSingleThreadScheduledExecutor(r ->
                Thread.ofVirtual().name("wal-syncer").unstarted(r)
        );
        this.syncExecutor.scheduleAtFixedRate(
                this::keepSyncing,
                SYNC_INTERVAL_MS,
                SYNC_INTERVAL_MS,
                TimeUnit.MILLISECONDS
        );
    }

    public static WriteAheadLog open(Path directory, boolean enableFsync,
                                     long maxFileSize, int maxSegments) throws IOException {
        Files.createDirectories(directory);

        List<Path> segmentFiles = findSegmentFiles(directory);

        int lastSegmentIndex;
        if (!segmentFiles.isEmpty()) {
            lastSegmentIndex = findLastSegmentIndex(segmentFiles);
        } else {
            createSegmentFile(directory, 0);
            lastSegmentIndex = 0;
        }

        Path segmentPath = segmentPath(directory, lastSegmentIndex);
        FileOutputStream fos = new FileOutputStream(segmentPath.toFile(), true);
        FileChannel fc = fos.getChannel();
        BufferedOutputStream bufWriter = new BufferedOutputStream(fos);


        WriteAheadLog wal = new WriteAheadLog(
                directory, enableFsync, maxFileSize, maxSegments,
                fc, bufWriter, fos, lastSegmentIndex, 0
        );

        // Read last sequence number from the segment
        long lastSeq = wal.getLastSequenceNo();
        wal.lastSequenceNo.set(lastSeq);

        return wal;
    }

    private static List<Path> findSegmentFiles(Path directory) throws IOException {
        try (var stream = Files.list(directory)) {
            return stream
                    .filter(p -> p.getFileName().toString().startsWith(SEGMENT_PREFIX))
                    .sorted(Comparator.comparingInt(p ->
                            Integer.parseInt(p.getFileName().toString()
                                    .replace(SEGMENT_PREFIX, ""))
                    ))
                    .toList();
        }
    }

    private static int findLastSegmentIndex(List<Path> segmentFiles) {
        return segmentFiles.stream()
                .mapToInt(
                        p -> Integer.parseInt(
                                p.getFileName().toString().replace(SEGMENT_PREFIX, "")
                        ))
                .max()
                .orElse(0);
    }

    private static void createSegmentFile(Path directory, int segmentIndex) throws IOException {
        Path path = segmentPath(directory, segmentIndex);
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
    }

    private static Path segmentPath(Path directory, int index) {
        return directory.resolve(SEGMENT_PREFIX + index);
    }

    private static WALRecord deserializeAndVerify(byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        long lsn = buf.getLong();
        int dataLen = buf.getInt();
        byte[] entryData = new byte[dataLen];
        buf.get(entryData);

        long storedCRC = buf.getInt() & 0xFFFFFFFFL;
        boolean isCheckpoint = buf.get() == 1;

        long computedCRC = computeCRC(entryData, lsn);
        if (computedCRC != storedCRC) {
            throw new IOException(
                    "CRC mismatch at LSN " + lsn
                            + " — expected " + storedCRC + ", got " + computedCRC
            );
        }
        return new WALRecord(lsn, entryData, storedCRC, isCheckpoint);
    }

    private static long computeCRC(byte[] data, long lsn) {
        CRC32 crc = new CRC32();
        crc.update(data);
        crc.update((byte) lsn); // append LSN byte
        return crc.getValue();
    }

    /**
     * Serialises a WALRecord to bytes for writing to disk.
     * <p>
     * Binary layout (little endian):
     * [8 bytes] logSequenceNumber
     * [4 bytes] dataLength
     * [n bytes] data               ← serialised application WALEntry
     * [4 bytes] crc32
     * [1 byte]  isCheckpoint
     */
    private static byte[] serialiseRecord(WALRecord record) {
        byte[] data = record.data() != null ? record.data() : new byte[0];
        int totalSize = Long.BYTES + Integer.BYTES + data.length + Integer.BYTES + 1;

        ByteBuffer buf = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
        buf.putLong(record.logSequenceNumber());
        buf.putInt(data.length);
        buf.put(data);
        buf.putInt((int) record.crc());
        buf.put(record.isCheckpoint() ? (byte) 1 : (byte) 0);

        return buf.array();
    }

    /**
     * Deserializes bytes into a WALRecord and verifies CRC.
     * Mirrors Go's unmarshalAndVerifyEntry().
     */
    private static WALRecord deserializeAndVerifyRecord(byte[] data) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        long lsn = buf.getLong();
        int dataLen = buf.getInt();
        byte[] entryData = new byte[dataLen];
        buf.get(entryData);
        long storedCRC = buf.getInt() & 0xFFFFFFFFL;
        boolean isCheckpoint = buf.get() == 1;

        long computedCRC = computeCRC(entryData, lsn);
        if (computedCRC != storedCRC) {
            throw new IOException(
                    "CRC mismatch at LSN " + lsn
                            + " — expected " + storedCRC + " got " + computedCRC
            );
        }

        return new WALRecord(lsn, entryData, storedCRC, isCheckpoint);
    }

    private long getLastSequenceNo() throws IOException {
        Path segPath = segmentPath(directory, currentSegmentIndex);

        if (!Files.exists(segPath) || Files.size(segPath) == 0) {
            return 0;
        }

        long lastLSN = 0;
        try (FileInputStream fis = new FileInputStream(segPath.toFile());
             DataInputStream dis = new DataInputStream(new BufferedInputStream(fis))) {

            while (true) {
                byte[] sizeBytes = new byte[Integer.BYTES];
                int n = dis.read(sizeBytes);
                if (n == -1) break;
                if (n < Integer.BYTES) break;

                int size = ByteBuffer.wrap(sizeBytes)
                        .order(ByteOrder.LITTLE_ENDIAN)
                        .getInt();

                byte[] data = new byte[size];

                try {
                    dis.readFully(data);
                } catch (EOFException eof) {
                    break;
                }

                try {
                    WALRecord walRecord = deserializeAndVerify(data);
                    lastLSN = walRecord.logSequenceNumber();
                } catch (IOException e) {
                    break;
                }
            }
        }

        return lastLSN;
    }

    /**
     * Writes a data entry to the WAL.
     * Mirrors Go's WriteEntry(data []byte) error.
     */
    @Override
    public void writeEntry(WALEntry entry) {
        byte[] serialisedData = WALEntrySerializer.INSTANCE.marshall(entry);
        try {
            writeRecordInternal(serialisedData, false);
        } catch (IOException e) {
            throw new RuntimeException("WAL writeEntry failed", e);
        }
    }

    @Override
    public void createCheckPoint(WALEntry entry) {
        byte[] serialisedData = WALEntrySerializer.INSTANCE.marshall(entry);
        try {
            writeRecordInternal(serialisedData, true);
        } catch (IOException e) {
            throw new RuntimeException("WAL writeEntry failed", e);
        }
    }

    @Override
    public List<WALEntry> readAll(boolean readFromCheckpoint) throws IOException {
        Path segPath = segmentPath(directory, currentSegmentIndex);

        try (FileInputStream fis = new FileInputStream(segPath.toFile())) {
            ReadResult result = readEntriesFromStream(fis, readFromCheckpoint);

            if (readFromCheckpoint && result.lastCheckpointLSN() <= 0) {
                return Collections.emptyList();
            }

            return result.entries();
        }
    }

    /**
     * Reads all entries from all segments starting from the given segment index.
     * Mirrors Go's ReadAllFromOffset(offset int, readFromCheckpoint bool).
     */
    @Override
    public List<WALEntry> readAllFromOffset(int offset,
                                            boolean readFromCheckpoint) throws IOException {
        List<Path> files = findSegmentFiles(directory);

        List<WALEntry> entries = new ArrayList<>();
        long prevCheckpointLSN = 0;

        for (Path file : files) {
            int segIndex = Integer.parseInt(
                    file.getFileName().toString().replace(SEGMENT_PREFIX, "")
            );

            // Skip segments before the requested offset
            if (segIndex < offset) continue;

            try (FileInputStream fis = new FileInputStream(file.toFile())) {
                ReadResult result = readEntriesFromStream(fis, readFromCheckpoint);

                // If we found a newer checkpoint, discard earlier entries
                if (readFromCheckpoint && result.lastCheckpointLSN() > prevCheckpointLSN) {
                    entries.clear();
                    prevCheckpointLSN = result.lastCheckpointLSN();
                }

                entries.addAll(result.entries());
            }
        }

        return entries;
    }

    /**
     * Reads all entries from a stream, deserialising WALRecords back into
     * application WALEntries via WALEntrySerializer.
     */
    private ReadResult readEntriesFromStream(InputStream stream,
                                             boolean readFromCheckpoint) throws IOException {
        List<WALEntry> entries = new ArrayList<>();
        long checkpointLSN = 0;

        DataInputStream dis = new DataInputStream(new BufferedInputStream(stream));

        while (true) {
            byte[] sizeBytes = new byte[Integer.BYTES];
            int n = dis.read(sizeBytes);
            if (n == -1) break;
            if (n < Integer.BYTES) throw new IOException("Truncated WAL size prefix");

            int size = ByteBuffer.wrap(sizeBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
            byte[] recordBytes = new byte[size];
            dis.readFully(recordBytes);

            // Deserialize WALRecord (verifies CRC)
            WALRecord record = deserializeAndVerifyRecord(recordBytes);

            // Deserialize inner data → application WALEntry
            WALEntry appEntry = WALEntrySerializer.INSTANCE.unmarshall(record.data());

            if (readFromCheckpoint && record.isCheckpoint()) {
                checkpointLSN = record.logSequenceNumber();
                entries.clear(); // discard entries before checkpoint
            }

            entries.add(appEntry);
        }

        return new ReadResult(entries, checkpointLSN);
    }

    /**
     * Core write method — builds WALRecord with LSN and CRC, writes to buffer.
     * Mirrors Go's writeEntry(data []byte, isCheckpoint bool).
     */
    private void writeRecordInternal(byte[] data, boolean isCheckpoint) throws IOException {
        lock.lock();
        try {
            rotateLogIfNeeded();

            long lsn = lastSequenceNo.incrementAndGet();
            long crc = computeCRC(data, lsn);

            // Checkpoint: flush everything BEFORE writing the marker
            if (isCheckpoint) {
                sync();
            }

            WALRecord record = new WALRecord(lsn, data, crc, isCheckpoint);
            writeRecordToBuffer(serialiseRecord(record));

        } finally {
            lock.unlock();
        }
    }

    private void keepSyncing() {
        if (closed) return;

        lock.lock();
        try {
            sync();
        } catch (IOException e) {
            System.err.println("WAL background sync failed: " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    private void sync() throws IOException {
        if (closed) return;

        bufWriter.flush();

        if (shouldFsync) {
            currentSegment.force(true);
        }
    }

    private void rotateLogIfNeeded() throws IOException {
        long currentSize = currentSegment.size();
        if (currentSize > maxFileSize) {
            rotateLog();
        }
    }

    private void rotateLog() throws IOException {
        // Flush and fsync current segment first
        sync();

        bufWriter.close();
//        currentSegment.close();

        currentSegmentIndex++;

        // Delete oldest segment if over the limit
        if (currentSegmentIndex >= maxSegments) {
            deleteOldestSegment();
        }

        createSegmentFile(directory, currentSegmentIndex);
        Path newPath = segmentPath(directory, currentSegmentIndex);
        FileOutputStream newFos = new FileOutputStream(newPath.toFile(), true);
        FileChannel newChannel = newFos.getChannel();

        this.fileOutputStream = newFos;
        this.currentSegment = newChannel;
        this.bufWriter = new BufferedOutputStream(newFos);
    }

    /**
     * Deletes the oldest segment file.
     */
    private void deleteOldestSegment() throws IOException {
        List<Path> files = findSegmentFiles(directory);
        if (files.isEmpty()) return;

        // Find segment with lowest index
        Path oldest = files.stream()
                .min(Comparator.comparingInt(p ->
                        Integer.parseInt(p.getFileName().toString()
                                .replace(SEGMENT_PREFIX, ""))
                ))
                .orElse(null);

        if (oldest != null) {
            Files.deleteIfExists(oldest);
            System.out.println("Deleted oldest WAL segment: " + oldest.getFileName());
        }
    }

    /**
     * Writes a size-prefixed serialised WAL record to the in-memory buffer.
     * Mirrors Go's writeEntryToBuffer(entry *WAL_Entry).
     * <p>
     * Layout written to buffer:
     * [4 bytes] record size  (int32 LE)
     * [n bytes] record data
     * <p>
     * The buffer is NOT flushed here — flushing happens either:
     * - Every SYNC_INTERVAL_MS via keepSyncing()
     * - Explicitly via sync() before a checkpoint
     * - On close()
     * <p>
     * Mirrors Go's:
     * size := int32(len(marshaledEntry))
     * binary.Write(wal.bufWriter, binary.LittleEndian, size)
     * wal.bufWriter.Write(marshaledEntry)
     */
    private void writeRecordToBuffer(byte[] serialisedRecord) throws IOException {
        // Write size prefix — mirrors Go's:
        // binary.Write(wal.bufWriter, binary.LittleEndian, size)
        ByteBuffer sizeBuf = ByteBuffer
                .allocate(Integer.BYTES)
                .order(ByteOrder.LITTLE_ENDIAN);
        sizeBuf.putInt(serialisedRecord.length);
        bufWriter.write(sizeBuf.array());

        // Write record bytes — mirrors Go's:
        // wal.bufWriter.Write(marshaledEntry)
        bufWriter.write(serialisedRecord);
    }

    @Override
    public void close() throws IOException {
        closed = true;

        // Stop the background sync thread
        syncExecutor.shutdown();
        try {
            syncExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Final flush and fsync
        lock.lock();
        try {
            sync();
            bufWriter.close();
//            currentSegment.close();
        } finally {
            lock.unlock();
        }
    }

    private record WALRecord(
            long logSequenceNumber,
            byte[] data,
            long crc,
            boolean isCheckpoint
    ) {
    }

    // Update ReadResult to hold application WALEntry list
    private record ReadResult(
            List<WALEntry> entries,
            long lastCheckpointLSN
    ) {
    }
}
