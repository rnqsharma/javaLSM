package org.db.core;

import org.db.core.sst.SSTable;
import org.db.dto.Command;
import org.db.dto.Levels;
import org.db.utility.LSMEntry;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class MemtableFlusher {

    private final Path directory;
    private final AtomicLong currentSSTSequence;
    private final Levels[] levels;
    private final List<Memtable> flushingQueue;
    private final ReentrantReadWriteLock flushingQueueMutex;
    private final BlockingQueue<Integer> compactionChan;
    private final WriteAheadLogger wal;
    private final String sstablePrefix;

    public MemtableFlusher(Path directory,
                           AtomicLong currentSSTSequence,
                           Levels[] levels,
                           List<Memtable> flushingQueue,
                           ReentrantReadWriteLock flushingQueueMutex,
                           BlockingQueue<Integer> compactionChan,
                           WriteAheadLogger wal,
                           String sstablePrefix) {
        this.directory = directory;
        this.currentSSTSequence = currentSSTSequence;
        this.levels = levels;
        this.flushingQueue = flushingQueue;
        this.flushingQueueMutex = flushingQueueMutex;
        this.compactionChan = compactionChan;
        this.wal = wal;
        this.sstablePrefix = sstablePrefix;
    }

    /**
     * Flushes one immutable memtable to a Level 0 SSTable on disk.
     * Mirrors Go's flushMemtable(memtable *Memtable).
     */
    public void flush(Memtable memtable) {
        if (memtable.sizeInBytes() == 0) {
            removeFromFlushingQueue();
            return;
        }

        SSTable sst = writeSSTable(memtable);
        commitToLevel0(sst, getSSTablePath(0, currentSSTSequence.get()));
        signalCompaction();
    }

    private SSTable writeSSTable(Memtable memtable) {
        long sequence = currentSSTSequence.incrementAndGet();
        Path path = getSSTablePath(0, sequence);
        List<LSMEntry> entries = memtable.getEntries();

        try {
            return SSTable.write(path, entries);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed to flush memtable to SSTable: " + path.getFileName(), e
            );
        }
    }

    private void commitToLevel0(SSTable sst, Path path) {
        // Canonical lock order: level lock → flushingQueueMutex
        levels[0].getMutex().writeLock().lock();
        flushingQueueMutex.writeLock().lock();
        try {
            wal.createCheckPoint(new WriteAheadLog.WALEntry(
                    path.getFileName().toString(),
                    null,
                    Command.WRITE_SST,
                    System.currentTimeMillis()
            ));
            levels[0].getSsTables().add(sst);
            removeFromFlushingQueue();
        } finally {
            flushingQueueMutex.writeLock().unlock();
            levels[0].getMutex().writeLock().unlock();
        }
    }

    private void removeFromFlushingQueue() {
        flushingQueueMutex.writeLock().lock();
        try {
            if (!flushingQueue.isEmpty()) {
                flushingQueue.removeFirst();
            }
        } finally {
            flushingQueueMutex.writeLock().unlock();
        }
    }

    private void signalCompaction() {
        try {
            boolean signalled = compactionChan.offer(0, 1, TimeUnit.SECONDS);
            if (!signalled) {
                System.err.println("compactionChan full — compaction may be stuck");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private Path getSSTablePath(int level, long sequence) {
        return directory.resolve(
                "%s%d_%d".formatted(sstablePrefix, level, sequence)
        );
    }
}