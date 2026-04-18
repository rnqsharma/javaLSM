package org.db.core;

import org.db.core.sst.SSTable;
import org.db.core.sst.SSTableLoader;
import org.db.dto.Command;
import org.db.dto.Levels;
import org.db.utility.LSMEntry;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class LSMTree implements Closeable {

    private static final int MAX_LEVELS = 6;
    private static final String WAL_DIR_SUFFIX = "_wal";
    private static final String SSTABLE_PREFIX = "sstable_";

    private static final Map<Integer, Integer> MAX_LEVEL_SSTABLES = Map.of(
            0, 4, 1, 8, 2, 16,
            3, 32, 4, 64, 5, 128, 6, 256
    );
    private final ReentrantReadWriteLock mutex = new ReentrantReadWriteLock();
    private final long maxMemtableSize;
    private final Path directory;
    private final WriteAheadLogger wal;
    private final Levels[] levels;
    private final AtomicLong currentSSTSequence = new AtomicLong(0);
    private final List<Memtable> flushingQueue = new ArrayList<>();
    private final ReentrantReadWriteLock flushingQueueMutex = new ReentrantReadWriteLock();
    private final BlockingQueue<Memtable> flushingChan = new LinkedBlockingQueue<>(1000);
    private final BlockingQueue<Integer> compactionChan = new LinkedBlockingQueue<>(100);
    private final CountDownLatch backgroundThreadLatch = new CountDownLatch(2);
    // ── Delegated components ──────────────────────────────────────────────────
    private final MemtableFlusher flusher;
    private final CompactionManager compactionManager;
    // ── State ─────────────────────────────────────────────────────────────────
    private volatile Memtable memtable;
    private volatile boolean inRecovery;
    private volatile boolean closed;

    private LSMTree(Path directory, long maxMemtableSize, WriteAheadLogger wal) {
        this.directory = directory;
        this.maxMemtableSize = maxMemtableSize;
        this.wal = wal;
        this.memtable = new Memtable();
        this.levels = new Levels[MAX_LEVELS];
        Arrays.setAll(levels, i -> new Levels());

        this.flusher = new MemtableFlusher(
                directory, currentSSTSequence, levels,
                flushingQueue, flushingQueueMutex,
                compactionChan, wal, SSTABLE_PREFIX
        );

        this.compactionManager = new CompactionManager(
                levels, MAX_LEVELS, MAX_LEVEL_SSTABLES,
                currentSSTSequence, directory,
                SSTABLE_PREFIX, compactionChan
        );
    }

    public static LSMTree open(Path directory,
                               long maxMemtableSize,
                               boolean recoverFromWAL) throws IOException {

        WriteAheadLogger wal = WriteAheadLog.open(
                directory.resolve(directory.getFileName() + WAL_DIR_SUFFIX),
                true, 128_000, 1_000
        );

        LSMTree lsm = new LSMTree(directory, maxMemtableSize, wal);
        lsm.closed = false;
        lsm.inRecovery = recoverFromWAL;

        // Load existing SSTables from disk
        new SSTableLoader(
                directory, SSTABLE_PREFIX, lsm.levels, lsm.currentSSTSequence
        ).load();

        // Start background virtual threads
        Thread.ofVirtual()
                .name("compaction")
                .start(() -> {
                    try {
                        lsm.backgroundCompaction();
                    } catch (Exception e) {
                        System.err.println("compaction: " + e.getMessage());
                    } finally {
                        lsm.backgroundThreadLatch.countDown();
                    }
                });

        Thread.ofVirtual()
                .name("memtable-flusher")
                .start(() -> {
                    try {
                        lsm.backgroundMemtableFlushing();
                    } catch (Exception e) {
                        System.err.println("flusher: " + e.getMessage());
                    } finally {
                        lsm.backgroundThreadLatch.countDown();
                    }
                });

        if (recoverFromWAL) lsm.recoverFromWAL();

        return lsm;
    }

    public void put(String key, byte[] value) {
        mutex.writeLock().lock();
        Memtable fullMemtable = null;
        try {
            if (!inRecovery) {
                wal.writeEntry(new WriteAheadLog.WALEntry(
                        key, value, Command.PUT, System.currentTimeMillis()
                ));
            }
            memtable.put(key, value);
            fullMemtable = maybeRotateMemtable();
        } finally {
            mutex.writeLock().unlock();
        }
        enqueueForFlush(fullMemtable);
    }

    public void delete(String key) {
        mutex.writeLock().lock();
        Memtable fullMemtable = null;
        try {
            if (!inRecovery) {
                wal.writeEntry(new WriteAheadLog.WALEntry(
                        key, null, Command.TOMBSTONE, System.currentTimeMillis()
                ));
            }
            memtable.delete(key);
            fullMemtable = maybeRotateMemtable();
        } finally {
            mutex.writeLock().unlock();
        }
        enqueueForFlush(fullMemtable);
    }

    /**
     * Rotates the active memtable if it exceeds capacity.
     * Called under write lock. Returns the full memtable or null.
     */
    private Memtable maybeRotateMemtable() {
        if (memtable.sizeInBytes() <= maxMemtableSize) return null;

        Memtable full = memtable;
        memtable = new Memtable();

        flushingQueueMutex.writeLock().lock();
        try {
            flushingQueue.add(full);
        } finally {
            flushingQueueMutex.writeLock().unlock();
        }

        return full;
    }

    /**
     * Enqueues a full memtable for flushing.
     * Called AFTER releasing the write lock — prevents deadlock.
     */
    private void enqueueForFlush(Memtable full) {
        if (full == null) return;
        try {
            flushingChan.put(full); // blocking — guaranteed to enqueue
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ── Read path ─────────────────────────────────────────────────────────────

    public byte[] get(String key) {
        // 1. Active memtable
        mutex.readLock().lock();
        LSMEntry value;
        try {
            value = memtable.get(key);
        } finally {
            mutex.readLock().unlock();
        }
        if (value != null) return resolveEntry(value);

        // 2. Flushing queue — newest first
        flushingQueueMutex.readLock().lock();
        try {
            for (int i = flushingQueue.size() - 1; i >= 0; i--) {
                value = flushingQueue.get(i).get(key);
                if (value != null) return resolveEntry(value);
            }
        } finally {
            flushingQueueMutex.readLock().unlock();
        }

        // 3. SSTable levels — newest SSTable first within each level
        for (Levels level : levels) {
            level.getMutex().readLock().lock();
            try {
                List<SSTable> ssts = level.getSsTables();
                for (int i = ssts.size() - 1; i >= 0; i--) {
                    value = ssts.get(i).get(key).orElse(null);
                    if (value != null) return resolveEntry(value);
                }
            } finally {
                level.getMutex().readLock().unlock();
            }
        }

        return null;
    }

    private byte[] resolveEntry(LSMEntry entry) {
        return switch (entry) {
            case LSMEntry.Put(var k, var v, var ts) -> v;
            case LSMEntry.Tombstone ignored -> null;
        };
    }

    // ── Background threads ────────────────────────────────────────────────────

    private void backgroundMemtableFlushing() {
        try {
            while (true) {
                Memtable toFlush = flushingChan.poll(100, TimeUnit.MILLISECONDS);

                if (toFlush != null) {
                    try {
                        flusher.flush(toFlush);  // delegate to MemtableFlusher
                    } catch (Exception e) {
                        System.err.println("flush failed: " + e.getMessage());
                        e.printStackTrace();
                    }
                } else if (closed && flushingChan.isEmpty() && flushingQueue.isEmpty()) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void backgroundCompaction() {
        try {
            while (true) {
                Integer candidate = compactionChan.poll(100, TimeUnit.MILLISECONDS);

                if (candidate != null) {
                    try {
                        compactionManager.compact(candidate); // delegate to CompactionManager
                    } catch (Exception e) {
                        System.err.println("compact failed: " + e.getMessage());
                        e.printStackTrace();
                    }
                } else if (closed && compactionChan.isEmpty()) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws IOException {
        // Enqueue final active memtable
        mutex.writeLock().lock();
        Memtable finalMemtable = memtable;
        memtable = new Memtable();
        flushingQueueMutex.writeLock().lock();
        try {
            flushingQueue.add(finalMemtable);
        } finally {
            flushingQueueMutex.writeLock().unlock();
            mutex.writeLock().unlock();
        }

        try {
            flushingChan.offer(finalMemtable, 5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Wait for flushingChan to drain before signalling shutdown
        long deadline = System.currentTimeMillis() + 30_000;
        while (!flushingChan.isEmpty() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        closed = true;

        try {
            if (!backgroundThreadLatch.await(30, TimeUnit.SECONDS)) {
                System.err.println("Background threads did not finish within 30s");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        wal.close();
        flushingChan.clear();
        compactionChan.clear();

        for (Levels level : levels) {
            for (SSTable sst : level.getSsTables()) {
                sst.close();
            }
        }
    }

    private void recoverFromWAL() throws IOException {
        inRecovery = true;
        // WAL recovery implementation
        inRecovery = false;
    }

    public BlockingQueue<Memtable> getFlushingChan() {
        return flushingChan;
    }

    public BlockingQueue<Integer> getCompactionChan() {
        return compactionChan;
    }
}