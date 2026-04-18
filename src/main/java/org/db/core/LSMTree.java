package org.db.core;

import org.db.dto.Command;
import org.db.dto.Levels;
import org.db.utility.LSMEntry;
import org.db.utility.SSTableIterable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMTree {

    /** Pairs of SSTable handles + their iterators (returned from getSSTableHandlesAtLevel) */
    record SSTableHandles(List<SSTable> sstables, List<SSTableIterable> iterators) {}

    private final static int MAX_LEVELS = 6;
    private final static String WAL_DIR_SUFFIX = "_wal";
    private final static String SSTABLE_PREFIX = "sstable_";

    private static final Map<Integer, Integer> MAX_LEVELS_SSTABLES = Map.of(
            0,  4,
            1,  8,
            2,  16,
            3,  32,
            4,  64,
            5,  128,
            6,  256
    );

    private volatile Memtable memtable;
    private final  ReentrantReadWriteLock mutex = new ReentrantReadWriteLock();
    private final long maxMemtableSize;
    private final Path directory;
    private final WriteAheadLog wal;
    private volatile boolean inRecovery;
    private final Levels[] levels;

    private final AtomicLong currentSSTSequence = new AtomicLong(0);

    private final List<Memtable> flushingQueue = new ArrayList<>();
    private final BlockingQueue<Memtable> flushingChan = new LinkedBlockingQueue<>(1000);
    private final ReentrantReadWriteLock flushingQueueMutex = new ReentrantReadWriteLock();

    public BlockingQueue<Integer> getCompactionChan() {
        return compactionChan;
    }

    public BlockingQueue<Memtable> getFlushingChan() {
        return flushingChan;
    }

    // Channels → bounded blocking queues
    private final BlockingQueue<Integer> compactionChan = new LinkedBlockingQueue<>(100);

    private volatile boolean closed;

    private final CountDownLatch backgroundThreadLatch; // one per background thread

    private LSMTree(Path directory, long maxMemtableSize, WriteAheadLog wal) {
        this.directory = directory;
        this.maxMemtableSize = maxMemtableSize;
        this.wal = wal;
        this.memtable = new Memtable();

        // Initializing levels
        this.levels = new Levels[MAX_LEVELS];
        Arrays.setAll(levels, i -> new Levels());

        // Similar to goroutines
        backgroundThreadLatch = new CountDownLatch(2);

        System.out.println("Creating LSMTree with maxMemtableSize " + maxMemtableSize);
    }

    public static LSMTree open(Path directory, long maxMemtableSize, boolean recoverFromWAL) throws IOException {
        System.out.println("Opening LSMTree with maxMemtableSize " + maxMemtableSize);
        WriteAheadLog wal = WriteAheadLog.open(
                directory.resolve(directory.getFileName() + WAL_DIR_SUFFIX),
                true,
                128_000,
                1_000
        );

        LSMTree lsm = new LSMTree(directory, maxMemtableSize, wal);
        lsm.closed = false;
        lsm.inRecovery = recoverFromWAL;

        lsm.loadSSTables();

        // Start background virtual threads → Similar to starting goroutines

        Thread.ofVirtual()
                .name("compaction")
                .start(() -> {
                    try {
                        lsm.backgroundCompaction();
                    } catch (Exception e) {
                        System.err.println("compaction died with: " + e.getMessage());
                    } finally {
                        System.out.println("compaction exiting, countDown");
                        lsm.backgroundThreadLatch.countDown();
                    }
                });

        Thread.ofVirtual()
                .name("memtable-flusher")
                .start(() -> {
                    System.out.println("memtable-flusher lambda started");
                    try {
                        lsm.backgroundMemtableFlushing();
                    } catch (Exception e) {
                        System.err.println("memtable-flusher died with: " + e.getMessage());
                    } finally {
                        System.out.println("memtable-flusher exiting, countDown");
                        lsm.backgroundThreadLatch.countDown();
                    }
                });

        System.out.println("memtable-flusher thread submitted");

        return lsm;
    }

    public void close() throws IOException {
        System.out.println("close() called");

        mutex.writeLock().lock();
        flushingQueueMutex.writeLock().lock();

        try {
            flushingQueue.add(memtable);
        }  finally {
            flushingQueueMutex.writeLock().unlock();
        }

        try {
            System.out.println("close() offering memtable to flushingChan, size: "
                    + flushingChan.size());

            boolean offered = flushingChan.offer(memtable, 5, TimeUnit.SECONDS);

            if (!offered) {
                System.err.println("close() could not enqueue final memtable — channel full");
            }
            memtable = new Memtable();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } finally {
            mutex.writeLock().unlock();
        }

        System.out.println("Waiting for flushingChan to drain, size: "
                + flushingChan.size());

        long drainDeadline = System.currentTimeMillis() + 30_000;
        while (!flushingChan.isEmpty() && System.currentTimeMillis() < drainDeadline) {
            try {
                Thread.sleep(50);
                System.out.println("Waiting... flushingChan.size()=" + flushingChan.size()
                        + " flushingQueue.size()=" + flushingQueue.size());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        System.out.println("flushingChan drained, setting closed=true");
        System.out.println("Marking closed as true");
        closed = true;
        try {
            boolean completed = backgroundThreadLatch.await(30, TimeUnit.SECONDS);
            System.out.println("close() latch completed: " + completed);
            if (!completed) {
                System.err.println(
                        "LSMTree.close() — virtual threads did not complete within 30s"
                );
            }
        } catch (InterruptedException e) {
            // Mirrors Go's behaviour — restore interrupt flag and continue closing
            Thread.currentThread().interrupt();
        }

        wal.close();
        flushingChan.clear();
        compactionChan.clear();

        // Close all open SSTable file handles
        for (Levels level : levels) {
            for (SSTable sst : level.getSsTables()) {
                sst.close();
            }
        }

    }

    public void put(String key, byte[] value) {
        this.mutex.writeLock().lock();
        try {
            if(!inRecovery) {
                this.wal.writeEntry(new WriteAheadLog.WALEntry(key, value, Command.PUT, System.nanoTime()));
            }

            memtable.put(key, value);

            if(memtable.sizeInBytes() > maxMemtableSize) {
                Memtable fullMemtable = memtable;
                memtable = new Memtable();
                System.out.println("Memtable: " + fullMemtable.getEntries());

                flushingQueueMutex.writeLock().lock();
                try {
                    flushingQueue.add(fullMemtable);
                } finally {
                    flushingQueueMutex.writeLock().unlock();
                }

                mutex.writeLock().unlock();         // release write lock before blocking put()

                try {
                    flushingChan.put(fullMemtable); // blocking — guaranteed to enqueue
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            System.out.println("FlusingQueue size: " + flushingQueue.size());
            System.out.println("FlusingChan size: " + flushingChan.size());
        } finally {
            if(mutex.isWriteLocked()) {
                mutex.writeLock().unlock();
            }
        }
    }

    public byte[] get(String key) {
        this.mutex.readLock().lock();
         LSMEntry value = memtable.get(key);
         if(value != null) {
             this.mutex.readLock().unlock();
             return handleValue(value);
         }
         this.mutex.readLock().unlock();

         // Check flushing queue memtables in reverse order
         flushingQueueMutex.readLock().lock();
         for(int i=flushingQueue.size() - 1; i >= 0; i--) {
             value = flushingQueue.get(i).get(key);
             if(value != null) {
                 flushingQueueMutex.readLock().unlock();
                 return handleValue(value);
             }
         }
         flushingQueueMutex.readLock().unlock();

         for(Levels level : levels) {
             level.getMutex().readLock().lock();

             for (int i= level.getSsTables().size()-1; i >= 0; i--) {
                 value = level.getSsTables().get(i).get(key).orElse(null);
                 if(value != null) {
                     level.getMutex().readLock().unlock();
                     return handleValue(value);
                 }
             }
             level.getMutex().readLock().unlock();
         }
         return null;
    }

    public void delete(String key) {
        System.out.println("Deleting key " + key);
        mutex.writeLock().lock();

        if(!inRecovery) {
            wal.writeEntry(new WriteAheadLog.WALEntry(
                    key,
                    null,
                    Command.TOMBSTONE,
                    System.nanoTime()
            ));
        }

        memtable.delete(key);

        if(memtable.sizeInBytes() > maxMemtableSize) {
            flushingQueueMutex.writeLock().lock();
            flushingQueue.add(memtable);
            flushingQueueMutex.writeLock().unlock();

            flushingChan.offer(memtable);
            memtable = new Memtable();
        }

        mutex.writeLock().unlock();
    }

    private void loadSSTables() throws IOException {
        System.out.println("Loading SSTables");
        Files.createDirectories(directory);
        loadSSTablesFromDisk();
        sortSSTablesBySequenceNumber();
        initializeCurrentSequenceNumber();
    }

    private void loadSSTablesFromDisk() throws IOException {
        try (var stream = Files.list(directory)) {
            stream.filter(path -> !Files.isDirectory(path))
                    .filter(path -> isSSTableFile(path.getFileName().toString()))
                    .forEach(path -> {
                        try {
                            SSTable sst = SSTable.open(path);
                            int level = getLevelFromSSTableFileName(path.toString());
                            levels[level].getSsTables().add(sst);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
    }

    private boolean isSSTableFile(String fileName) {
        return fileName.startsWith(SSTABLE_PREFIX);
    }

    private void sortSSTablesBySequenceNumber() {
        for (Levels level : levels) {
            level.getSsTables().sort(
                    Comparator.comparingLong(sst -> getSequenceNumber(sst.getDirectory().toString()))
            );
        }
    }

    private long getSequenceNumber(String fileName) {
        // skip: directory "/" "sstable_" level "_"
        int prefixLen = directory.toString().length() + 1 + SSTABLE_PREFIX.length() + 2;
        return Long.parseLong(fileName.substring(prefixLen));
    }

    private void initializeCurrentSequenceNumber() {
        long max = 0;
        for (Levels level : levels) {
            for(SSTable sstable : level.getSsTables()) {
                max = Math.max(max, getSequenceNumber(sstable.getDirectory().toString()));
            }
        }
        currentSSTSequence.set(max);
    }

    /**
     * Format: <directory>/sstable_<level>_<sequence>
     * eg: /data/sstable_0_42  -> level 0, sequence 42
     * @param fileName
     * @return
     */
    private int getLevelFromSSTableFileName(String fileName) {
        int prefixLen = directory.toString().length() + 1 + SSTABLE_PREFIX.length();
        String levelChar = fileName.substring(prefixLen, prefixLen + 1);
        return Integer.parseInt(levelChar);
    }

    private void backgroundCompaction() {
        try {
            while (true) {
                Integer candidate = compactionChan.poll(100, TimeUnit.MILLISECONDS);
                if (candidate != null) {
                    compactLevel(candidate);
                } else if (closed) {
                    if (compactionChan.isEmpty()) {
                        return;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void compactLevel(int level) {
        if (level >= MAX_LEVELS - 1) return;

        Levels srcLevel  = levels[level];
        Levels destLevel = levels[level + 1];

        // ── Phase 1: read lock to collect handles ─────────────────────────────
        srcLevel.getMutex().readLock().lock();
        destLevel.getMutex().readLock().lock();

        List<SSTable>         srcHandles;
        List<SSTable>         destHandles;
        List<SSTableIterable> iterators;

        try {
            if (srcLevel.getSsTables().size() < MAX_LEVELS_SSTABLES.get(level)) {
                return; // not enough SSTables to compact
            }

            var srcPair  = getSSTableHandlesAtLevel(level);
            var destPair = getSSTableHandlesAtLevel(level + 1);

            srcHandles  = srcPair.sstables();
            destHandles = destPair.sstables();

            iterators = new ArrayList<>();
            iterators.addAll(srcPair.iterators());
            iterators.addAll(destPair.iterators());

        } finally {
            // ALWAYS release read locks before acquiring write locks
            // ReentrantReadWriteLock does not support lock upgrading — deadlock otherwise
            destLevel.getMutex().readLock().unlock();
            srcLevel.getMutex().readLock().unlock();
        }

        // ── Phase 2: merge outside of any lock ────────────────────────────────
        // SSTables are immutable — safe to read without a lock
        // This is the expensive CPU/IO work — we do not want locks held here
        SSTable mergedSSTable = mergeSSTables(iterators, level + 1);

        // ── Phase 3: write lock to commit the result ──────────────────────────
        srcLevel.getMutex().writeLock().lock();
        destLevel.getMutex().writeLock().lock();
        try {
            // Revalidate — another thread may have compacted while we were merging
            // If srcHandles are no longer in srcLevel, our merge is stale — skip
            if (!srcLevel.getSsTables().containsAll(srcHandles)) {
                System.out.println("compactLevel: stale compaction detected, skipping commit");
                // Clean up the merged SSTable we just wrote
                try { Files.deleteIfExists(mergedSSTable.getDirectory()); }
                catch (IOException ignored) {}
                mergedSSTable.close();
                return;
            }

            deleteSSTablesAtLevel(level, srcHandles);
            deleteSSTablesAtLevel(level + 1, destHandles);
            addSSTableAtLevel(mergedSSTable, level + 1);

        } catch (IOException e) {
            throw new RuntimeException("compactLevel commit failed", e);
        } finally {
            // Always unlock in reverse order of acquisition
            destLevel.getMutex().writeLock().unlock();
            srcLevel.getMutex().writeLock().unlock();
        }
    }

    private SSTableHandles getSSTableHandlesAtLevel(int level) {
        List<SSTable> ssTables = new ArrayList<>(levels[level].getSsTables());
        List<SSTableIterable> iterators = ssTables.stream().map(ssTable ->
                new SSTableIterable(ssTable, ssTable.getDirectory(), null)
        ).toList();
        return new SSTableHandles(ssTables, iterators);
    }

    private void backgroundMemtableFlushing() {
        System.out.println("backgroundMemtableFlushing started on: "
                + Thread.currentThread().getName());
        try {
            while (true) {
                Memtable memtable = flushingChan.poll(100, TimeUnit.MILLISECONDS);

                if (memtable != null) {
                    System.out.println("Polled memtable: " + memtable.getEntries());
                    System.out.println("Size of flushingChan after polling: " + flushingChan.size());
                    try {
                        flushMemtable(memtable);
                    } catch (Exception e) {
                        System.err.println("flushMemtable failed: " + e.getMessage());
                        e.printStackTrace();
                    }
                } else {
                    // flushingChan is empty — check flushingQueue for orphaned memtables
                    // These are memtables that were added to flushingQueue but
                    // could not be offered to flushingChan because it was full
                    flushingQueueMutex.readLock().lock();
                    Memtable orphaned = null;
                    try {
                        if (!flushingQueue.isEmpty()) {
                            orphaned = flushingQueue.getFirst();
                        }
                    } finally {
                        flushingQueueMutex.readLock().unlock();
                    }

                    if (orphaned != null) {
                        System.out.println("Found orphaned memtable in flushingQueue, flushing directly");
                        try {
                            flushMemtable(orphaned);
                        } catch (Exception e) {
                            System.err.println("Orphaned flushMemtable failed: " + e.getMessage());
                            e.printStackTrace();
                        }
                    } else if (closed) {
                        System.out.println("closed=true and queues empty, exiting");
                        return;
                    }
                }
            }
        } catch (InterruptedException e) {
            System.out.println("backgroundMemtableFlushing interrupted");
            Thread.currentThread().interrupt();
        }
    }

    public void flushMemtable (Memtable memtable) {
        if(memtable.sizeInBytes() == 0) {
            System.out.println("Memtable is empty");

            // Still remove from flushingQueue so it does not accumulate
            flushingQueueMutex.writeLock().lock();
            try {
                if (!flushingQueue.isEmpty()) {
                    flushingQueue.removeFirst();
                }
            } finally {
                flushingQueueMutex.writeLock().unlock();
            }
            return;
        }

        long sequence = currentSSTSequence.incrementAndGet();
        Path path = getSSTableFilename(0, sequence);

        List<LSMEntry> entries = memtable.getEntries();

        System.out.println("Flushing memtable to: " + path.getFileName());
        System.out.println("Entry count: " + entries.size());
        System.out.println("First key: " + (entries.isEmpty() ? "none"
                : entries.getFirst().key()));
        System.out.println("Last key:  " + (entries.isEmpty() ? "none"
                : entries.getLast().key()));

        SSTable sst;
        try {
            System.out.println("Writing memtable to disk for sequence number " + currentSSTSequence.get());
            sst = SSTable.write(path, entries);
            System.out.println("Wrote memtable to SSTable: " + sst.getDirectory().getFileName());
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush memtable to SSTable: " + path.getFileName().toString(), e);
        }

        System.out.println("Trying to get the lock");
        levels[0].getMutex().writeLock().lock();
        System.out.println("Lock for level acquired");
        flushingQueueMutex.writeLock().lock();
        System.out.println("Lock Acquired");

        try {
            wal.createCheckPoint(new WriteAheadLog.WALEntry(
                    path.getFileName().toString(),
                    null,
                    Command.WRITE_SST,
                    System.nanoTime()
            ));

            levels[0].getSsTables().add(sst);

            if(!flushingQueue.isEmpty()) {
                System.out.println("Removing flushed memtable");
                Memtable removedMemtable = flushingQueue.removeFirst();
                System.out.println("Removed flushed memtable: " + removedMemtable.getEntries());
            }
        } catch (Exception e) {
            System.err.println("Removing flushMemtable failed: " + e.getMessage());
        }
        finally {
            System.out.println("Releasing lockkk");
            flushingQueueMutex.writeLock().unlock();
            System.out.println("Releasing level acquired");
            levels[0].getMutex().writeLock().unlock();
            System.out.println("Level Released");
        }

        // Always signals compaction after a flush — no capacity check
        // Unlike our earlier offer(), this mirrors Go's blocking send
        // Use put() to block if queue is full, matching Go's blocking channel send
        try {
            System.out.println("Signalling compactionChan");
            boolean signalled = compactionChan.offer(0, 1, TimeUnit.SECONDS);
            System.out.println("Signalled compactionChan");
            if (!signalled) {
                System.err.println("compactionChan full — compaction thread may be stuck");
            }
        } catch (InterruptedException e) {
            System.err.println("Exception in flushMemtable");
            Thread.currentThread().interrupt();
        }
    }

    private SSTable mergeSSTables(List<SSTableIterable> iterators, int targetLevel) {
        List<LSMEntry> mergedEntries = mergeIterators(iterators);
        long sequence = currentSSTSequence.incrementAndGet();
        Path path = getSSTableFilename(targetLevel, sequence);
        try {
            return SSTable.write(path, mergedEntries);
        } catch (IOException e) {
            throw new RuntimeException("Could write to SSTable due to : ", e);
        }
    }

    private List<LSMEntry> mergeIterators(List<SSTableIterable> iterators) {
        record HeapEntry(LSMEntry entry, SSTableIterable iterable) {}

        var pq = new PriorityQueue<HeapEntry>(
                Comparator.comparing(h -> h.entry.key())
        );

        var seen = new TreeMap<String, HeapEntry>();

        // FIXME: Could be a bug here
        for (SSTableIterable ssTableIterable : iterators) {
            if (ssTableIterable == null || !ssTableIterable.hasNext()) {
                continue;
            }
            SSTableIterable next = ssTableIterable.next();
            pq.offer(new HeapEntry(next.getEntry(), ssTableIterable));
        }

        // ── Phase 1: drain heap → build seen map
        while(!pq.isEmpty()) {
            HeapEntry current = pq.poll();
            String key = current.entry.key();

            HeapEntry previousValue = seen.get(key);

            if(previousValue != null) {
                if(current.entry.timestamp() > previousValue.entry.timestamp()) {
                    // Current entry is newer — replace existing
                    seen.put(key, current);
                }
            } else {
                // First time seeing this key — insert unconditionally
                seen.put(key, current);
            }

            // Add the next element from the same list to the heap
            if(current.iterable.hasNext()) {
                SSTableIterable next = current.iterable.next();
                pq.offer(new HeapEntry(next.getEntry(), current.iterable));
            }
        }

        // ── Phase 2: walk seen map → filter tombstones → build results ────────
        var results =  new ArrayList<LSMEntry>();

        for(HeapEntry winner : seen.values()) {
            switch (winner.entry) {
                case LSMEntry.Put p -> results.add(p);
                case LSMEntry.Tombstone tombstone -> {} // drop tombstone
            }
        }

        return results;
    }

    /** Mirrors Go's checkAndTriggerCompaction(). */
    private boolean checkAndTriggerCompaction() {
        boolean readyToExit = true;
        for (int idx = 0; idx < MAX_LEVELS; idx++) {
            Levels level = levels[idx];
            level.getMutex().readLock().lock();
            try {
                if (level.getSsTables().size() > MAX_LEVELS_SSTABLES.get(idx)) {
                    compactionChan.offer(idx);
                    readyToExit = false;
                }
            } finally {
                level.getMutex().readLock().unlock();
            }
        }
        return readyToExit;
    }

    private Path getSSTableFilename(int targetLevel, long sequence) {
        return directory.resolve(
                "%s%d_%d".formatted(SSTABLE_PREFIX, targetLevel, sequence)
        );
    }

    private void deleteSSTablesAtLevel(int levelIdx, List<SSTable> toRemove) {
        levels[levelIdx].getSsTables().removeAll(toRemove);
        toRemove.forEach(sst -> {
            try { Files.deleteIfExists(sst.getDirectory()); }
            catch (IOException ignored) {}
        });
    }

    private void addSSTableAtLevel(SSTable mergedSSTable, int levelIdx) {
        levels[levelIdx].getSsTables().add(mergedSSTable);
        if (levelIdx < MAX_LEVELS - 1 &&
                levels[levelIdx].getSsTables().size() > MAX_LEVELS_SSTABLES.get(levelIdx)) {
            compactionChan.offer(levelIdx);
        }
    }

    private byte[] handleValue(LSMEntry entry) {
        if (entry instanceof LSMEntry.Tombstone) {
            return null;
        }
        return ((LSMEntry.Put) entry).value();
    }
}