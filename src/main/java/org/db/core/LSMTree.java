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
//    private volatile long currentSSTSequence;

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

    private final ExecutorService executor;

    private volatile boolean closed;

    private LSMTree(Path directory, long maxMemtableSize, WriteAheadLog wal) {
        this.directory = directory;
        this.maxMemtableSize = maxMemtableSize;
        this.wal = wal;
        this.memtable = new Memtable();

        // Initializing levels
        this.levels = new Levels[MAX_LEVELS];
        Arrays.setAll(levels, i -> new Levels());

        // Similar to goroutines
        this.executor = Executors.newVirtualThreadPerTaskExecutor();

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
        lsm.executor.submit(lsm::backgroundCompaction);
        lsm.executor.submit(lsm::backgroundMemtableFlushing);

        return lsm;
    }

    public void close() throws IOException {
        mutex.writeLock().lock();
        flushingQueueMutex.writeLock().lock();

        try {
            flushingQueue.add(memtable);
        }  finally {
            flushingQueueMutex.writeLock().unlock();
        }

        try {
            flushingChan.put(memtable);

            memtable = new Memtable();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            mutex.writeLock().unlock();
        }

        closed = true;
        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
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
        System.out.println("Putting key " + key + " with value " + value);
        this.mutex.writeLock().lock();
        try {
            if(!inRecovery) {
                this.wal.writeEntry(new WriteAheadLog.WALEntry(key, value, Command.PUT, System.nanoTime()));
            }

            memtable.put(key, value);

            if(memtable.sizeInBytes() > maxMemtableSize) {
                flushingQueueMutex.writeLock().lock();
                flushingQueue.add(memtable);
                flushingQueueMutex.writeLock().unlock();

                flushingChan.offer(memtable);
                memtable = new Memtable();
            }
        } finally {
            mutex.writeLock().unlock();
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
            while(!closed) {
                Integer candidate = compactionChan.poll(50, TimeUnit.MILLISECONDS);
                if(candidate != null) {
                    compactLevel(candidate);
                } else if (closed) {
                    closed = checkAndTriggerCompaction();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void compactLevel(int level) {
        if(level >= MAX_LEVELS - 1) {
            return;
        }

        Levels srcLevel = levels[level];
        srcLevel.getMutex().readLock().lock();

        List<SSTable>           handles;
        List<SSTableIterable>   iterators;
        try {
            if(srcLevel.getSsTables().size() < MAX_LEVELS_SSTABLES.get(level)) {
                return;     // another thread may have already compacted
            }
            var pair = getSSTableHandlesAtLevel(level);
            handles = pair.sstables;
            iterators = pair.iterators;
        } finally {
            // We can release the lock on the level now while we process the SSTables. This
            // is safe because these SSTables are immutable, and can only be deleted by
            // this function (which is single-threaded).
            srcLevel.getMutex().readLock().unlock();
        }

        SSTable mergedSSTable = mergeSSTables(iterators, level + 1);

        srcLevel.getMutex().writeLock().lock();
        levels[level + 1].getMutex().writeLock().lock();

        try {
            deleteSSTablesAtLevel(level, handles);
            addSSTableAtLevel(mergedSSTable, level + 1);
        } finally {
            levels[level + 1].getMutex().writeLock().unlock();
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
        try {
            while(true) {
                Memtable memtable = flushingChan.poll(100, TimeUnit.MILLISECONDS);
                if(memtable != null) {
                    flushMemtable(memtable);
                } else if (closed) {
                    if(flushingChan.isEmpty()) {
                        return;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void flushMemtable (Memtable memtable) {
        if(memtable.sizeInBytes() == 0) {
            return;
        }

        currentSSTSequence.incrementAndGet();

        Path path = getSSTableFilename(0);

        SSTable sst;
        try {
            sst = SSTable.write(path, memtable.getEntries());
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush memtable to SSTable: " + path.getFileName().toString(), e);
        }

        levels[0].getMutex().writeLock().lock();
        flushingQueueMutex.writeLock().lock();

        try {
            wal.createCheckPoint(new WriteAheadLog.WALEntry(
                    path.getFileName().toString(),
                    null,
                    Command.WRITE_SST,
                    System.nanoTime()
            ));

            levels[0].getSsTables().add(sst);

            if(!flushingQueue.isEmpty()) {
                flushingQueue.removeFirst();
            }
        } finally {
            flushingQueueMutex.writeLock().unlock();
            levels[0].getMutex().writeLock().unlock();
        }

        // Always signals compaction after a flush — no capacity check
        // Unlike our earlier offer(), this mirrors Go's blocking send
        // Use put() to block if queue is full, matching Go's blocking channel send
        try {
            compactionChan.put(0);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private SSTable mergeSSTables(List<SSTableIterable> iterators, int targetLevel) {
        List<LSMEntry> mergedEntries = mergeIterators(iterators);
        Path path = getSSTableFilename(targetLevel);
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

    private Path getSSTableFilename(int targetLevel) {
        return directory.resolve(
                "%s%d_%d".formatted(SSTABLE_PREFIX, targetLevel, currentSSTSequence.get())
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
        compactionChan.offer(levelIdx);
    }

    private byte[] handleValue(LSMEntry entry) {
        if (entry instanceof LSMEntry.Tombstone) {
            return null;
        }
        return ((LSMEntry.Put) entry).value();
    }
}