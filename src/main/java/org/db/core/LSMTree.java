package org.db.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LSMTree {

    private final static int MAX_LEVELS = 6;
    private final static String WAL_DIR_SUFFIX = "_wal";
    private final static String SSTABLE_PREFIX = "sstable_";

    private volatile Memtable memtable;
    private final  ReentrantReadWriteLock mutex = new ReentrantReadWriteLock();
    private final long maxMemtableSize;
    private final Path directory;
    private final WriteAheadLog wal;
    private volatile boolean inRecovery;
    private final Levels[] levels;
    private volatile long currentSSTSequence;

    private final List<Memtable> flushingQueue = new ArrayList<>();
    private final BlockingQueue<Memtable> flushingChan = new LinkedBlockingQueue<>(100);
    private final ReentrantReadWriteLock flushingQueueMutex = new ReentrantReadWriteLock();

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
    }

    public static LSMTree open(Path directory, long maxMemtableSize, boolean recoverFromWAL) throws IOException {
        WriteAheadLog wal = WriteAheadLog.open(
                directory.resolve(directory.getFileName() + WAL_DIR_SUFFIX),
                true,
                128_000,
                1_000
        );

        LSMTree lsm = new LSMTree(directory, maxMemtableSize, wal);
        lsm.inRecovery = recoverFromWAL;

        lsm.loadSSTables();

        // Start background virtual threads → Similar to starting goroutines
        lsm.executor.submit(lsm::backgroundCompaction);
        lsm.executor.submit(lsm::backgroundMemtableFlushing);

        return lsm;
    }

    private void loadSSTables() throws IOException {
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
                    Comparator.comparingLong(sst -> getSequenceNumber(sst.getPath().toString()))
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
                max = Math.max(max, getSequenceNumber(sstable.getPath().toString()));
            }
        }
        currentSSTSequence = max;
    }

    private int getLevelFromSSTableFileName(String fileName) {

        return 1;
    }

    private void backgroundCompaction() {

    }

    private void backgroundMemtableFlushing() {

    }
}
