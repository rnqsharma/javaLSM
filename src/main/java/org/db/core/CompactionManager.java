package org.db.core;

import org.db.core.sst.SSTable;
import org.db.dto.Levels;
import org.db.dto.LSMEntry;
import org.db.core.sst.SSTableIterable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class CompactionManager {

    private final Levels[] levels;
    private final int maxLevels;
    private final Map<Integer, Integer> maxLevelSSTables;
    private final AtomicLong currentSSTSequence;
    private final Path directory;
    private final String sstablePrefix;
    private final BlockingQueue<Integer> compactionChan;

    public CompactionManager(Levels[] levels,
                             int maxLevels,
                             Map<Integer, Integer> maxLevelSSTables,
                             AtomicLong currentSSTSequence,
                             Path directory,
                             String sstablePrefix,
                             BlockingQueue<Integer> compactionChan) {
        this.levels = levels;
        this.maxLevels = maxLevels;
        this.maxLevelSSTables = maxLevelSSTables;
        this.currentSSTSequence = currentSSTSequence;
        this.directory = directory;
        this.sstablePrefix = sstablePrefix;
        this.compactionChan = compactionChan;
    }

    /**
     * Compacts level `levelIdx` into level `levelIdx + 1`.
     * Three-phase: collect under read lock → merge without lock → commit under write lock.
     */
    public void compact(int levelIdx) {
        if (levelIdx >= maxLevels - 1) return;

        Levels srcLevel = levels[levelIdx];
        Levels destLevel = levels[levelIdx + 1];

        // ── Phase 1: collect handles under read lock ──────────────────────────
        CompactionHandles handles = collectHandles(levelIdx, srcLevel, destLevel);
        if (handles == null) return; // not enough SSTables

        // ── Phase 2: merge without any lock ───────────────────────────────────
        SSTable merged = null;
        try {
            merged = merge(handles.iterators(), levelIdx + 1);
        } catch (Exception e) {
            System.err.println("merge failed at level " + levelIdx + ": " + e.getMessage());
            throw e;
        } finally {
            closeIterators(handles.iterators());
        }

        if (merged == null) return;

        // ── Phase 3: commit under write lock ──────────────────────────────────
        commit(levelIdx, srcLevel, destLevel, handles, merged);
    }

    private CompactionHandles collectHandles(int levelIdx,
                                             Levels srcLevel,
                                             Levels destLevel) {
        srcLevel.getMutex().readLock().lock();
        destLevel.getMutex().readLock().lock();
        try {
            if (srcLevel.getSsTables().size() < maxLevelSSTables.get(levelIdx)) {
                return null;
            }

            List<SSTable> srcSSTables = new ArrayList<>(srcLevel.getSsTables());
            List<SSTable> destSSTables = new ArrayList<>(destLevel.getSsTables());

            List<SSTableIterable> iterators = new ArrayList<>();
            iterators.addAll(openIterators(srcSSTables));
            iterators.addAll(openIterators(destSSTables));

            return new CompactionHandles(srcSSTables, destSSTables, iterators);

        } finally {
            destLevel.getMutex().readLock().unlock();
            srcLevel.getMutex().readLock().unlock();
        }
    }

    private List<SSTableIterable> openIterators(List<SSTable> ssTables) {
        return ssTables.stream()
                .map(sst -> {
                    try {
                        return new SSTableIterable(sst, sst.getDirectory());
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Failed to open iterator for: " + sst.getDirectory(), e
                        );
                    }
                })
                .toList();
    }

    private SSTable merge(List<SSTableIterable> iterators, int targetLevel) {
        List<LSMEntry> merged = mergeIterators(iterators);
        long sequence = currentSSTSequence.incrementAndGet();
        Path path = getSSTablePath(targetLevel, sequence);

        try {
            return SSTable.write(path, merged);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write merged SSTable", e);
        }
    }

    private List<LSMEntry> mergeIterators(List<SSTableIterable> iterators) {
        record HeapEntry(LSMEntry entry, SSTableIterable iterable) {
        }

        var pq = new PriorityQueue<HeapEntry>(
                Comparator.comparing(h -> h.entry.key())
        );
        var seen = new TreeMap<String, HeapEntry>();

        // Seed heap with first entry from each iterator
        for (SSTableIterable it : iterators) {
            if (it == null || !it.hasNext()) continue;
            SSTableIterable next = it.next();
            pq.offer(new HeapEntry(next.getEntry(), it));
        }

        // Phase 1: drain heap → build seen map (newest timestamp wins)
        while (!pq.isEmpty()) {
            HeapEntry current = pq.poll();
            String key = current.entry.key();
            HeapEntry previous = seen.get(key);

            if (previous == null ||
                    current.entry.timestamp() > previous.entry.timestamp()) {
                seen.put(key, current);
            }

            if (current.iterable.hasNext()) {
                SSTableIterable next = current.iterable.next();
                pq.offer(new HeapEntry(next.getEntry(), current.iterable));
            }
        }

        // Phase 2: filter tombstones → build results
        var results = new ArrayList<LSMEntry>();
        for (HeapEntry winner : seen.values()) {
            switch (winner.entry) {
                case LSMEntry.Put p -> results.add(p);
                case LSMEntry.Tombstone ignored -> {
                } // drop tombstones
            }
        }
        return results;
    }

    private void commit(int levelIdx,
                        Levels srcLevel,
                        Levels destLevel,
                        CompactionHandles handles,
                        SSTable merged) {
        srcLevel.getMutex().writeLock().lock();
        destLevel.getMutex().writeLock().lock();
        try {
            // Revalidate — another thread may have compacted while we were merging
            if (!srcLevel.getSsTables().containsAll(handles.srcSSTables())) {
                System.out.println("Stale compaction detected — aborting");
                cleanupMerged(merged);
                return;
            }

            // Close file handles before deleting files
            closeSSTableHandles(handles.srcSSTables());
            closeSSTableHandles(handles.destSSTables());

            // Remove from in-memory level lists and delete from disk
            deleteSSTables(levelIdx, handles.srcSSTables());
            deleteSSTables(levelIdx + 1, handles.destSSTables());

            // Add merged SSTable to destination level
            levels[levelIdx + 1].getSsTables().add(merged);

            // Signal further compaction if destination level now overflows
            if (levelIdx + 1 < maxLevels - 1 &&
                    levels[levelIdx + 1].getSsTables().size()
                            > maxLevelSSTables.get(levelIdx + 1)) {
                compactionChan.offer(levelIdx + 1);
            }

        } finally {
            destLevel.getMutex().writeLock().unlock();
            srcLevel.getMutex().writeLock().unlock();
        }
    }

    private void deleteSSTables(int levelIdx, List<SSTable> toRemove) {
        levels[levelIdx].getSsTables().removeAll(toRemove);
        toRemove.forEach(sst -> {
            try {
                Files.deleteIfExists(sst.getDirectory());
            } catch (IOException e) {
                System.err.println("Failed to delete: "
                        + sst.getDirectory().getFileName());
            }
        });
    }

    private void closeSSTableHandles(List<SSTable> ssTables) {
        ssTables.forEach(sst -> {
            try {
                sst.close();
            } catch (IOException ignored) {
            }
        });
    }

    private void closeIterators(List<SSTableIterable> iterators) {
        iterators.forEach(it -> {
            try {
                it.close();
            } catch (IOException ignored) {
            }
        });
    }

    private void cleanupMerged(SSTable merged) {
        try {
            Files.deleteIfExists(merged.getDirectory());
            merged.close();
        } catch (IOException ignored) {
        }
    }

    private Path getSSTablePath(int level, long sequence) {
        return directory.resolve(
                "%s%d_%d".formatted(sstablePrefix, level, sequence)
        );
    }

    /**
     * Holds the snapshot of SSTable handles and iterators for one compaction run
     */
    private record CompactionHandles(
            List<SSTable> srcSSTables,
            List<SSTable> destSSTables,
            List<SSTableIterable> iterators
    ) {
    }
}