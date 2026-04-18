// ── 4. SSTableLoader — Single Responsibility: loads SSTables from disk ─────────
package org.db.core.sst;

import org.db.dto.Levels;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Single Responsibility: owns the SSTable discovery and loading pipeline.
 * Extracted from LSMTree.loadSSTables(), loadSSTablesFromDisk(),
 * sortSSTablesBySequenceNumber(), initializeCurrentSequenceNumber().
 */
public final class SSTableLoader {

    private final Path directory;
    private final String     sstablePrefix;
    private final Levels[]   levels;
    private final AtomicLong currentSSTSequence;

    public SSTableLoader(Path directory,
                          String sstablePrefix,
                          Levels[] levels,
                          AtomicLong currentSSTSequence) {
        this.directory          = directory;
        this.sstablePrefix      = sstablePrefix;
        this.levels             = levels;
        this.currentSSTSequence = currentSSTSequence;
    }

    public void load() throws IOException {
        Files.createDirectories(directory);
        loadFromDisk();
        sortBySequenceNumber();
        initializeSequenceNumber();
    }

    private void loadFromDisk() throws IOException {
        try (var stream = Files.list(directory)) {
            stream.filter(path -> !Files.isDirectory(path))
                  .filter(path -> path.getFileName().toString().startsWith(sstablePrefix))
                  .forEach(path -> {
                      try {
                          SSTable sst   = SSTable.open(path);
                          int     level = getLevelFromFileName(path.toString());
                          levels[level].getSsTables().add(sst);
                      } catch (IOException e) {
                          throw new RuntimeException(
                              "Failed to open SSTable: " + path, e
                          );
                      }
                  });
        }
    }

    private void sortBySequenceNumber() {
        for (Levels level : levels) {
            level.getSsTables().sort(
                Comparator.comparingLong(
                    sst -> getSequenceNumber(sst.getDirectory().toString())
                )
            );
        }
    }

    private void initializeSequenceNumber() {
        long max = 0;
        for (Levels level : levels) {
            for (SSTable sst : level.getSsTables()) {
                max = Math.max(max, getSequenceNumber(sst.getDirectory().toString()));
            }
        }
        currentSSTSequence.set(max);
    }

    private int getLevelFromFileName(String fileName) {
        int    prefixLen  = directory.toString().length() + 1 + sstablePrefix.length();
        String levelChar  = fileName.substring(prefixLen, prefixLen + 1);
        return Integer.parseInt(levelChar);
    }

    private long getSequenceNumber(String fileName) {
        int prefixLen = directory.toString().length() + 1 + sstablePrefix.length() + 2;
        return Long.parseLong(fileName.substring(prefixLen));
    }
}