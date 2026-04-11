package org.db.core;

import java.nio.file.Path;

public class SSTableCollection {
    private final SSTable ssTable;
    private final Path path;
    private final LSMEntry entry;

    public SSTableCollection(SSTable ssTable, Path path, LSMEntry entry) {
        this.ssTable = ssTable;
        this.path = path;
        this.entry = entry;
    }

    public SSTable getSsTable() {
        return ssTable;
    }

    public Path getPath() {
        return path;
    }

    public LSMEntry getEntry() {
        return entry;
    }
}
