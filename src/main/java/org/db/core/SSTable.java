package org.db.core;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public final class SSTable {

    private final Path path;

    public SSTable(Path path) {
        this.path = path;
    }

    public Path getPath() {
        return path;
    }

    public static SSTable open(Path directory) throws IOException {
        // TODO: open and memory-map the file, load block index
        return new SSTable(directory);
    }

    public static SSTable write(Path directory, List<KVPair> entries) throws IOException {
        // TODO: serialise entries, write block index, fsync
        return new SSTable(directory);
    }
}
