package org.db.core;

import java.nio.file.Path;

// TODO: Implement WAL
public class WriteAheadLog {

    public WriteAheadLog() {}

    public static WriteAheadLog open(Path directory, boolean enableFsync,
                                     long maxFileSize, int maxSegments) {
        return new WriteAheadLog();
    }
}
