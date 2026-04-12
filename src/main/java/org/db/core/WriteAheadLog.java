package org.db.core;

import java.nio.file.Path;

// TODO: Implement WAL
public class WriteAheadLog {

    record WALEntry(
            String key, byte[] value, Command command, long timestamp
    ) {}
    public WriteAheadLog() {}

    public static WriteAheadLog open(Path directory, boolean enableFsync,
                                     long maxFileSize, int maxSegments) {
        return new WriteAheadLog();
    }

    public void writeEntry(WALEntry entry) {

    }
}
