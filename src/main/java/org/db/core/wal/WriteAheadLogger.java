package org.db.core.wal;

import java.io.IOException;
import java.util.List;

public interface WriteAheadLogger {
    void writeEntry(WALEntry entry);
    void createCheckPoint(WALEntry entry);
    List<WALEntry> readAll(boolean readFromCheckpoint) throws IOException;

    List<WALEntry> readAllFromOffset(int offset,
                                     boolean readFromCheckpoint) throws IOException;

    void close() throws IOException;
}