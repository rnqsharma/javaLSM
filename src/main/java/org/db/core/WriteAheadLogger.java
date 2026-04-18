package org.db.core;

import java.io.IOException;

public interface WriteAheadLogger {
    void writeEntry(WriteAheadLog.WALEntry entry);
    void createCheckPoint(WriteAheadLog.WALEntry entry);
    void close() throws IOException;
}