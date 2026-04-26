package org.db.core.wal;

import org.db.dto.Command;

public record WALEntry(
        String key, byte[] value, Command command, long timestamp
    ) {}