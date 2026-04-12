package org.db.utility;

public sealed interface LSMEntry permits LSMEntry.Put, LSMEntry.Tombstone {

    String key();
    long timestamp();

    record Put(
            String key,
            byte[] value,
            long timestamp
    ) implements LSMEntry {}

    /**
     * Tombstone can't contain value as it's just a mark for deletion
     * @param key
     * @param timestamp
     */
    record Tombstone(
            String key,
            long timestamp
    ) implements LSMEntry {}
}
