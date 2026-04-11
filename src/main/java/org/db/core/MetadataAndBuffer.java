package org.db.core;

import java.util.ArrayList;

public record MetadataAndBuffer(BloomFilter bloomFilter, ArrayList<IndexEntry> indexEntries, byte[] entriesBuffer) {
}
