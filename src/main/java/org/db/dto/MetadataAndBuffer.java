package org.db.dto;

import org.db.core.BloomFilter;

import java.util.ArrayList;

public record MetadataAndBuffer(BloomFilter bloomFilter, ArrayList<IndexEntry> indexEntries, byte[] entriesBuffer) {
}
