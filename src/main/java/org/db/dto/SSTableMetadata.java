package org.db.dto;

import org.db.core.BloomFilter;

import java.util.List;

public record SSTableMetadata(
            BloomFilter bloomFilter,
            List<IndexEntry> index,
            long             dataOffset
    ) {}