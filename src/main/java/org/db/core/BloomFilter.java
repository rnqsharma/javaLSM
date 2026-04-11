package org.db.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

public final class BloomFilter {

    private final boolean[] bitset;
    private final long size;

    public BloomFilter(long size) {
        this.size = size;
        this.bitset = new boolean[(int) (size)];
    }

    // Deserialization constructor
    private BloomFilter(boolean[] bitset, long size) {
        this.bitset = bitset;
        this.size = size;
    }

    public void add(String key) {
        add(key.getBytes(StandardCharsets.UTF_8));
    }

    public boolean mightContain(String key) {
        return mightContain(key.getBytes(StandardCharsets.UTF_8));
    }

    private void add(byte[] item) {
        long hash1 = murmur64(item, 0);
        long hash2 = murmur64(item, 1);
        long hash3 = murmur64(item, 2);

        bitset[(int) Math.floorMod(hash1, size)] = true;
        bitset[(int) Math.floorMod(hash2, size)] = true;
        bitset[(int) Math.floorMod(hash3, size)] = true;
    }

    private static long murmur64(byte[] data, long seed) {
        final long C1 = 0xff51afd7ed558ccdL;
        final long C2 = 0xc4ceb9fe1a85ec53L;

        long h = seed ^ data.length;

        int i = 0;
        while (i + 8 <= data.length) {
            long k = readLongLE(data, i);
            k *= C1;
            k  = Long.rotateLeft(k, 31);
            k *= C2;
            h ^= k;
            h  = Long.rotateLeft(h, 27);
            h  = h * 5 + 0x52dce729;
            i += 8;
        }

        long remaining = 0;
        for (int j = data.length - 1; j >= i; j--) {
            remaining = (remaining << 8) | (data[j] & 0xFFL);
        }
        if (data.length > i) {
            h ^= remaining * C1;
        }

        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        h *= 0xc4ceb9fe1a85ec53L;
        h ^= h >>> 33;

        return h;
    }

    private static long readLongLE(byte[] data, int offset) {
        return ((long)(data[offset    ] & 0xFF))
                | ((long)(data[offset + 1] & 0xFF)) << 8
                | ((long)(data[offset + 2] & 0xFF)) << 16
                | ((long)(data[offset + 3] & 0xFF)) << 24
                | ((long)(data[offset + 4] & 0xFF)) << 32
                | ((long)(data[offset + 5] & 0xFF)) << 40
                | ((long)(data[offset + 6] & 0xFF)) << 48
                | ((long)(data[offset + 7] & 0xFF)) << 56;
    }

    private boolean mightContain(byte[] item) {
        long hash1 = murmur64(item, 0);
        long hash2 = murmur64(item, 1);
        long hash3 = murmur64(item, 2);

        return bitset[(int) Math.floorMod(hash1, size)]
                && bitset[(int) Math.floorMod(hash2, size)]
                && bitset[(int) Math.floorMod(hash3, size)];
    }

    public long size() { return size; }

    public byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + bitset.length)
                .order(ByteOrder.LITTLE_ENDIAN);
        buffer.putLong(size);

        buffer.putLong(size);
        for (boolean b : bitset) {
            buffer.put(b ? (byte) 1 : (byte) 0);
        }

        return buffer.array();
    }

    public static BloomFilter deSerialise(byte[] data) {
        java.nio.ByteBuffer buf = java.nio.ByteBuffer
                .wrap(data)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN);

        long size = buf.getLong();
        boolean[] bitset = new boolean[(int) size];
        for (int i = 0; i < size; i++) {
            bitset[i] = buf.get() == 1;
        }

        return new BloomFilter(bitset, size);
    }

    @Override
    public String toString() {
        int setBits = 0;
        for (boolean b : bitset) if (b) setBits++;
        return "BloomFilter{size=%d, setBits=%d}".formatted(size, setBits);
    }

}