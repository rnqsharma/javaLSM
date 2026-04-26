package org.db.core;

import org.db.dto.Command;
import org.db.dto.LSMEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.db.dto.Command.PUT;
import static org.db.dto.Command.TOMBSTONE;

public class Memtable {

    private final TreeMap<String, LSMEntry> data;
    private long size;

    public Memtable() {
        this.data = new TreeMap<>();
        this.size = 0;
    }

    public void put(String key, byte[] value) {
        int sizeChange = value.length;
        LSMEntry existing = data.get(key);
        if (existing != null) {
            if(existing instanceof LSMEntry.Put(var k, var v, var t)) {
                size -= v.length;
            }
        } else {
            sizeChange += key.length();
        }

        data.put(key, getLSMEntry(key, value, PUT));
        size += sizeChange;
    }

    public void delete(String key) {
        LSMEntry existing = data.get(key);
        if (existing != null) {
            if(existing instanceof LSMEntry.Put(var k, var v, var t)) {
                size -= v.length;
            }
        } else {
            size += key.length();
        }
        data.put(key, getLSMEntry(key, null, TOMBSTONE));
    }

    public LSMEntry get(String key) {
        return data.get(key);
    }

    public long sizeInBytes() {
        return size;
    }

    public void clear () {
        data.clear();
        size = 0;
    }

    public int len() {
        return data.size();
    }

    public List<LSMEntry> getEntries() {
        return new ArrayList<>(data.values());
    }

    private LSMEntry getLSMEntry(String key, byte[] value, Command command) {
        long timestamp = System.nanoTime();
        return switch (command) {
            case PUT -> new LSMEntry.Put(key, value, timestamp);
            case TOMBSTONE -> new LSMEntry.Tombstone(key, timestamp);
            default -> throw new IllegalStateException("Unexpected value: " + command);
        };
    }
}
