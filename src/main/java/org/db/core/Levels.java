package org.db.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class Levels {
    private static List<SSTable> ssTables = new ArrayList<>();
    private final ReentrantReadWriteLock mutex = new ReentrantReadWriteLock();

    public List<SSTable> getSsTables() {
        return ssTables;
    }

    public void setSsTables(List<SSTable> ssTables) {
        Levels.ssTables = ssTables;
    }

    public ReentrantReadWriteLock getMutex() {
        return mutex;
    }
}
