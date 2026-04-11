package org.db.core;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class SSTableIterator implements Iterator<SSTableIterable> {

    private final List<SSTableIterable> items;
    private int cursor = 0;

    public SSTableIterator(List<SSTableIterable> items) {
        this.items = items;
    }

    @Override
    public boolean hasNext() {
        return cursor < items.size();
    }

    @Override
    public SSTableIterable next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return items.get(cursor++);
    }
}