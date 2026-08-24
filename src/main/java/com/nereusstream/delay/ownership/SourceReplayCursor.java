package com.nereusstream.delay.ownership;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * Caller-owned source cursor with one bounded look-ahead item.
 *
 * <p>The look-ahead lets a replay turn enforce the canonical-byte cap before
 * consuming the next record. A caller keeps the same cursor across turns so
 * a yielded record is never lost or replayed by re-iterating an {@code Iterable}.</p>
 */
public final class SourceReplayCursor<T> {
    private final Iterator<? extends T> iterator;
    private boolean loaded;
    private boolean exhausted;
    private T next;

    private SourceReplayCursor(final Iterator<? extends T> iterator) {
        this.iterator = Objects.requireNonNull(iterator, "iterator");
    }

    public static <T> SourceReplayCursor<T> of(final Iterator<? extends T> iterator) {
        return new SourceReplayCursor<>(iterator);
    }

    public boolean hasNext() {
        load();
        return loaded;
    }

    public T peek() {
        load();
        if (!loaded) {
            throw new NoSuchElementException("source replay cursor is exhausted");
        }
        return next;
    }

    public T next() {
        final T result = peek();
        next = null;
        loaded = false;
        return result;
    }

    private void load() {
        if (loaded || exhausted) {
            return;
        }
        if (iterator.hasNext()) {
            next = Objects.requireNonNull(iterator.next(), "source replay entry");
            loaded = true;
        } else {
            exhausted = true;
        }
    }
}
