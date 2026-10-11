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
    private final Runnable protection;
    private boolean loaded;
    private boolean exhausted;
    private T next;

    private SourceReplayCursor(final Iterator<? extends T> iterator, final Runnable protection) {
        this.iterator = Objects.requireNonNull(iterator, "iterator");
        this.protection = protection;
    }

    public static <T> SourceReplayCursor<T> of(final Iterator<? extends T> iterator) {
        return new SourceReplayCursor<>(iterator, null);
    }

    /** Keeps the caller-owned native Source guard live even while a look-ahead entry is cached. */
    public static <T> SourceReplayCursor<T> guarded(Iterator<? extends T> iterator, Runnable protection) {
        return new SourceReplayCursor<>(iterator, Objects.requireNonNull(protection, "protection"));
    }

    public void requireGuarded() {
        if (protection == null) {
            throw new IllegalArgumentException("Target recovery requires a guarded Source cursor");
        }
        requireCurrent();
    }

    /** Rechecks held native identity/connection and retention protections without advancing the cursor. */
    public void requireCurrent() {
        if (protection != null) { protection.run(); }
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
        requireCurrent();
        if (loaded || exhausted) {
            return;
        }
        if (iterator.hasNext()) {
            next = Objects.requireNonNull(iterator.next(), "source replay entry");
            loaded = true;
        } else {
            exhausted = true;
        }
        requireCurrent();
    }
}
