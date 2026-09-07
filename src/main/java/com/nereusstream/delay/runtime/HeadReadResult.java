package com.nereusstream.delay.runtime;

import com.nereusstream.delay.store.BoundedReadBudget;
import java.util.Objects;

/** One validated prefix result. INCOMPLETE carries no candidate or empty-prefix authority. */
record HeadReadResult<T>(Kind kind, T candidate, BoundedReadBudget.Exhaustion reason, long keysRead) {
    enum Kind {
        FOUND,
        EMPTY_CONFIRMED,
        INCOMPLETE
    }

    HeadReadResult {
        Objects.requireNonNull(kind, "kind");
        if (keysRead < 0
                || (kind == Kind.FOUND) != (candidate != null)
                || (kind == Kind.INCOMPLETE) != (reason != null)) {
            throw new IllegalArgumentException("invalid head read result");
        }
    }
}
