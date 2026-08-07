package io.nereusstream.delay.ownership;

import java.util.List;
import java.util.Objects;

/** Results from one bounded source replay turn. */
public record SourceReplayTurn<T>(List<T> results, boolean exhausted) {
    public SourceReplayTurn {
        results = List.copyOf(Objects.requireNonNull(results, "results"));
    }

    /** Returns whether the source cursor still has a record for a later turn. */
    public boolean hasMore() {
        return !exhausted;
    }
}
