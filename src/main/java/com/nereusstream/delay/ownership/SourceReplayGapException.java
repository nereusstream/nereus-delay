package com.nereusstream.delay.ownership;

/** Deterministic proof that a source replay position is not the successor. */
final class SourceReplayGapException extends IllegalStateException {
    private static final long serialVersionUID = 1L;

    SourceReplayGapException(final String message) {
        super(message);
    }
}
