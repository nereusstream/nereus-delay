package com.nereusstream.delay.submission;

import com.nereusstream.delay.protocol.StableCode;

/** Local, pre-ownership failure while resolving an exact prepared submission. */
public final class SubmissionPlanException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    private final StableCode code;

    public SubmissionPlanException(final StableCode code, final Throwable cause) {
        super(code.name(), cause);
        this.code = java.util.Objects.requireNonNull(code, "code");
    }

    public StableCode code() {
        return code;
    }
}
