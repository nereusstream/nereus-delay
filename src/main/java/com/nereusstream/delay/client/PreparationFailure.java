package com.nereusstream.delay.client;

import com.nereusstream.delay.protocol.FailureStageV1;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableErrorV1;
import java.util.Objects;

/**
 * Typed synchronous preparation failure. The stable error is the public
 * contract; the exception message is deliberately diagnostic only.
 */
public final class PreparationFailure extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    private final transient StableErrorV1 error;

    public PreparationFailure(final StableErrorV1 error) {
        super("preparation failed: " + Objects.requireNonNull(error, "error").code());
        if (error.stage() != FailureStageV1.PREPARATION) {
            throw new IllegalArgumentException("PreparationFailure requires PREPARATION stage");
        }
        this.error = error;
    }

    public PreparationFailure(final StableErrorV1 error, final Throwable cause) {
        super("preparation failed: " + Objects.requireNonNull(error, "error").code(), cause);
        if (error.stage() != FailureStageV1.PREPARATION) {
            throw new IllegalArgumentException("PreparationFailure requires PREPARATION stage");
        }
        this.error = error;
    }

    public static PreparationFailure of(final StableCode code) {
        return new PreparationFailure(StableErrorV1.of(
                FailureStageV1.PREPARATION, Objects.requireNonNull(code, "code"), null, null, null, null));
    }

    public static PreparationFailure of(final StableCode code, final Throwable cause) {
        return new PreparationFailure(
                StableErrorV1.of(
                        FailureStageV1.PREPARATION, Objects.requireNonNull(code, "code"), null, null, null, null),
                cause);
    }

    public StableErrorV1 error() {
        return error;
    }
}
