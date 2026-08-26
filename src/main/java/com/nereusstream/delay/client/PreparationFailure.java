package com.nereusstream.delay.client;

import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import java.util.Objects;

/**
 * Typed synchronous preparation failure. The stable error is the public
 * contract; the exception message is deliberately diagnostic only.
 */
public final class PreparationFailure extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    private final transient StableError error;

    public PreparationFailure(final StableError error) {
        super("preparation failed: " + Objects.requireNonNull(error, "error").code());
        if (error.stage() != FailureStage.PREPARATION) {
            throw new IllegalArgumentException("PreparationFailure requires PREPARATION stage");
        }
        this.error = error;
    }

    public PreparationFailure(final StableError error, final Throwable cause) {
        super("preparation failed: " + Objects.requireNonNull(error, "error").code(), cause);
        if (error.stage() != FailureStage.PREPARATION) {
            throw new IllegalArgumentException("PreparationFailure requires PREPARATION stage");
        }
        this.error = error;
    }

    public static PreparationFailure of(final StableCode code) {
        return new PreparationFailure(
                StableError.of(FailureStage.PREPARATION, Objects.requireNonNull(code, "code"), null, null, null, null));
    }

    public static PreparationFailure of(final StableCode code, final Throwable cause) {
        return new PreparationFailure(
                StableError.of(FailureStage.PREPARATION, Objects.requireNonNull(code, "code"), null, null, null, null),
                cause);
    }

    public StableError error() {
        return error;
    }
}
