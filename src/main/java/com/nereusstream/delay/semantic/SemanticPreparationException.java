package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.FailureStageV1;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableErrorV1;
import java.util.Objects;

/** Stable, synchronous, zero-I/O preparation failure exposed by the Semantic Core. */
public final class SemanticPreparationException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    private final transient StableErrorV1 error;

    public SemanticPreparationException(final StableErrorV1 error, final Throwable cause) {
        super(
                "semantic preparation failed: "
                        + Objects.requireNonNull(error, "error").code(),
                cause);
        if (error.stage() != FailureStageV1.PREPARATION) {
            throw new IllegalArgumentException("SemanticPreparationException requires PREPARATION stage");
        }
        this.error = error;
    }

    public static SemanticPreparationException of(final StableCode code, final Throwable cause) {
        return new SemanticPreparationException(
                StableErrorV1.of(FailureStageV1.PREPARATION, code, null, null, null, null), cause);
    }

    public StableErrorV1 error() {
        return error;
    }
}
