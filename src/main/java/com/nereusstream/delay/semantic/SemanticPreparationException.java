package com.nereusstream.delay.semantic;

import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import java.util.Objects;

/** Stable, synchronous, zero-I/O preparation failure exposed by the Semantic Core. */
public final class SemanticPreparationException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    private final transient StableError error;

    public SemanticPreparationException(final StableError error, final Throwable cause) {
        super(
                "semantic preparation failed: "
                        + Objects.requireNonNull(error, "error").code(),
                cause);
        if (error.stage() != FailureStage.PREPARATION) {
            throw new IllegalArgumentException("SemanticPreparationException requires PREPARATION stage");
        }
        this.error = error;
    }

    public static SemanticPreparationException of(final StableCode code, final Throwable cause) {
        return new SemanticPreparationException(
                StableError.of(FailureStage.PREPARATION, code, null, null, null, null), cause);
    }

    public StableError error() {
        return error;
    }
}
