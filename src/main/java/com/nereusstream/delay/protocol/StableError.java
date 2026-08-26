package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Closed, text-free public error projection shared by all outcomes. */
public final class StableError {
    private final FailureStage stage;
    private final StableCode code;
    private final Retryability retryability;
    private final Long retryAtEpochMs;
    private final CanonicalCommandQueuedReceipt.PreparedCommandRef command;
    private final NativePreparedRef nativePrepared;
    private final Integer diagnosticCode;

    public StableError(
            final FailureStage stage,
            final StableCode code,
            final Retryability retryability,
            final Long retryAtEpochMs,
            final CanonicalCommandQueuedReceipt.PreparedCommandRef command,
            final NativePreparedRef nativePrepared,
            final Integer diagnosticCode) {
        this.stage = Objects.requireNonNull(stage, "stage");
        this.code = Objects.requireNonNull(code, "code");
        if (code == StableCode.OK) {
            throw new IllegalArgumentException("StableError cannot carry the successful OK code");
        }
        this.retryability = Objects.requireNonNull(retryability, "retryability");
        if (retryability != Retryability.forCode(code)) {
            throw new IllegalArgumentException("retryability does not match stable code registry");
        }
        if ((retryability == Retryability.RETRY_EXACT_BYTES_AFTER_RETRY_AT) != (retryAtEpochMs != null)
                || (retryAtEpochMs != null && retryAtEpochMs < 0)) {
            throw new IllegalArgumentException("retry_at presence does not match retryability");
        }
        if (command != null && nativePrepared != null) {
            throw new IllegalArgumentException("StableError cannot carry managed and native prepared refs together");
        }
        if (diagnosticCode != null && (diagnosticCode < 0 || diagnosticCode > 0xffff)) {
            throw new IllegalArgumentException("diagnostic code is outside the bounded public range");
        }
        this.retryAtEpochMs = retryAtEpochMs;
        this.command = command;
        this.nativePrepared = nativePrepared;
        this.diagnosticCode = diagnosticCode;
    }

    public static StableError of(
            final FailureStage stage,
            final StableCode code,
            final Long retryAtEpochMs,
            final CanonicalCommandQueuedReceipt.PreparedCommandRef command,
            final NativePreparedRef nativePrepared,
            final Integer diagnosticCode) {
        return new StableError(
                stage, code, Retryability.forCode(code), retryAtEpochMs, command, nativePrepared, diagnosticCode);
    }

    public static StableError decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "StableError");
        if (fields.size() < 3 || fields.size() > 7) {
            throw new IllegalArgumentException("StableError fields are incomplete or unknown");
        }
        final FailureStage stage = FailureStage.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final StableCode code = StableCode.fromWire(QueryCodecSupport.uint32(fields.get(1), 2));
        final Retryability retryability = Retryability.fromWire(QueryCodecSupport.uint(fields.get(2), 3));
        int index = 3;
        Long retryAt = null;
        CanonicalCommandQueuedReceipt.PreparedCommandRef command = null;
        NativePreparedRef nativePrepared = null;
        Integer diagnostic = null;
        if (index < fields.size() && fields.get(index).number() == 4) {
            retryAt = QueryCodecSupport.uint(fields.get(index), 4);
            index++;
        }
        if (index < fields.size() && fields.get(index).number() == 5) {
            command = CanonicalCommandQueuedReceipt.PreparedCommandRef.decode(
                    QueryCodecSupport.nested(fields.get(index), 5));
            index++;
        }
        if (index < fields.size() && fields.get(index).number() == 6) {
            nativePrepared = NativePreparedRef.decode(QueryCodecSupport.nested(fields.get(index), 6));
            index++;
        }
        if (index < fields.size() && fields.get(index).number() == 7) {
            diagnostic = QueryCodecSupport.uint32(fields.get(index), 7);
            index++;
        }
        if (index != fields.size()) {
            throw new IllegalArgumentException("StableError field order is invalid");
        }
        final StableError result =
                new StableError(stage, code, retryability, retryAt, command, nativePrepared, diagnostic);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "StableError");
        return result;
    }

    public FailureStage stage() {
        return stage;
    }

    public StableCode code() {
        return code;
    }

    public Retryability retryability() {
        return retryability;
    }

    public Long retryAtEpochMs() {
        return retryAtEpochMs;
    }

    public CanonicalCommandQueuedReceipt.PreparedCommandRef command() {
        return command;
    }

    public NativePreparedRef nativePrepared() {
        return nativePrepared;
    }

    public Integer diagnosticCode() {
        return diagnosticCode;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, stage.wireValue());
            CanonicalProtobuf.uint32(output, 2, code.wireValue());
            CanonicalProtobuf.uint32(output, 3, retryability.wireValue());
            if (retryAtEpochMs != null) {
                CanonicalProtobuf.int64(output, 4, retryAtEpochMs);
            }
            if (command != null) {
                CanonicalProtobuf.bytes(output, 5, command.canonicalBytes());
            }
            if (nativePrepared != null) {
                CanonicalProtobuf.bytes(output, 6, nativePrepared.canonicalBytes());
            }
            if (diagnosticCode != null) {
                CanonicalProtobuf.uint32(output, 7, diagnosticCode);
            }
        });
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof StableError that)) {
            return false;
        }
        return stage == that.stage
                && code == that.code
                && retryability == that.retryability
                && Objects.equals(retryAtEpochMs, that.retryAtEpochMs)
                && Objects.equals(command, that.command)
                && Objects.equals(nativePrepared, that.nativePrepared)
                && Objects.equals(diagnosticCode, that.diagnosticCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stage, code, retryability, retryAtEpochMs, command, nativePrepared, diagnosticCode);
    }
}
