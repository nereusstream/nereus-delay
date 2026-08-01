package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** Closed, text-free public error projection shared by all V1 outcomes. */
public final class StableErrorV1 {
    private final FailureStageV1 stage;
    private final StableCode code;
    private final RetryabilityV1 retryability;
    private final Long retryAtEpochMs;
    private final CommandQueuedReceiptV1.PreparedCommandRef command;
    private final NativePreparedRefV1 nativePrepared;
    private final Integer diagnosticCode;

    public StableErrorV1(final FailureStageV1 stage, final StableCode code,
                         final RetryabilityV1 retryability, final Long retryAtEpochMs,
                         final CommandQueuedReceiptV1.PreparedCommandRef command,
                         final NativePreparedRefV1 nativePrepared, final Integer diagnosticCode) {
        this.stage = Objects.requireNonNull(stage, "stage");
        this.code = Objects.requireNonNull(code, "code");
        this.retryability = Objects.requireNonNull(retryability, "retryability");
        if (retryability != RetryabilityV1.forCode(code)) {
            throw new IllegalArgumentException("retryability does not match stable code registry");
        }
        if ((retryability == RetryabilityV1.RETRY_EXACT_BYTES_AFTER_RETRY_AT) != (retryAtEpochMs != null)
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

    public static StableErrorV1 of(final FailureStageV1 stage, final StableCode code, final Long retryAtEpochMs,
                                   final CommandQueuedReceiptV1.PreparedCommandRef command,
                                   final NativePreparedRefV1 nativePrepared, final Integer diagnosticCode) {
        return new StableErrorV1(stage, code, RetryabilityV1.forCode(code), retryAtEpochMs, command, nativePrepared,
                diagnosticCode);
    }

    public static StableErrorV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "StableErrorV1");
        if (fields.size() < 3 || fields.size() > 7) {
            throw new IllegalArgumentException("StableErrorV1 fields are incomplete or unknown");
        }
        final FailureStageV1 stage = FailureStageV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1));
        final StableCode code = StableCode.fromWire(QueryCodecSupport.uint32(fields.get(1), 2));
        final RetryabilityV1 retryability = RetryabilityV1.fromWire(QueryCodecSupport.uint(fields.get(2), 3));
        int index = 3;
        Long retryAt = null;
        CommandQueuedReceiptV1.PreparedCommandRef command = null;
        NativePreparedRefV1 nativePrepared = null;
        Integer diagnostic = null;
        if (index < fields.size() && fields.get(index).number() == 4) {
            retryAt = QueryCodecSupport.uint(fields.get(index), 4);
            index++;
        }
        if (index < fields.size() && fields.get(index).number() == 5) {
            command = CommandQueuedReceiptV1.PreparedCommandRef.decode(QueryCodecSupport.nested(fields.get(index), 5));
            index++;
        }
        if (index < fields.size() && fields.get(index).number() == 6) {
            nativePrepared = NativePreparedRefV1.decode(QueryCodecSupport.nested(fields.get(index), 6));
            index++;
        }
        if (index < fields.size() && fields.get(index).number() == 7) {
            diagnostic = QueryCodecSupport.uint32(fields.get(index), 7);
            index++;
        }
        if (index != fields.size()) {
            throw new IllegalArgumentException("StableErrorV1 field order is invalid");
        }
        final StableErrorV1 result = new StableErrorV1(stage, code, retryability, retryAt, command, nativePrepared,
                diagnostic);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "StableErrorV1");
        return result;
    }

    public FailureStageV1 stage() {
        return stage;
    }

    public StableCode code() {
        return code;
    }

    public RetryabilityV1 retryability() {
        return retryability;
    }

    public Long retryAtEpochMs() {
        return retryAtEpochMs;
    }

    public CommandQueuedReceiptV1.PreparedCommandRef command() {
        return command;
    }

    public NativePreparedRefV1 nativePrepared() {
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
        if (!(other instanceof StableErrorV1 that)) {
            return false;
        }
        return stage == that.stage && code == that.code && retryability == that.retryability
                && Objects.equals(retryAtEpochMs, that.retryAtEpochMs) && Objects.equals(command, that.command)
                && Objects.equals(nativePrepared, that.nativePrepared)
                && Objects.equals(diagnosticCode, that.diagnosticCode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stage, code, retryability, retryAtEpochMs, command, nativePrepared, diagnosticCode);
    }
}
