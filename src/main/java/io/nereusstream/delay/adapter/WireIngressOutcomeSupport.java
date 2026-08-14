package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.DefinitelyNotQueuedV1;
import io.nereusstream.delay.protocol.EnqueueOutcomeMessageV1;
import io.nereusstream.delay.protocol.EnqueueUncertainV1;
import io.nereusstream.delay.protocol.FailureStageV1;
import io.nereusstream.delay.protocol.NonPersistenceProofKindV1;
import io.nereusstream.delay.protocol.NonPersistenceProofV1;
import io.nereusstream.delay.protocol.PreparedCommand;
import io.nereusstream.delay.protocol.RetryabilityV1;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.StableErrorV1;

import java.util.Objects;

/** Internal common projection rules for request-level pinned ingress adapters. */
public final class WireIngressOutcomeSupport {
    private WireIngressOutcomeSupport() {
    }

    public static EnqueueOutcomeMessageV1 localDefinite(final PreparedCommand command, final StableCode code) {
        final CommandQueuedReceiptV1.PreparedCommandRef ref = CommandQueuedReceiptV1.PreparedCommandRef.from(command);
        final NonPersistenceProofV1 proof = NonPersistenceProofV1.create(
                NonPersistenceProofKindV1.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, ref.frameSha256(), null, null, null);
        return EnqueueOutcomeMessageV1.definitelyNotQueued(new DefinitelyNotQueuedV1(ref, proof,
                StableErrorV1.of(FailureStageV1.ENQUEUE, code, null, ref, null, null)));
    }

    public static EnqueueOutcomeMessageV1 uncertain(final PreparedCommand command, final byte[] physicalAttemptId,
                                             final StableCode code, final Integer diagnosticCode) {
        final CommandQueuedReceiptV1.PreparedCommandRef ref = CommandQueuedReceiptV1.PreparedCommandRef.from(command);
        return EnqueueOutcomeMessageV1.uncertain(new EnqueueUncertainV1(ref, requireAttempt(physicalAttemptId),
                StableErrorV1.of(FailureStageV1.ENQUEUE, exactRetryCode(code), null, ref, null, diagnosticCode)));
    }

    public static EnqueueOutcomeMessageV1 brokerDefinite(final PreparedCommand command, final byte[] physicalAttemptId,
                                                  final StableCode code, final NonPersistenceProofKindV1 proofKind,
                                                  final BrokerResourceIdentityV1 resource, final byte[] requestBytes,
                                                  final byte[] responseBytes) {
        Objects.requireNonNull(resource, "resource");
        final CommandQueuedReceiptV1.PreparedCommandRef ref = CommandQueuedReceiptV1.PreparedCommandRef.from(command);
        final byte[] attempt = requireAttempt(physicalAttemptId);
        if (responseBytes == null) {
            return uncertain(command, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN, code.wireValue());
        }
        final NonPersistenceProofV1 proof = NonPersistenceProofV1.create(proofKind, attempt, ref.frameSha256(),
                resource, Bytes.sha256(Objects.requireNonNull(requestBytes, "requestBytes")),
                Bytes.sha256(responseBytes));
        return EnqueueOutcomeMessageV1.definitelyNotQueued(new DefinitelyNotQueuedV1(ref, proof,
                StableErrorV1.of(FailureStageV1.ENQUEUE, code, null, ref, null, null)));
    }

    public static StableCode stableCode(final int wireValue, final StableCode fallback) {
        try {
            return StableCode.fromWire(wireValue);
        } catch (IllegalArgumentException ignored) {
            return fallback;
        }
    }

    /**
     * Managed Command outcomes must not expose the native-only error family.
     * A transport implementation may share result plumbing with AUTO_FAST,
     * but the public branch is still selected by the prepared submission type.
     */
    public static StableCode managedCode(final StableCode code) {
        return switch (Objects.requireNonNull(code, "code")) {
            case NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED -> StableCode.BROKER_DEFINITIVE_NOT_PERSISTED;
            case NATIVE_ENQUEUE_RESULT_UNCERTAIN -> StableCode.ENQUEUE_RESULT_UNCERTAIN;
            default -> code;
        };
    }

    /**
     * A definitive managed ingress result is only proof-bearing for the two
     * guard/rejection codes that the shared transport SPI is allowed to emit.
     * A disposition without one of these codes is a malformed adapter result,
     * not evidence that the Broker did not persist the request.
     */
    public static StableCode definitiveManagedCode(final int wireValue) {
        try {
            return switch (StableCode.fromWire(wireValue)) {
                case BROKER_DEFINITIVE_NOT_PERSISTED, NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED
                        -> StableCode.BROKER_DEFINITIVE_NOT_PERSISTED;
                default -> null;
            };
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    public static byte[] requireAttempt(final byte[] physicalAttemptId) {
        Bytes.requireLength(physicalAttemptId, NonPersistenceProofV1.ATTEMPT_ID_LENGTH,
                "physicalEnqueueAttemptId");
        for (byte value : physicalAttemptId) {
            if (value != 0) {
                return Bytes.copy(physicalAttemptId);
            }
        }
        throw new IllegalArgumentException("physicalEnqueueAttemptId must be non-zero");
    }

    private static StableCode exactRetryCode(final StableCode code) {
        final StableCode managed = managedCode(code);
        return RetryabilityV1.forCode(managed) == RetryabilityV1.RETRY_EXACT_BYTES
                ? managed : StableCode.ENQUEUE_RESULT_UNCERTAIN;
    }
}
