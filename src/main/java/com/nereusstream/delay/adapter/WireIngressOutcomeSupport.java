package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.DefinitelyNotQueued;
import com.nereusstream.delay.protocol.EnqueueOutcomeMessage;
import com.nereusstream.delay.protocol.EnqueueUncertain;
import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.NativeEnqueueUncertain;
import com.nereusstream.delay.protocol.NativePreparedDelivery;
import com.nereusstream.delay.protocol.NonPersistenceProof;
import com.nereusstream.delay.protocol.NonPersistenceProofKind;
import com.nereusstream.delay.protocol.PreparedCommand;
import com.nereusstream.delay.protocol.PreparedSubmission;
import com.nereusstream.delay.protocol.Retryability;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import com.nereusstream.delay.transport.PhysicalEnqueueAttemptId;
import java.util.Objects;

/** Internal common projection rules for request-level pinned ingress adapters. */
public final class WireIngressOutcomeSupport {
    private WireIngressOutcomeSupport() {}

    public static EnqueueOutcomeMessage localDefinite(final PreparedCommand command, final StableCode code) {
        final CanonicalCommandQueuedReceipt.PreparedCommandRef ref =
                CanonicalCommandQueuedReceipt.PreparedCommandRef.from(command);
        final NonPersistenceProof proof = NonPersistenceProof.create(
                NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, ref.frameSha256(), null, null, null);
        return EnqueueOutcomeMessage.definitelyNotQueued(
                new DefinitelyNotQueued(ref, proof, StableError.of(FailureStage.ENQUEUE, code, null, ref, null, null)));
    }

    public static EnqueueOutcomeMessage uncertain(
            final PreparedCommand command,
            final byte[] physicalAttemptId,
            final StableCode code,
            final Integer diagnosticCode) {
        final CanonicalCommandQueuedReceipt.PreparedCommandRef ref =
                CanonicalCommandQueuedReceipt.PreparedCommandRef.from(command);
        return EnqueueOutcomeMessage.uncertain(new EnqueueUncertain(
                ref,
                requireAttempt(physicalAttemptId),
                StableError.of(FailureStage.ENQUEUE, exactRetryCode(code), null, ref, null, diagnosticCode)));
    }

    /**
     * Converts a post-transport local evidence failure into the same exact
     * retry branch as an unobservable transport result. The prepared branch
     * and physical attempt are never replaced while projecting uncertainty.
     */
    public static SubmissionOutcomeMessage uncertain(
            final PreparedSubmission submission, final PhysicalEnqueueAttemptId physicalAttemptId) {
        Objects.requireNonNull(submission, "submission");
        Objects.requireNonNull(physicalAttemptId, "physicalAttemptId");
        if (submission.isManaged()) {
            final PreparedCommand command =
                    com.nereusstream.delay.protocol.CommandCodec.decodeManagedFrame(submission.managedFrame());
            return SubmissionOutcomeMessage.managed(
                    uncertain(command, physicalAttemptId.bytes(), StableCode.ENQUEUE_RESULT_UNCERTAIN, null));
        }
        final NativePreparedDelivery prepared = submission.nativePrepared();
        final StableError error = StableError.of(
                FailureStage.ENQUEUE,
                StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN,
                null,
                null,
                prepared.preparedRef(),
                null);
        return SubmissionOutcomeMessage.nativeUncertain(
                new NativeEnqueueUncertain(prepared.preparedRef(), physicalAttemptId.bytes(), error));
    }

    public static EnqueueOutcomeMessage brokerDefinite(
            final PreparedCommand command,
            final byte[] physicalAttemptId,
            final StableCode code,
            final NonPersistenceProofKind proofKind,
            final BrokerResourceIdentity resource,
            final byte[] requestBytes,
            final byte[] responseBytes) {
        Objects.requireNonNull(resource, "resource");
        final CanonicalCommandQueuedReceipt.PreparedCommandRef ref =
                CanonicalCommandQueuedReceipt.PreparedCommandRef.from(command);
        final byte[] attempt = requireAttempt(physicalAttemptId);
        if (responseBytes == null) {
            return uncertain(command, attempt, StableCode.ENQUEUE_RESULT_UNCERTAIN, code.wireValue());
        }
        final NonPersistenceProof proof = NonPersistenceProof.create(
                proofKind,
                attempt,
                ref.frameSha256(),
                resource,
                Bytes.sha256(Objects.requireNonNull(requestBytes, "requestBytes")),
                Bytes.sha256(responseBytes));
        return EnqueueOutcomeMessage.definitelyNotQueued(
                new DefinitelyNotQueued(ref, proof, StableError.of(FailureStage.ENQUEUE, code, null, ref, null, null)));
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
                case BROKER_DEFINITIVE_NOT_PERSISTED, NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED ->
                    StableCode.BROKER_DEFINITIVE_NOT_PERSISTED;
                default -> null;
            };
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    public static byte[] requireAttempt(final byte[] physicalAttemptId) {
        Bytes.requireLength(physicalAttemptId, NonPersistenceProof.ATTEMPT_ID_LENGTH, "physicalEnqueueAttemptId");
        for (byte value : physicalAttemptId) {
            if (value != 0) {
                return Bytes.copy(physicalAttemptId);
            }
        }
        throw new IllegalArgumentException("physicalEnqueueAttemptId must be non-zero");
    }

    private static StableCode exactRetryCode(final StableCode code) {
        final StableCode managed = managedCode(code);
        return Retryability.forCode(managed) == Retryability.RETRY_EXACT_BYTES
                ? managed
                : StableCode.ENQUEUE_RESULT_UNCERTAIN;
    }
}
