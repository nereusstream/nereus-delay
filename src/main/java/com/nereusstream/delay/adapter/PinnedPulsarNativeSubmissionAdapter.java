package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.NativeCapabilitySnapshot;
import com.nereusstream.delay.protocol.NativeDefinitelyNotQueued;
import com.nereusstream.delay.protocol.NativeDeliveryReceipt;
import com.nereusstream.delay.protocol.NativeEnqueueUncertain;
import com.nereusstream.delay.protocol.NativePreparedDelivery;
import com.nereusstream.delay.protocol.NativePreparedRef;
import com.nereusstream.delay.protocol.NonPersistenceProof;
import com.nereusstream.delay.protocol.NonPersistenceProofKind;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.Retryability;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.SubmissionOutcomeMessage;
import java.security.PublicKey;
import java.time.Clock;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Pulsar AUTO_FAST submission boundary. All checks in this adapter happen
 * before the transport is allowed to acquire Producer ownership.
 */
public final class PinnedPulsarNativeSubmissionAdapter implements AutoCloseable {
    private final PulsarTargetResource resource;
    private final PublicKey issuerKey;
    private final Clock clock;
    private final PulsarNativeSendTransport transport;
    private final CredentialFingerprintProvider credentialFingerprintProvider;
    private final CloseGuard closeGuard = new CloseGuard();

    public PinnedPulsarNativeSubmissionAdapter(
            final PulsarTargetResource resource,
            final PublicKey issuerKey,
            final Clock clock,
            final PulsarNativeSendTransport transport) {
        this(resource, issuerKey, clock, transport, null);
    }

    /**
     * Creates a native adapter with an optional credential-binding resolver.
     * When configured, the resolver is checked before Producer ownership and
     * its result must equal the fingerprint bound into the signed snapshot.
     */
    public PinnedPulsarNativeSubmissionAdapter(
            final PulsarTargetResource resource,
            final PublicKey issuerKey,
            final Clock clock,
            final PulsarNativeSendTransport transport,
            final CredentialFingerprintProvider credentialFingerprintProvider) {
        this.resource = Objects.requireNonNull(resource, "resource");
        this.issuerKey = Objects.requireNonNull(issuerKey, "issuerKey");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.transport = Objects.requireNonNull(transport, "transport");
        this.credentialFingerprintProvider = credentialFingerprintProvider;
    }

    public PinnedPulsarNativeSubmissionAdapter(
            final PulsarTargetResource resource, final PublicKey issuerKey, final PulsarNativeSendTransport transport) {
        this(resource, issuerKey, Clock.systemUTC(), transport);
    }

    public PinnedPulsarNativeSubmissionAdapter(
            final PulsarTargetResource resource,
            final PublicKey issuerKey,
            final PulsarNativeSendTransport transport,
            final CredentialFingerprintProvider credentialFingerprintProvider) {
        this(resource, issuerKey, Clock.systemUTC(), transport, credentialFingerprintProvider);
    }

    /**
     * Submits one exact prepared native delivery. A transport exception or
     * an untrusted result is possible persistence and therefore never becomes
     * a definitive rejection.
     */
    public CompletionStage<SubmissionOutcomeMessage> submit(
            final NativePreparedDelivery prepared, final byte[] physicalEnqueueAttemptId) {
        Objects.requireNonNull(prepared, "prepared");
        return closeGuard.invokeIfOpen(
                () -> submitOpen(prepared, physicalEnqueueAttemptId),
                () -> completed(localDefinite(prepared, StableCode.CLIENT_CLOSED)));
    }

    private CompletionStage<SubmissionOutcomeMessage> submitOpen(
            final NativePreparedDelivery prepared, final byte[] physicalEnqueueAttemptId) {
        if (!matchesPinnedResource(prepared)) {
            return completed(localDefinite(prepared, StableCode.PREPARED_SUBMISSION_MISMATCH));
        }
        final boolean signatureValid;
        try {
            signatureValid = prepared.capabilitySnapshot().verifySignature(issuerKey);
        } catch (RuntimeException exception) {
            return completed(localDefinite(prepared, StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE));
        }
        if (!signatureValid) {
            return completed(localDefinite(prepared, StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE));
        }
        final long nowEpochMs;
        try {
            nowEpochMs = clock.millis();
        } catch (RuntimeException unavailable) {
            return completed(localDefinite(prepared, StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE));
        }
        if (nowEpochMs < 0) {
            return completed(localDefinite(prepared, StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE));
        }
        if (nowEpochMs >= prepared.capabilityExpiryEpochMs()) {
            return completed(localDefinite(prepared, StableCode.NATIVE_PREPARED_SUBMISSION_EXPIRED));
        }
        if (credentialFingerprintProvider != null) {
            final byte[] resolvedFingerprint;
            try {
                resolvedFingerprint = credentialFingerprintProvider.resolve(prepared);
                Bytes.requireLength(
                        resolvedFingerprint,
                        NativeCapabilitySnapshot.HASH_LENGTH,
                        "resolvedCredentialFingerprintDigest");
            } catch (RuntimeException unavailable) {
                return completed(localDefinite(prepared, StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE));
            }
            if (!Bytes.constantTimeEquals(
                    resolvedFingerprint, prepared.capabilitySnapshot().resolvedCredentialFingerprintDigest())) {
                return completed(localDefinite(prepared, StableCode.CREDENTIAL_BINDING_DRIFT));
            }
        }
        final byte[] attempt;
        try {
            attempt = requireAttempt(physicalEnqueueAttemptId);
        } catch (RuntimeException exception) {
            return completed(localDefinite(prepared, StableCode.INVALID_PREPARED_COMMAND));
        }

        final PulsarNativeSendRequest request;
        try {
            request = PulsarNativeSendRequest.from(resource, prepared);
        } catch (RuntimeException exception) {
            return completed(localDefinite(prepared, StableCode.AUTO_FAST_PREREQUISITE_UNAVAILABLE));
        }
        final CompletionStage<PulsarSendResult> result;
        try {
            result = transport.send(request);
        } catch (RuntimeException exception) {
            return completed(uncertain(prepared, attempt, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, null));
        }
        if (result == null) {
            return completed(uncertain(prepared, attempt, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, null));
        }
        try {
            final CompletionStage<SubmissionOutcomeMessage> handled = result.handle((value, error) -> {
                if (error != null) {
                    return uncertain(prepared, attempt, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, null);
                }
                try {
                    return project(prepared, request, attempt, value);
                } catch (RuntimeException ignored) {
                    // A malformed adapter result is not evidence of non-persistence.
                    return uncertain(
                            prepared,
                            attempt,
                            StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN,
                            StableCode.INTEGRITY_ERROR.wireValue());
                }
            });
            return handled == null
                    ? completed(uncertain(prepared, attempt, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, null))
                    : handled;
        } catch (RuntimeException registrationFailure) {
            // A broken CompletionStage implementation is not evidence that
            // the Broker rejected a request after Producer ownership.
            return completed(uncertain(prepared, attempt, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, null));
        }
    }

    @Override
    public void close() {
        closeGuard.close(transport::close);
    }

    private SubmissionOutcomeMessage project(
            final NativePreparedDelivery prepared,
            final PulsarNativeSendRequest request,
            final byte[] attempt,
            final PulsarSendResult result) {
        if (result == null) {
            return uncertain(prepared, attempt, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, null);
        }
        return switch (result.disposition()) {
            case PERSISTED -> persisted(prepared, attempt, result);
            case DEFINITIVELY_NOT_PERSISTED -> definite(prepared, request, attempt, result);
            case UNKNOWN ->
                uncertain(prepared, attempt, exactRetryCode(result.stableCode()), diagnosticFor(result.stableCode()));
        };
    }

    private SubmissionOutcomeMessage persisted(
            final NativePreparedDelivery prepared, final byte[] attempt, final PulsarSendResult result) {
        if (!matchesPinnedResult(result)) {
            return uncertain(
                    prepared,
                    attempt,
                    StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN,
                    StableCode.RESOURCE_INCARNATION_MISMATCH.wireValue());
        }
        if (result.responseEvidenceBytes() == null) {
            return uncertain(prepared, attempt, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, null);
        }
        final CanonicalCommandQueuedReceipt.PulsarQueuedAck ack = new CanonicalCommandQueuedReceipt.PulsarQueuedAck(
                result.authenticatedClusterId(),
                result.resourceIncarnation(),
                result.physicalTopic(),
                result.physicalTopicCreationTimestamp(),
                result.partition(),
                result.ledgerId(),
                result.entryId(),
                result.batchIndex(),
                result.batchSize(),
                result.brokerEntryTimestampEpochMs(),
                Bytes.sha256(result.responseEvidenceBytes()));
        final NativeDeliveryReceipt receipt = NativeDeliveryReceipt.create(prepared.preparedRef(), ack, attempt);
        return SubmissionOutcomeMessage.nativeReceipt(receipt);
    }

    private SubmissionOutcomeMessage definite(
            final NativePreparedDelivery prepared,
            final PulsarNativeSendRequest request,
            final byte[] attempt,
            final PulsarSendResult result) {
        if (!isNativeDefinitiveCode(result.stableCode())) {
            return uncertain(prepared, attempt, StableCode.INTEGRITY_ERROR, result.stableCode());
        }
        if (result.requestEvidenceBytes() == null || result.responseEvidenceBytes() == null) {
            return uncertain(prepared, attempt, StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN, null);
        }
        final NonPersistenceProof proof = NonPersistenceProof.create(
                NonPersistenceProofKind.PULSAR_GUARD_REJECTION,
                attempt,
                prepared.submissionHash(),
                BrokerResourceIdentity.pulsar(pulsarIdentity(prepared)),
                Bytes.sha256(result.requestEvidenceBytes()),
                Bytes.sha256(result.responseEvidenceBytes()));
        final NativePreparedRef ref = prepared.preparedRef();
        final StableError error = StableError.of(
                FailureStage.ENQUEUE, StableCode.NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED, null, null, ref, null);
        return SubmissionOutcomeMessage.nativeDefinitelyNotQueued(new NativeDefinitelyNotQueued(ref, proof, error));
    }

    private static boolean isNativeDefinitiveCode(final int wireValue) {
        try {
            return switch (StableCode.fromWire(wireValue)) {
                case BROKER_DEFINITIVE_NOT_PERSISTED, NATIVE_GUARD_DEFINITIVE_NOT_PERSISTED -> true;
                default -> false;
            };
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    private SubmissionOutcomeMessage localDefinite(final NativePreparedDelivery prepared, final StableCode code) {
        final NativePreparedRef ref = prepared.preparedRef();
        final NonPersistenceProof proof = NonPersistenceProof.create(
                NonPersistenceProofKind.LOCAL_BEFORE_PRODUCER_OWNERSHIP, null, ref.submissionHash(), null, null, null);
        final StableError error = StableError.of(FailureStage.ENQUEUE, code, null, null, ref, null);
        return SubmissionOutcomeMessage.nativeDefinitelyNotQueued(new NativeDefinitelyNotQueued(ref, proof, error));
    }

    private SubmissionOutcomeMessage uncertain(
            final NativePreparedDelivery prepared,
            final byte[] attempt,
            final StableCode code,
            final Integer diagnosticCode) {
        final NativePreparedRef ref = prepared.preparedRef();
        final StableCode retryCode = exactRetryCode(code.wireValue());
        final StableError error = StableError.of(FailureStage.ENQUEUE, retryCode, null, null, ref, diagnosticCode);
        return SubmissionOutcomeMessage.nativeUncertain(new NativeEnqueueUncertain(ref, attempt, error));
    }

    private boolean matchesPinnedResource(final NativePreparedDelivery prepared) {
        final PulsarBrokerResourceIdentity target = prepared.target();
        return resource.authenticatedClusterId().equals(target.authenticatedClusterId())
                && Arrays.equals(resource.resourceIncarnation(), target.resourceIncarnation())
                && resource.physicalTopic().equals(target.physicalTopic())
                && resource.physicalTopicCreationTimestamp() == target.physicalTopicCreationTimestamp()
                && resource.partition() == prepared.physicalPartition();
    }

    private boolean matchesPinnedResult(final PulsarSendResult result) {
        return resource.authenticatedClusterId().equals(result.authenticatedClusterId())
                && Arrays.equals(resource.resourceIncarnation(), result.resourceIncarnation())
                && resource.physicalTopic().equals(result.physicalTopic())
                && resource.physicalTopicCreationTimestamp() == result.physicalTopicCreationTimestamp()
                && resource.partition() == result.partition();
    }

    private static PulsarBrokerResourceIdentity pulsarIdentity(final NativePreparedDelivery prepared) {
        return prepared.target();
    }

    private static StableCode exactRetryCode(final int wireValue) {
        try {
            final StableCode candidate = StableCode.fromWire(wireValue);
            return Retryability.forCode(candidate) == Retryability.RETRY_EXACT_BYTES
                    ? candidate
                    : StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN;
        } catch (IllegalArgumentException exception) {
            return StableCode.NATIVE_ENQUEUE_RESULT_UNCERTAIN;
        }
    }

    private static Integer diagnosticFor(final int wireValue) {
        try {
            final StableCode candidate = StableCode.fromWire(wireValue);
            return candidate == StableCode.RESOURCE_INCARNATION_MISMATCH ? candidate.wireValue() : null;
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private static byte[] requireAttempt(final byte[] value) {
        Bytes.requireLength(value, NonPersistenceProof.ATTEMPT_ID_LENGTH, "physicalEnqueueAttemptId");
        for (byte item : value) {
            if (item != 0) {
                return Bytes.copy(value);
            }
        }
        throw new IllegalArgumentException("physicalEnqueueAttemptId must be non-zero");
    }

    private static CompletionStage<SubmissionOutcomeMessage> completed(final SubmissionOutcomeMessage outcome) {
        return CompletableFuture.completedFuture(outcome);
    }

    @FunctionalInterface
    public interface PulsarNativeSendTransport extends AutoCloseable {
        CompletionStage<PulsarSendResult> send(PulsarNativeSendRequest request);

        @Override
        default void close() {
            // Implementations close their native Producer/connection here.
        }
    }

    @FunctionalInterface
    public interface CredentialFingerprintProvider {
        /** Resolves the immutable credential fingerprint for this exact prepared request. */
        byte[] resolve(NativePreparedDelivery prepared);
    }
}
