package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.FailureStageV1;
import io.nereusstream.delay.protocol.LargeScheduleIntent;
import io.nereusstream.delay.protocol.ObjectStoreProfileSemanticV1;
import io.nereusstream.delay.protocol.OpaquePayloadUploadHandleV1;
import io.nereusstream.delay.protocol.PayloadAttestationOutcomeV1;
import io.nereusstream.delay.protocol.PayloadAttestationResponseV1;
import io.nereusstream.delay.protocol.PayloadCommitProofV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetSemanticV1;
import io.nereusstream.delay.protocol.PayloadProofVerifierKeyV1;
import io.nereusstream.delay.protocol.PayloadReservationReceiptV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleOutcomeV1;
import io.nereusstream.delay.protocol.PayloadUploadHandleResponseV1;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileSemanticEnvelopeV1;
import io.nereusstream.delay.protocol.SourcePositionCodec;
import io.nereusstream.delay.protocol.StableCode;
import io.nereusstream.delay.protocol.StableErrorV1;
import io.nereusstream.delay.protocol.UploadHandleKindV1;
import io.nereusstream.delay.runtime.PayloadReservation;
import io.nereusstream.delay.runtime.PayloadReservationStatus;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Deterministic local Object Store adapter seam for the V1 large-payload path.
 *
 * <p>The adapter binds a durable {@link PayloadReservation} to one service-owned
 * immutable object identity, issues an opaque handle, enforces the profile's
 * if-absent/length/SHA-256 rules, and signs an idempotent
 * {@link PayloadCommitProofV1}. It intentionally keeps capability bytes and
 * payload bytes in memory only. It does not provide provider credentials,
 * remote immutability, Object Store availability evidence, Oxia authority, or
 * production authentication.</p>
 */
public final class InMemoryPayloadObjectStore {
    private static final long RETRY_DELAY_MS = 1_000;
    private static final byte[] CAPABILITY_DOMAIN = Bytes.utf8("nereus-delay-local-upload-capability-v1\0");
    private static final byte[] CONTAINER_PREFIX = Bytes.utf8("nereus-delay-local/");
    private static final byte[] OBJECT_KEY_PREFIX = Bytes.utf8("reservation/");
    private static final byte[] OBJECT_VERSION_PREFIX = Bytes.utf8("sha256-");

    private final ProfileSemanticEnvelopeV1 profile;
    private final ObjectStoreProfileSemanticV1 objectStore;
    private final PayloadProofTrustSetSemanticV1 trustSet;
    private final byte[] tenantRoutingScope;
    private final int proofKeyVersion;
    private final long maxUploadHandleLifetimeMs;
    private final PrivateKey proofSigningKey;
    private final Map<String, ReservationState> reservations = new HashMap<>();

    /**
     * Creates a local adapter with one immutable Profile and one proof key.
     * The private key is test/local input; production key custody is external.
     */
    public InMemoryPayloadObjectStore(final ProfileSemanticEnvelopeV1 profile,
                                      final byte[] tenantRoutingScope,
                                      final PayloadProofTrustSetSemanticV1 trustSet,
                                      final int proofKeyVersion,
                                      final PrivateKey proofSigningKey) {
        this(profile, tenantRoutingScope, trustSet, proofKeyVersion, Long.MAX_VALUE, proofSigningKey);
    }

    /**
     * Creates a local adapter with an explicit short-lived handle lifetime.
     * The effective expiry is the checked minimum of this bound and the
     * reservation expiry; the bound is not a provider credential lease.
     */
    public InMemoryPayloadObjectStore(final ProfileSemanticEnvelopeV1 profile,
                                      final byte[] tenantRoutingScope,
                                      final PayloadProofTrustSetSemanticV1 trustSet,
                                      final int proofKeyVersion,
                                      final long maxUploadHandleLifetimeMs,
                                      final PrivateKey proofSigningKey) {
        this.profile = Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKindV1.OBJECT_STORE
                || !(profile.body() instanceof ObjectStoreProfileSemanticV1 body)) {
            throw new IllegalArgumentException("payload adapter requires an OBJECT_STORE profile");
        }
        this.objectStore = body;
        Bytes.requireLength(tenantRoutingScope, 32, "tenantRoutingScope");
        this.tenantRoutingScope = Bytes.copy(tenantRoutingScope);
        this.trustSet = Objects.requireNonNull(trustSet, "trustSet");
        this.proofKeyVersion = proofKeyVersion;
        if (maxUploadHandleLifetimeMs <= 0) {
            throw new IllegalArgumentException("maxUploadHandleLifetimeMs must be positive");
        }
        this.maxUploadHandleLifetimeMs = maxUploadHandleLifetimeMs;
        this.proofSigningKey = Objects.requireNonNull(proofSigningKey, "proofSigningKey");
        requireTrustKey(trustSet, proofKeyVersion);
        requireEd25519PrivateKey(proofSigningKey);
    }

    /**
     * Registers the exact durable reservation binding. Re-registering the same
     * canonical reservation is response-loss idempotent; a legal source-ordered
     * lifecycle advance updates only the current closed-outcome projection while
     * retaining the original Prepare receipt anchor. Any identity or illegal
     * state drift is rejected before a capability can be issued.
     */
    public synchronized void register(final PayloadReservation reservation) {
        Objects.requireNonNull(reservation, "reservation");
        final LargeScheduleIntent intent = reservation.intent();
        if (intent.payloadProofTrustSetVersion() != trustSet.version()) {
            throw new IllegalArgumentException("reservation trust-set version does not match adapter");
        }
        final String key = key(reservation.reservationId());
        final ReservationState previous = reservations.get(key);
        if (previous == null) {
            reservations.put(key, new ReservationState(reservation));
        } else if (Arrays.equals(previous.reservation.encode(), reservation.encode())) {
            return;
        } else {
            if (!sameReservationIdentity(previous.reservation, reservation)
                    || !validStateAdvance(previous.reservation, reservation)) {
                throw new IllegalStateException("reservation identity or state drifted");
            }
            // The receipt anchor remains the first registered Prepare state;
            // only the lifecycle projection used for closed outcomes moves
            // forward.  This lets a previously issued receipt resolve to
            // EXPIRED/ABANDONED/CLOSED instead of being misclassified as an
            // integrity failure after a source-ordered state transition.
            previous.reservation = reservation;
        }
    }

    /**
     * Projects the exact registered reservation into the client receipt used
     * by the authenticated handle/attestation API. The object identity is
     * service-owned and deterministic; it is never accepted from the caller.
     */
    public synchronized PayloadReservationReceiptV1 reservationReceipt(final PayloadReservation reservation) {
        Objects.requireNonNull(reservation, "reservation");
        final ReservationState state = reservations.get(key(reservation.reservationId()));
        if (state == null || !Arrays.equals(state.reservation.encode(), reservation.encode())) {
            throw new IllegalArgumentException("reservation is not the exact registered binding");
        }
        final PayloadReservation anchor = state.receiptAnchor;
        return PayloadReservationReceiptV1.create(anchor.reservationId(), anchor.delayMessageId(),
                anchor.shardId(), SourcePositionCodec.decode(anchor.sourcePosition()),
                anchor.stateVersion(), profile.ref(), containerFor(profile), objectKeyFor(anchor),
                anchor.intent().expectedPayloadLength(), anchor.intent().payloadSha256(),
                anchor.reservationExpiryEpochMs(), trustSet.ref());
    }

    /**
     * Issues a stable handle for the registered reservation. The same
     * reservation/kind returns byte-identical handle bytes after response loss.
     */
    public synchronized PayloadUploadHandleResponseV1 issueUploadHandle(final byte[] reservationId,
                                                                          final UploadHandleKindV1 kind,
                                                                          final long nowEpochMs) {
        if (nowEpochMs < 0) {
            return uploadError(PayloadUploadHandleOutcomeV1.INTEGRITY_ERROR, null);
        }
        Objects.requireNonNull(kind, "kind");
        requireNow(nowEpochMs);
        final ReservationState state = reservations.get(key(reservationId));
        if (state == null) {
            return uploadError(PayloadUploadHandleOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED, null);
        }
        final PayloadUploadHandleOutcomeV1 lifecycle = uploadLifecycle(state, nowEpochMs);
        if (lifecycle != null) {
            return uploadError(lifecycle, null);
        }
        if (!supports(kind)) {
            return uploadError(PayloadUploadHandleOutcomeV1.INTEGRITY_ERROR, null);
        }
        final OpaquePayloadUploadHandleV1 existing = state.handles.get(kind);
        if (existing != null && nowEpochMs <= existing.expiresAtEpochMs()) {
            return PayloadUploadHandleResponseV1.issued(existing);
        }
        if (existing != null) {
            state.handles.remove(kind);
        }
        final long handleExpiry = boundedHandleExpiry(nowEpochMs,
                state.reservation.reservationExpiryEpochMs(), maxUploadHandleLifetimeMs);
        final byte[] capability = Bytes.sha256(CAPABILITY_DOMAIN, state.reservation.reservationId(),
                Bytes.u32be(kind.wireValue()), Bytes.u64be(handleExpiry),
                profile.semanticHash());
        final OpaquePayloadUploadHandleV1 handle = OpaquePayloadUploadHandleV1.create(
                state.reservation.reservationId(), profile.ref(), kind,
                handleExpiry, capability);
        state.handles.put(kind, handle);
        return PayloadUploadHandleResponseV1.issued(handle);
    }

    /** Issues a handle only when the complete receipt still matches the durable binding. */
    public synchronized PayloadUploadHandleResponseV1 issueUploadHandle(final PayloadReservationReceiptV1 receipt,
                                                                          final UploadHandleKindV1 kind,
                                                                          final long nowEpochMs) {
        if (nowEpochMs < 0) {
            return uploadError(PayloadUploadHandleOutcomeV1.INTEGRITY_ERROR, null);
        }
        Objects.requireNonNull(receipt, "receipt");
        final ReservationState state = reservations.get(key(receipt.reservationId()));
        if (state == null || !matchesReceipt(state, receipt)) {
            return uploadError(PayloadUploadHandleOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED, null);
        }
        return issueUploadHandle(receipt.reservationId(), kind, nowEpochMs);
    }

    /**
     * Stores payload bytes under the service-owned identity. Repeating the same
     * bytes is idempotent; a different value is an immutable-object conflict.
     */
    public synchronized void upload(final OpaquePayloadUploadHandleV1 handle, final byte[] payload,
                                    final long nowEpochMs) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(payload, "payload");
        requireNow(nowEpochMs);
        final ReservationState state = requireHandle(handle, nowEpochMs);
        if (state.payload != null) {
            if (Arrays.equals(state.payload, payload)) {
                return;
            }
            throw new IllegalStateException("immutable Object Store object identity conflict");
        }
        if (payload.length > objectStore.maxObjectBytes()) {
            throw new IllegalArgumentException("payload exceeds Object Store profile maximum");
        }
        if (payload.length != state.reservation.intent().expectedPayloadLength()
                || !Bytes.constantTimeEquals(Bytes.sha256(payload), state.reservation.intent().payloadSha256())) {
            throw new IllegalArgumentException("payload length or SHA-256 does not match reservation");
        }
        state.payload = Bytes.copy(payload);
    }

    /** Uploads through a receipt-bound API; receipt drift is rejected before bytes are accepted. */
    public synchronized void upload(final PayloadReservationReceiptV1 receipt,
                                    final OpaquePayloadUploadHandleV1 handle, final byte[] payload,
                                    final long nowEpochMs) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(handle, "handle");
        final ReservationState state = reservations.get(key(receipt.reservationId()));
        if (state == null || !matchesReceipt(state, receipt)
                || !Arrays.equals(receipt.reservationId(), handle.reservationId())) {
            throw new IllegalArgumentException("payload receipt is not authorized for this upload handle");
        }
        upload(handle, payload, nowEpochMs);
    }

    /**
     * Attests the uploaded object and returns a stable proof. The proof is
     * cached by the reservation so retries after a lost response return the
     * exact same canonical bytes.
     */
    public synchronized PayloadAttestationResponseV1 attest(final OpaquePayloadUploadHandleV1 handle,
                                                             final long nowEpochMs) {
        if (nowEpochMs < 0) {
            return attestationError(PayloadAttestationOutcomeV1.INTEGRITY_ERROR, null);
        }
        Objects.requireNonNull(handle, "handle");
        requireNow(nowEpochMs);
        final ReservationState state = reservations.get(key(handle.reservationId()));
        if (state == null || !state.matches(handle, nowEpochMs)) {
            return attestationError(PayloadAttestationOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED, null);
        }
        final PayloadAttestationOutcomeV1 lifecycle = attestationLifecycle(state, nowEpochMs);
        if (lifecycle != null) {
            return attestationError(lifecycle, null);
        }
        if (state.payload == null) {
            return attestationError(PayloadAttestationOutcomeV1.OBJECT_NOT_READY_RETRYABLE,
                    retryAt(state.reservation.reservationExpiryEpochMs(), nowEpochMs));
        }
        if (state.proof != null) {
            return PayloadAttestationResponseV1.attested(state.proof);
        }
        final PayloadProofVerifierKeyV1 key = trustKey(trustSet, proofKeyVersion);
        if (nowEpochMs < key.verifyNotBeforeEpochMs() || nowEpochMs > key.verifyNotAfterEpochMs()) {
            return attestationError(PayloadAttestationOutcomeV1.INTEGRITY_ERROR, null);
        }
        final byte[] payloadHash = Bytes.sha256(state.payload);
        final byte[] container = containerFor(profile);
        final byte[] objectKey = objectKeyFor(state.reservation);
        final byte[] immutableVersion = Bytes.concat(OBJECT_VERSION_PREFIX, Bytes.utf8(Bytes.hex(payloadHash)));
        final byte[] etag = payloadHash;
        state.proof = PayloadCommitProofV1.signed(state.reservation.reservationId(), tenantRoutingScope,
                state.reservation.shardId().routeIncarnation().bytes(), state.reservation.shardId().partition(),
                state.reservation.delayMessageId(), profile.ref(), trustSet.version(), proofKeyVersion, container,
                objectKey, immutableVersion, etag, state.payload.length, payloadHash,
                state.reservation.reservationExpiryEpochMs(), proofSigningKey);
        return PayloadAttestationResponseV1.attested(state.proof);
    }

    /** Attests only when both the receipt and handle bind to the same reservation state. */
    public synchronized PayloadAttestationResponseV1 attest(final PayloadReservationReceiptV1 receipt,
                                                             final OpaquePayloadUploadHandleV1 handle,
                                                             final long nowEpochMs) {
        if (nowEpochMs < 0) {
            return attestationError(PayloadAttestationOutcomeV1.INTEGRITY_ERROR, null);
        }
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(handle, "handle");
        final ReservationState state = reservations.get(key(receipt.reservationId()));
        if (state == null || !matchesReceipt(state, receipt)
                || !Arrays.equals(receipt.reservationId(), handle.reservationId())) {
            return attestationError(PayloadAttestationOutcomeV1.NOT_FOUND_OR_NOT_AUTHORIZED, null);
        }
        return attest(handle, nowEpochMs);
    }

    private ReservationState requireHandle(final OpaquePayloadUploadHandleV1 handle, final long nowEpochMs) {
        final ReservationState state = reservations.get(key(handle.reservationId()));
        if (state == null || !state.matches(handle, nowEpochMs)) {
            throw new IllegalArgumentException("upload handle is not authorized for this reservation");
        }
        final PayloadUploadHandleOutcomeV1 lifecycle = uploadLifecycle(state, nowEpochMs);
        if (lifecycle != null) {
            throw new IllegalStateException("reservation is not uploadable: " + lifecycle);
        }
        return state;
    }

    private PayloadUploadHandleOutcomeV1 uploadLifecycle(final ReservationState state, final long nowEpochMs) {
        return switch (state.reservation.status()) {
            case RESERVED -> nowEpochMs > state.reservation.reservationExpiryEpochMs()
                    ? PayloadUploadHandleOutcomeV1.RESERVATION_EXPIRED : null;
            case EXPIRED -> PayloadUploadHandleOutcomeV1.RESERVATION_EXPIRED;
            case ABANDONED -> PayloadUploadHandleOutcomeV1.RESERVATION_ABANDONED;
            case COMMITTED -> PayloadUploadHandleOutcomeV1.RESERVATION_CLOSED;
        };
    }

    private PayloadAttestationOutcomeV1 attestationLifecycle(final ReservationState state, final long nowEpochMs) {
        return switch (state.reservation.status()) {
            case RESERVED -> nowEpochMs > state.reservation.reservationExpiryEpochMs()
                    ? PayloadAttestationOutcomeV1.RESERVATION_EXPIRED : null;
            case EXPIRED -> PayloadAttestationOutcomeV1.RESERVATION_EXPIRED;
            case ABANDONED -> PayloadAttestationOutcomeV1.RESERVATION_ABANDONED;
            case COMMITTED -> PayloadAttestationOutcomeV1.RESERVATION_CLOSED;
        };
    }

    private boolean supports(final UploadHandleKindV1 kind) {
        final int bit = kind == UploadHandleKindV1.OPAQUE_SINGLE_PUT
                ? ObjectStoreProfileSemanticV1.SINGLE_PUT : ObjectStoreProfileSemanticV1.MULTIPART;
        return (objectStore.allowedUploadHandleBits() & bit) != 0;
    }

    private boolean matchesReceipt(final ReservationState state, final PayloadReservationReceiptV1 receipt) {
        final PayloadReservation reservation = state.receiptAnchor;
        try {
            return Arrays.equals(receipt.reservationId(), reservation.reservationId())
                    && receipt.delayMessageId().equals(reservation.delayMessageId())
                    && receipt.shardId().equals(reservation.shardId())
                    && Arrays.equals(receipt.appliedSourcePosition().canonicalBytes(), reservation.sourcePosition())
                    && receipt.stateVersion() == reservation.stateVersion()
                    && profile.ref().equals(receipt.objectStoreProfile())
                    && Arrays.equals(receipt.container(), containerFor(profile))
                    && Arrays.equals(receipt.objectKey(), objectKeyFor(reservation))
                    && receipt.expectedLength() == reservation.intent().expectedPayloadLength()
                    && Bytes.constantTimeEquals(receipt.payloadSha256(), reservation.intent().payloadSha256())
                    && receipt.reservationExpiryEpochMs() == reservation.reservationExpiryEpochMs()
                    && trustSet.ref().equals(receipt.trustSet());
        } catch (IllegalArgumentException mismatch) {
            return false;
        }
    }

    private static boolean sameReservationIdentity(final PayloadReservation left,
                                                   final PayloadReservation right) {
        return left.shardId().equals(right.shardId())
                && Arrays.equals(left.reservationId(), right.reservationId())
                && left.commandId().equals(right.commandId())
                && left.delayMessageId().equals(right.delayMessageId())
                && Arrays.equals(left.commandHash(), right.commandHash())
                && left.intent().equals(right.intent())
                && left.reservationExpiryEpochMs() == right.reservationExpiryEpochMs();
    }

    private boolean validStateAdvance(final PayloadReservation previous,
                                      final PayloadReservation next) {
        if (previous.status() == PayloadReservationStatus.RESERVED
                && next.status() == PayloadReservationStatus.EXPIRED
                && previous.stateVersion() == next.stateVersion()
                && Arrays.equals(previous.sourcePosition(), next.sourcePosition())) {
            // TIME_FENCE's logical overlay changes only the projected status;
            // it does not create a new durable state version.
            return true;
        }
        if (previous.status() == PayloadReservationStatus.RESERVED
                && (next.status() == PayloadReservationStatus.ABANDONED
                || next.status() == PayloadReservationStatus.COMMITTED)) {
            return (next.status() != PayloadReservationStatus.COMMITTED
                    || committedPayloadMatchesAdapter(next))
                    && isImmediateSuccessor(previous, next);
        }
        if (previous.status() == PayloadReservationStatus.EXPIRED
                && next.status() == PayloadReservationStatus.EXPIRED) {
            // Expiry materialization may persist the already selected overlay
            // as a new state version after the adapter has observed it.
            return isImmediateSuccessor(previous, next, false);
        }
        return false;
    }

    private boolean committedPayloadMatchesAdapter(final PayloadReservation reservation) {
        final var reference = reservation.committedPayload();
        return reference != null
                && Arrays.equals(reference.objectStoreProfileHash(), profile.semanticHash())
                && Arrays.equals(reference.container(), containerFor(profile))
                && Arrays.equals(reference.objectKey(), objectKeyFor(reservation))
                && reference.length() == reservation.intent().expectedPayloadLength()
                && Bytes.constantTimeEquals(reference.payloadSha256(), reservation.intent().payloadSha256());
    }

    private static boolean isImmediateSuccessor(final PayloadReservation previous,
                                                final PayloadReservation next) {
        return isImmediateSuccessor(previous, next, true);
    }

    private static boolean isImmediateSuccessor(final PayloadReservation previous,
                                                final PayloadReservation next,
                                                final boolean requireNewSourcePosition) {
        final long expectedVersion;
        try {
            expectedVersion = Math.addExact(previous.stateVersion(), 1);
        } catch (ArithmeticException overflow) {
            return false;
        }
        return next.stateVersion() == expectedVersion
                && (!requireNewSourcePosition
                || !Arrays.equals(previous.sourcePosition(), next.sourcePosition()));
    }

    private static byte[] containerFor(final ProfileSemanticEnvelopeV1 profile) {
        return Bytes.concat(CONTAINER_PREFIX, Bytes.utf8(Bytes.hex(profile.profileId())));
    }

    private static byte[] objectKeyFor(final PayloadReservation reservation) {
        return Bytes.concat(OBJECT_KEY_PREFIX, Bytes.utf8(key(reservation.reservationId())));
    }

    private static PayloadUploadHandleResponseV1 uploadError(final PayloadUploadHandleOutcomeV1 outcome,
                                                              final Long retryAtEpochMs) {
        return PayloadUploadHandleResponseV1.error(outcome, error(stableCode(outcome), retryAtEpochMs));
    }

    private static PayloadAttestationResponseV1 attestationError(final PayloadAttestationOutcomeV1 outcome,
                                                                  final Long retryAtEpochMs) {
        return PayloadAttestationResponseV1.error(outcome, error(stableCode(outcome), retryAtEpochMs));
    }

    private static StableErrorV1 error(final StableCode code, final Long retryAtEpochMs) {
        return StableErrorV1.of(FailureStageV1.PAYLOAD, code, retryAtEpochMs, null, null, null);
    }

    private static StableCode stableCode(final PayloadUploadHandleOutcomeV1 outcome) {
        return switch (outcome) {
            case RESERVATION_EXPIRED -> StableCode.RESERVATION_EXPIRED;
            case RESERVATION_ABANDONED -> StableCode.RESERVATION_ABANDONED;
            case RESERVATION_CLOSED -> StableCode.PAYLOAD_RESERVATION_CLOSED;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> StableCode.NOT_FOUND_OR_NOT_AUTHORIZED;
            case SHARD_TRANSITIONING -> StableCode.SHARD_TRANSITIONING;
            case SHARD_UNAVAILABLE -> StableCode.SHARD_UNAVAILABLE;
            case INTEGRITY_ERROR -> StableCode.INTEGRITY_ERROR;
            case OBJECT_STORE_UNAVAILABLE_RETRYABLE -> StableCode.OBJECT_STORE_UNAVAILABLE_RETRYABLE;
            case ISSUED -> throw new IllegalArgumentException("ISSUED has no error");
        };
    }

    private static StableCode stableCode(final PayloadAttestationOutcomeV1 outcome) {
        return switch (outcome) {
            case OBJECT_NOT_READY_RETRYABLE -> StableCode.OBJECT_NOT_READY_RETRYABLE;
            case OBJECT_STORE_UNAVAILABLE_RETRYABLE -> StableCode.OBJECT_STORE_UNAVAILABLE_RETRYABLE;
            case OBJECT_IDENTITY_CONFLICT -> StableCode.OBJECT_IDENTITY_CONFLICT;
            case RESERVATION_EXPIRED -> StableCode.RESERVATION_EXPIRED;
            case RESERVATION_ABANDONED -> StableCode.RESERVATION_ABANDONED;
            case RESERVATION_CLOSED -> StableCode.PAYLOAD_RESERVATION_CLOSED;
            case NOT_FOUND_OR_NOT_AUTHORIZED -> StableCode.NOT_FOUND_OR_NOT_AUTHORIZED;
            case SHARD_TRANSITIONING -> StableCode.SHARD_TRANSITIONING;
            case SHARD_UNAVAILABLE -> StableCode.SHARD_UNAVAILABLE;
            case INTEGRITY_ERROR -> StableCode.INTEGRITY_ERROR;
            case ATTESTED -> throw new IllegalArgumentException("ATTESTED has no error");
        };
    }

    private static long retryAt(final long expiryEpochMs, final long nowEpochMs) {
        final long candidate;
        try {
            candidate = Math.addExact(nowEpochMs, RETRY_DELAY_MS);
        } catch (ArithmeticException overflow) {
            return expiryEpochMs;
        }
        return Math.min(candidate, expiryEpochMs);
    }

    private static long boundedHandleExpiry(final long nowEpochMs, final long reservationExpiryEpochMs,
                                            final long maxLifetimeMs) {
        final long candidate;
        try {
            candidate = Math.addExact(nowEpochMs, maxLifetimeMs);
        } catch (ArithmeticException overflow) {
            return reservationExpiryEpochMs;
        }
        return Math.min(candidate, reservationExpiryEpochMs);
    }

    private static void requireNow(final long nowEpochMs) {
        if (nowEpochMs < 0) {
            throw new IllegalArgumentException("nowEpochMs must be non-negative");
        }
    }

    private static String key(final byte[] reservationId) {
        Bytes.requireLength(reservationId, 32, "reservationId");
        return Bytes.hex(reservationId);
    }

    private static PayloadProofVerifierKeyV1 requireTrustKey(final PayloadProofTrustSetSemanticV1 trustSet,
                                                              final int keyVersion) {
        return trustKey(trustSet, keyVersion);
    }

    private static PayloadProofVerifierKeyV1 trustKey(final PayloadProofTrustSetSemanticV1 trustSet,
                                                       final int keyVersion) {
        return trustSet.keys().stream().filter(key -> key.keyVersion() == keyVersion).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("proof key is not in trust set"));
    }

    private static void requireEd25519PrivateKey(final PrivateKey privateKey) {
        if (!"Ed25519".equalsIgnoreCase(privateKey.getAlgorithm())
                && !"EdDSA".equalsIgnoreCase(privateKey.getAlgorithm())) {
            throw new IllegalArgumentException("proof signing key must use Ed25519");
        }
        try {
            Signature.getInstance("Ed25519");
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 signing is unavailable", exception);
        }
    }

    private final class ReservationState {
        private PayloadReservation reservation;
        private final PayloadReservation receiptAnchor;
        private final Map<UploadHandleKindV1, OpaquePayloadUploadHandleV1> handles = new HashMap<>();
        private byte[] payload;
        private PayloadCommitProofV1 proof;

        private ReservationState(final PayloadReservation reservation) {
            this.reservation = reservation;
            this.receiptAnchor = reservation;
        }

        private boolean matches(final OpaquePayloadUploadHandleV1 handle, final long nowEpochMs) {
            return profile.ref().equals(handle.objectStoreProfile())
                    && Arrays.equals(handle.reservationId(), reservation.reservationId())
                    && nowEpochMs <= handle.expiresAtEpochMs()
                    && handle.expiresAtEpochMs() <= reservation.reservationExpiryEpochMs()
                    && Objects.equals(handles.get(handle.kind()), handle);
        }
    }
}
