package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalPayloadCommitProof;
import com.nereusstream.delay.protocol.FailureStage;
import com.nereusstream.delay.protocol.LargeScheduleIntent;
import com.nereusstream.delay.protocol.ObjectStoreProfileSemantic;
import com.nereusstream.delay.protocol.OpaquePayloadUploadHandle;
import com.nereusstream.delay.protocol.PayloadAttestationOutcome;
import com.nereusstream.delay.protocol.PayloadAttestationResponse;
import com.nereusstream.delay.protocol.PayloadProofTrustSetRef;
import com.nereusstream.delay.protocol.PayloadProofTrustSetSemantic;
import com.nereusstream.delay.protocol.PayloadProofVerifierKey;
import com.nereusstream.delay.protocol.PayloadReservationReceipt;
import com.nereusstream.delay.protocol.PayloadUploadHandleOutcome;
import com.nereusstream.delay.protocol.PayloadUploadHandleResponse;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.StableError;
import com.nereusstream.delay.protocol.UploadHandleKind;
import com.nereusstream.delay.runtime.PayloadReservation;
import com.nereusstream.delay.runtime.PayloadReservationStatus;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.Signature;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Deterministic local Object Store adapter seam for the large-payload path.
 *
 * <p>The adapter binds a durable {@link PayloadReservation} to one service-owned
 * immutable object identity, issues an opaque handle, enforces the profile's
 * if-absent/length/SHA-256 rules, and signs an idempotent
 * {@link CanonicalPayloadCommitProof}. It intentionally keeps capability bytes and
 * payload bytes in memory only. It does not provide provider credentials,
 * remote immutability, Object Store availability evidence, Oxia authority, or
 * production authentication.</p>
 */
public final class InMemoryPayloadObjectStore {
    private static final long RETRY_DELAY_MS = 1_000;
    private static final byte[] CAPABILITY_DOMAIN = Bytes.utf8("nereus-delay-local-upload-capability\0");
    private static final byte[] CONTAINER_PREFIX = Bytes.utf8("nereus-delay-local/");
    private static final byte[] OBJECT_KEY_PREFIX = Bytes.utf8("reservation/");

    private final ProfileSemanticEnvelope profile;
    private final ObjectStoreProfileSemantic objectStore;
    private final PayloadProofTrustSetSemantic trustSet;
    private final byte[] tenantRoutingScope;
    private final int proofKeyVersion;
    private final long maxUploadHandleLifetimeMs;
    private final PrivateKey proofSigningKey;
    private final PayloadObjectBackend payloadBackend;
    private final Map<String, ReservationState> reservations = new HashMap<>();

    /**
     * Creates a local adapter with one immutable Profile and one proof key.
     * The private key is test/local input; production key custody is external.
     */
    public InMemoryPayloadObjectStore(
            final ProfileSemanticEnvelope profile,
            final byte[] tenantRoutingScope,
            final PayloadProofTrustSetSemantic trustSet,
            final int proofKeyVersion,
            final PrivateKey proofSigningKey) {
        this(profile, tenantRoutingScope, trustSet, proofKeyVersion, Long.MAX_VALUE, proofSigningKey);
    }

    /**
     * Creates a local adapter with an explicit short-lived handle lifetime.
     * The effective expiry is the checked minimum of this bound and the
     * reservation expiry; the bound is not a provider credential lease.
     */
    public InMemoryPayloadObjectStore(
            final ProfileSemanticEnvelope profile,
            final byte[] tenantRoutingScope,
            final PayloadProofTrustSetSemantic trustSet,
            final int proofKeyVersion,
            final long maxUploadHandleLifetimeMs,
            final PrivateKey proofSigningKey) {
        this(
                profile,
                tenantRoutingScope,
                trustSet,
                proofKeyVersion,
                maxUploadHandleLifetimeMs,
                proofSigningKey,
                new MemoryPayloadObjectBackend());
    }

    /**
     * Creates the protocol state machine over an explicit immutable-byte
     * backend. This package-private seam is used by the durable local
     * filesystem adapter; production providers still need a separately
     * authenticated adapter and authority transaction.
     */
    InMemoryPayloadObjectStore(
            final ProfileSemanticEnvelope profile,
            final byte[] tenantRoutingScope,
            final PayloadProofTrustSetSemantic trustSet,
            final int proofKeyVersion,
            final long maxUploadHandleLifetimeMs,
            final PrivateKey proofSigningKey,
            final PayloadObjectBackend payloadBackend) {
        this.profile = Objects.requireNonNull(profile, "profile");
        if (profile.profileKind() != ProfileKind.OBJECT_STORE
                || !(profile.body() instanceof ObjectStoreProfileSemantic body)) {
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
        this.payloadBackend = Objects.requireNonNull(payloadBackend, "payloadBackend");
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
            // forward. This lets a previously issued receipt resolve to
            // EXPIRED/ABANDONED/CLOSED instead of being misclassified as an
            // integrity failure after a source-ordered state transition.
            previous.reservation = reservation.withReceiptAnchor(previous.receiptAnchor);
        }
    }

    /**
     * Registers a Registry reservation only when this adapter owns the
     * exact trust-set semantic pinned by the durable Prepare binding. A
     * version-only match is insufficient because two immutable semantics may
     * intentionally share a version only in a corrupt or split authority
     * graph.
     */
    public synchronized void register(
            final PayloadReservation reservation,
            final PayloadProofTrustSetRef pinnedTrustSet,
            final ProfileRef pinnedObjectStoreProfile) {
        Objects.requireNonNull(pinnedTrustSet, "pinnedTrustSet");
        Objects.requireNonNull(pinnedObjectStoreProfile, "pinnedObjectStoreProfile");
        if (!trustSet.ref().equals(pinnedTrustSet)) {
            throw new IllegalArgumentException("pinned reservation trust-set does not match adapter semantic");
        }
        if (!profile.ref().equals(pinnedObjectStoreProfile)) {
            throw new IllegalArgumentException("pinned reservation Object Store Profile does not match adapter");
        }
        register(reservation);
    }

    /**
     * Projects the exact registered reservation into the client receipt used
     * by the authenticated handle/attestation API. The object identity is
     * service-owned and deterministic; it is never accepted from the caller.
     */
    public synchronized PayloadReservationReceipt reservationReceipt(final PayloadReservation reservation) {
        Objects.requireNonNull(reservation, "reservation");
        final ReservationState state = reservations.get(key(reservation.reservationId()));
        if (state == null || !Arrays.equals(state.reservation.encode(), reservation.encode())) {
            throw new IllegalArgumentException("reservation is not the exact registered binding");
        }
        final PayloadReservation anchor = state.receiptAnchor;
        return PayloadReservationReceipt.create(
                anchor.reservationId(),
                anchor.delayMessageId(),
                anchor.shardId(),
                SourcePositionCodec.decode(anchor.sourcePosition()),
                anchor.stateVersion(),
                profile.ref(),
                containerFor(profile),
                objectKeyFor(anchor),
                anchor.intent().expectedPayloadLength(),
                anchor.intent().payloadSha256(),
                anchor.reservationExpiryEpochMs(),
                trustSet.ref());
    }

    /**
     * Issues a stable handle for the registered reservation. The same
     * reservation/kind returns byte-identical handle bytes after response loss.
     */
    public synchronized PayloadUploadHandleResponse issueUploadHandle(
            final byte[] reservationId, final UploadHandleKind kind, final long nowEpochMs) {
        if (nowEpochMs < 0) {
            return uploadError(PayloadUploadHandleOutcome.INTEGRITY_ERROR, null);
        }
        Objects.requireNonNull(kind, "kind");
        requireNow(nowEpochMs);
        final ReservationState state = reservations.get(key(reservationId));
        if (state == null) {
            return uploadError(PayloadUploadHandleOutcome.NOT_FOUND_OR_NOT_AUTHORIZED, null);
        }
        final PayloadUploadHandleOutcome lifecycle = uploadLifecycle(state, nowEpochMs);
        if (lifecycle != null) {
            return uploadError(lifecycle, null);
        }
        if (!supports(kind)) {
            return uploadError(PayloadUploadHandleOutcome.INTEGRITY_ERROR, null);
        }
        final OpaquePayloadUploadHandle existing = state.handles.get(kind);
        if (existing != null && nowEpochMs <= existing.expiresAtEpochMs()) {
            return PayloadUploadHandleResponse.issued(existing);
        }
        if (existing != null) {
            state.handles.remove(kind);
        }
        final long handleExpiry = boundedHandleExpiry(
                nowEpochMs, state.reservation.reservationExpiryEpochMs(), maxUploadHandleLifetimeMs);
        final byte[] capability = Bytes.sha256(
                CAPABILITY_DOMAIN,
                state.reservation.reservationId(),
                Bytes.u32be(kind.wireValue()),
                Bytes.u64be(handleExpiry),
                profile.semanticHash());
        final OpaquePayloadUploadHandle handle = OpaquePayloadUploadHandle.create(
                state.reservation.reservationId(), profile.ref(), kind, handleExpiry, capability);
        state.handles.put(kind, handle);
        return PayloadUploadHandleResponse.issued(handle);
    }

    /** Issues a handle only when the complete receipt still matches the durable binding. */
    public synchronized PayloadUploadHandleResponse issueUploadHandle(
            final PayloadReservationReceipt receipt, final UploadHandleKind kind, final long nowEpochMs) {
        if (nowEpochMs < 0) {
            return uploadError(PayloadUploadHandleOutcome.INTEGRITY_ERROR, null);
        }
        Objects.requireNonNull(receipt, "receipt");
        final ReservationState state = reservations.get(key(receipt.reservationId()));
        if (state == null || !matchesReceipt(state, receipt)) {
            return uploadError(PayloadUploadHandleOutcome.NOT_FOUND_OR_NOT_AUTHORIZED, null);
        }
        return issueUploadHandle(receipt.reservationId(), kind, nowEpochMs);
    }

    /**
     * Stores payload bytes under the service-owned identity. Repeating the same
     * bytes is idempotent; a different value is an immutable-object conflict.
     */
    public synchronized void upload(
            final OpaquePayloadUploadHandle handle, final byte[] payload, final long nowEpochMs) {
        Objects.requireNonNull(handle, "handle");
        Objects.requireNonNull(payload, "payload");
        requireNow(nowEpochMs);
        final ReservationState state = requireHandle(handle, nowEpochMs);
        final String objectIdentity = objectIdentity(state.reservation);
        final byte[] existing = payloadBackend.read(objectIdentity);
        if (existing != null) {
            if (Arrays.equals(existing, payload)) {
                return;
            }
            throw new IllegalStateException("immutable Object Store object identity conflict");
        }
        if (payload.length > objectStore.maxObjectBytes()) {
            throw new IllegalArgumentException("payload exceeds Object Store profile maximum");
        }
        if (payload.length != state.reservation.intent().expectedPayloadLength()
                || !Bytes.constantTimeEquals(
                        Bytes.sha256(payload), state.reservation.intent().payloadSha256())) {
            throw new IllegalArgumentException("payload length or SHA-256 does not match reservation");
        }
        payloadBackend.putIfAbsent(objectIdentity, payload, objectStore.maxObjectBytes());
    }

    /** Uploads through a receipt-bound API; receipt drift is rejected before bytes are accepted. */
    public synchronized void upload(
            final PayloadReservationReceipt receipt,
            final OpaquePayloadUploadHandle handle,
            final byte[] payload,
            final long nowEpochMs) {
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(handle, "handle");
        final ReservationState state = reservations.get(key(receipt.reservationId()));
        if (state == null
                || !matchesReceipt(state, receipt)
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
    public synchronized PayloadAttestationResponse attest(
            final OpaquePayloadUploadHandle handle, final long nowEpochMs) {
        if (nowEpochMs < 0) {
            return attestationError(PayloadAttestationOutcome.INTEGRITY_ERROR, null);
        }
        Objects.requireNonNull(handle, "handle");
        requireNow(nowEpochMs);
        final ReservationState state = reservations.get(key(handle.reservationId()));
        if (state == null || !state.matches(handle, nowEpochMs)) {
            return attestationError(PayloadAttestationOutcome.NOT_FOUND_OR_NOT_AUTHORIZED, null);
        }
        final PayloadAttestationOutcome lifecycle = attestationLifecycle(state, nowEpochMs);
        if (lifecycle != null) {
            return attestationError(lifecycle, null);
        }
        final byte[] payload = payloadBackend.read(objectIdentity(state.reservation));
        if (payload == null) {
            return attestationError(
                    PayloadAttestationOutcome.OBJECT_NOT_READY_RETRYABLE,
                    retryAt(state.reservation.reservationExpiryEpochMs(), nowEpochMs));
        }
        if (payload.length != state.reservation.intent().expectedPayloadLength()
                || !Bytes.constantTimeEquals(
                        Bytes.sha256(payload), state.reservation.intent().payloadSha256())) {
            return attestationError(PayloadAttestationOutcome.OBJECT_IDENTITY_CONFLICT, null);
        }
        if (state.proof != null) {
            return PayloadAttestationResponse.attested(state.proof);
        }
        final PayloadProofVerifierKey key = trustKey(trustSet, proofKeyVersion);
        if (nowEpochMs < key.verifyNotBeforeEpochMs() || nowEpochMs > key.verifyNotAfterEpochMs()) {
            return attestationError(PayloadAttestationOutcome.INTEGRITY_ERROR, null);
        }
        final byte[] payloadHash = Bytes.sha256(payload);
        final byte[] container = containerFor(profile);
        final byte[] objectKey = objectKeyFor(state.reservation);
        final byte[] immutableVersion =
                payloadBackend.immutableObjectVersion(objectIdentity(state.reservation), payloadHash);
        final byte[] etag = payloadBackend.etag(objectIdentity(state.reservation), payloadHash);
        state.proof = CanonicalPayloadCommitProof.signed(
                state.reservation.reservationId(),
                tenantRoutingScope,
                state.reservation.shardId().routeIncarnation().bytes(),
                state.reservation.shardId().partition(),
                state.reservation.delayMessageId(),
                profile.ref(),
                trustSet.version(),
                proofKeyVersion,
                container,
                objectKey,
                immutableVersion,
                etag,
                payload.length,
                payloadHash,
                state.reservation.reservationExpiryEpochMs(),
                proofSigningKey);
        return PayloadAttestationResponse.attested(state.proof);
    }

    /** Attests only when both the receipt and handle bind to the same reservation state. */
    public synchronized PayloadAttestationResponse attest(
            final PayloadReservationReceipt receipt, final OpaquePayloadUploadHandle handle, final long nowEpochMs) {
        if (nowEpochMs < 0) {
            return attestationError(PayloadAttestationOutcome.INTEGRITY_ERROR, null);
        }
        Objects.requireNonNull(receipt, "receipt");
        Objects.requireNonNull(handle, "handle");
        final ReservationState state = reservations.get(key(receipt.reservationId()));
        if (state == null
                || !matchesReceipt(state, receipt)
                || !Arrays.equals(receipt.reservationId(), handle.reservationId())) {
            return attestationError(PayloadAttestationOutcome.NOT_FOUND_OR_NOT_AUTHORIZED, null);
        }
        return attest(handle, nowEpochMs);
    }

    private ReservationState requireHandle(final OpaquePayloadUploadHandle handle, final long nowEpochMs) {
        final ReservationState state = reservations.get(key(handle.reservationId()));
        if (state == null || !state.matches(handle, nowEpochMs)) {
            throw new IllegalArgumentException("upload handle is not authorized for this reservation");
        }
        final PayloadUploadHandleOutcome lifecycle = uploadLifecycle(state, nowEpochMs);
        if (lifecycle != null) {
            throw new IllegalStateException("reservation is not uploadable: " + lifecycle);
        }
        return state;
    }

    private PayloadUploadHandleOutcome uploadLifecycle(final ReservationState state, final long nowEpochMs) {
        return switch (state.reservation.status()) {
            case RESERVED ->
                nowEpochMs > state.reservation.reservationExpiryEpochMs()
                        ? PayloadUploadHandleOutcome.RESERVATION_EXPIRED
                        : null;
            case EXPIRED -> PayloadUploadHandleOutcome.RESERVATION_EXPIRED;
            case ABANDONED -> PayloadUploadHandleOutcome.RESERVATION_ABANDONED;
            case COMMITTED -> PayloadUploadHandleOutcome.RESERVATION_CLOSED;
        };
    }

    private PayloadAttestationOutcome attestationLifecycle(final ReservationState state, final long nowEpochMs) {
        return switch (state.reservation.status()) {
            case RESERVED ->
                nowEpochMs > state.reservation.reservationExpiryEpochMs()
                        ? PayloadAttestationOutcome.RESERVATION_EXPIRED
                        : null;
            case EXPIRED -> PayloadAttestationOutcome.RESERVATION_EXPIRED;
            case ABANDONED -> PayloadAttestationOutcome.RESERVATION_ABANDONED;
            case COMMITTED -> PayloadAttestationOutcome.RESERVATION_CLOSED;
        };
    }

    private boolean supports(final UploadHandleKind kind) {
        final int bit = kind == UploadHandleKind.OPAQUE_SINGLE_PUT
                ? ObjectStoreProfileSemantic.SINGLE_PUT
                : ObjectStoreProfileSemantic.MULTIPART;
        return (objectStore.allowedUploadHandleBits() & bit) != 0;
    }

    private boolean matchesReceipt(final ReservationState state, final PayloadReservationReceipt receipt) {
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
                    && Bytes.constantTimeEquals(
                            receipt.payloadSha256(), reservation.intent().payloadSha256())
                    && receipt.reservationExpiryEpochMs() == reservation.reservationExpiryEpochMs()
                    && trustSet.ref().equals(receipt.trustSet());
        } catch (IllegalArgumentException mismatch) {
            return false;
        }
    }

    private static boolean sameReservationIdentity(final PayloadReservation left, final PayloadReservation right) {
        return left.shardId().equals(right.shardId())
                && Arrays.equals(left.reservationId(), right.reservationId())
                && left.commandId().equals(right.commandId())
                && left.delayMessageId().equals(right.delayMessageId())
                && Arrays.equals(left.commandHash(), right.commandHash())
                && left.intent().equals(right.intent())
                && left.reservationExpiryEpochMs() == right.reservationExpiryEpochMs();
    }

    private boolean validStateAdvance(final PayloadReservation previous, final PayloadReservation next) {
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
            return (next.status() != PayloadReservationStatus.COMMITTED || committedPayloadMatchesAdapter(next))
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
                && Bytes.constantTimeEquals(
                        reference.payloadSha256(), reservation.intent().payloadSha256());
    }

    private static boolean isImmediateSuccessor(final PayloadReservation previous, final PayloadReservation next) {
        return isImmediateSuccessor(previous, next, true);
    }

    private static boolean isImmediateSuccessor(
            final PayloadReservation previous, final PayloadReservation next, final boolean requireNewSourcePosition) {
        final long expectedVersion;
        try {
            expectedVersion = Math.addExact(previous.stateVersion(), 1);
        } catch (ArithmeticException overflow) {
            return false;
        }
        return next.stateVersion() == expectedVersion
                && (!requireNewSourcePosition || !Arrays.equals(previous.sourcePosition(), next.sourcePosition()));
    }

    private static byte[] containerFor(final ProfileSemanticEnvelope profile) {
        return Bytes.concat(CONTAINER_PREFIX, Bytes.utf8(Bytes.hex(profile.profileId())));
    }

    private static byte[] objectKeyFor(final PayloadReservation reservation) {
        return Bytes.concat(OBJECT_KEY_PREFIX, Bytes.utf8(key(reservation.reservationId())));
    }

    private static String objectIdentity(final PayloadReservation reservation) {
        return key(reservation.reservationId());
    }

    private static PayloadUploadHandleResponse uploadError(
            final PayloadUploadHandleOutcome outcome, final Long retryAtEpochMs) {
        return PayloadUploadHandleResponse.error(outcome, error(stableCode(outcome), retryAtEpochMs));
    }

    private static PayloadAttestationResponse attestationError(
            final PayloadAttestationOutcome outcome, final Long retryAtEpochMs) {
        return PayloadAttestationResponse.error(outcome, error(stableCode(outcome), retryAtEpochMs));
    }

    private static StableError error(final StableCode code, final Long retryAtEpochMs) {
        return StableError.of(FailureStage.PAYLOAD, code, retryAtEpochMs, null, null, null);
    }

    private static StableCode stableCode(final PayloadUploadHandleOutcome outcome) {
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

    private static StableCode stableCode(final PayloadAttestationOutcome outcome) {
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

    private static long boundedHandleExpiry(
            final long nowEpochMs, final long reservationExpiryEpochMs, final long maxLifetimeMs) {
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

    private static PayloadProofVerifierKey requireTrustKey(
            final PayloadProofTrustSetSemantic trustSet, final int keyVersion) {
        return trustKey(trustSet, keyVersion);
    }

    private static PayloadProofVerifierKey trustKey(final PayloadProofTrustSetSemantic trustSet, final int keyVersion) {
        return trustSet.keys().stream()
                .filter(key -> key.keyVersion() == keyVersion)
                .findFirst()
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
        private final Map<UploadHandleKind, OpaquePayloadUploadHandle> handles = new HashMap<>();
        private CanonicalPayloadCommitProof proof;

        private ReservationState(final PayloadReservation reservation) {
            this.reservation = reservation;
            this.receiptAnchor = reservation.receiptAnchor();
        }

        private boolean matches(final OpaquePayloadUploadHandle handle, final long nowEpochMs) {
            return profile.ref().equals(handle.objectStoreProfile())
                    && Arrays.equals(handle.reservationId(), reservation.reservationId())
                    && nowEpochMs <= handle.expiresAtEpochMs()
                    && handle.expiresAtEpochMs() <= reservation.reservationExpiryEpochMs()
                    && Objects.equals(handles.get(handle.kind()), handle);
        }
    }

    private static final class MemoryPayloadObjectBackend implements PayloadObjectBackend {
        private final Map<String, byte[]> values = new HashMap<>();

        @Override
        public synchronized byte[] read(final String objectIdentity) {
            final byte[] value = values.get(Objects.requireNonNull(objectIdentity, "objectIdentity"));
            return value == null ? null : Bytes.copy(value);
        }

        @Override
        public synchronized void putIfAbsent(final String objectIdentity, final byte[] payload, final long maxBytes) {
            Objects.requireNonNull(objectIdentity, "objectIdentity");
            Objects.requireNonNull(payload, "payload");
            if (payload.length > maxBytes) {
                throw new IllegalArgumentException("payload exceeds Object Store profile maximum");
            }
            final byte[] existing = values.get(objectIdentity);
            if (existing != null && !Arrays.equals(existing, payload)) {
                throw new IllegalStateException("immutable Object Store object identity conflict");
            }
            if (existing == null) {
                values.put(objectIdentity, Bytes.copy(payload));
            }
        }
    }
}
