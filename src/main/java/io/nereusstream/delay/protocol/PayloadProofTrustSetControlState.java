package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Replay-stable local projection of the source-ordered payload-proof controls.
 *
 * <p>The trust-set bytes themselves are resolved by an external catalog.  This
 * value deliberately stores only the activation and issuance-close markers,
 * because a marker must never make a historical verifier set disappear from
 * the replay/Recovery-Floor path.</p>
 */
public final class PayloadProofTrustSetControlState {
    private static final int STATE_VERSION = 1;

    private final List<ActivationMarker> activations;
    private final List<IssuanceClosure> closures;

    private PayloadProofTrustSetControlState(final List<ActivationMarker> activations,
                                             final List<IssuanceClosure> closures) {
        this.activations = validateActivations(activations);
        this.closures = validateClosures(closures);
    }

    public static PayloadProofTrustSetControlState empty() {
        return new PayloadProofTrustSetControlState(List.of(), List.of());
    }

    public List<ActivationMarker> activations() {
        return activations;
    }

    public List<IssuanceClosure> closures() {
        return closures;
    }

    public Optional<PayloadProofTrustSetRefV1> activeTrustSet() {
        return activations.isEmpty()
                ? Optional.empty() : Optional.of(activations.get(activations.size() - 1).trustSet());
    }

    public Optional<SourcePosition> activeTrustSetPosition() {
        return activations.isEmpty()
                ? Optional.empty() : Optional.of(activations.get(activations.size() - 1).sourcePosition());
    }

    /**
     * Applies an activation marker.  Replaying the exact marker is an
     * idempotent no-op; a version can never be activated again at a later
     * source position.
     */
    public PayloadProofTrustSetControlState activate(final PayloadProofTrustSetRefV1 trustSet,
                                                     final SourcePosition sourcePosition) {
        Objects.requireNonNull(trustSet, "trustSet");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (!activations.isEmpty()) {
            final ActivationMarker previous = activations.get(activations.size() - 1);
            final int order = compare(sourcePosition, previous.sourcePosition());
            if (trustSet.equals(previous.trustSet()) && order == 0) {
                return this;
            }
            if (order <= 0) {
                throw new IllegalArgumentException("trust-set activation source position regressed");
            }
            if (trustSet.version() <= previous.trustSet().version()) {
                throw new IllegalArgumentException("trust-set activation version regressed");
            }
        }
        final List<ActivationMarker> next = new ArrayList<>(activations);
        next.add(new ActivationMarker(trustSet, sourcePosition));
        return new PayloadProofTrustSetControlState(next, closures);
    }

    /** Applies the first-seen issuance close marker for one trust-set key. */
    public PayloadProofTrustSetControlState close(final PayloadProofIssuanceClosePayloadV1 close,
                                                  final SourcePosition sourcePosition) {
        Objects.requireNonNull(close, "close");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        final ActivationMarker activation = activationFor(close.trustSet());
        if (activation == null) {
            throw new IllegalArgumentException("cannot close a trust set before activation");
        }
        final int activationOrder = compare(sourcePosition, activation.sourcePosition());
        if (activationOrder <= 0) {
            throw new IllegalArgumentException("trust-set close must follow activation");
        }
        final IssuanceClosure existing = closureFor(close.trustSet(), close.proofKeyVersion());
        if (existing != null) {
            if (compare(sourcePosition, existing.sourcePosition()) == 0
                    && existing.reason().equals(close.reason())) {
                return this;
            }
            throw new IllegalArgumentException("payload proof key issuance is already closed");
        }
        final List<IssuanceClosure> next = new ArrayList<>(closures);
        next.add(new IssuanceClosure(close.trustSet(), close.proofKeyVersion(), sourcePosition, close.reason()));
        next.sort(closureComparator());
        return new PayloadProofTrustSetControlState(activations, next);
    }

    /** Returns true when a trust-set ref had been activated by the supplied position. */
    public boolean activatedAt(final PayloadProofTrustSetRefV1 trustSet, final SourcePosition sourcePosition) {
        Objects.requireNonNull(trustSet, "trustSet");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        final ActivationMarker activation = activationFor(trustSet);
        return activation != null && compare(activation.sourcePosition(), sourcePosition) <= 0;
    }

    /**
     * Checks source-ordered authorization for a first-seen Commit.  Key
     * existence and signature verification are intentionally delegated to the
     * resolved immutable trust-set value.
     */
    public boolean firstSeenIssuanceOpen(final PayloadProofTrustSetRefV1 trustSet, final int proofKeyVersion,
                                         final SourcePosition sourcePosition) {
        if (proofKeyVersion == 0 || !activatedAt(trustSet, sourcePosition)) {
            return false;
        }
        final IssuanceClosure closure = closureFor(trustSet, proofKeyVersion);
        return closure == null || compare(closure.sourcePosition(), sourcePosition) > 0;
    }

    /** Historical verification remains possible after issuance is closed. */
    public boolean historicalVerificationAllowed(final PayloadProofTrustSetRefV1 trustSet,
                                                  final int proofKeyVersion,
                                                  final SourcePosition sourcePosition) {
        return proofKeyVersion != 0 && activatedAt(trustSet, sourcePosition);
    }

    public Optional<IssuanceClosure> closure(final PayloadProofTrustSetRefV1 trustSet,
                                             final int proofKeyVersion) {
        return Optional.ofNullable(closureFor(trustSet, proofKeyVersion));
    }

    /** Canonical local value projection used by the shard meta_cf state. */
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, STATE_VERSION);
            for (ActivationMarker activation : activations) {
                CanonicalProtobuf.bytes(output, 2, activation.canonicalBytes());
            }
            for (IssuanceClosure closure : closures) {
                CanonicalProtobuf.bytes(output, 3, closure.canonicalBytes());
            }
        });
    }

    public static PayloadProofTrustSetControlState decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = readRepeated(encoded,
                "PayloadProofTrustSetControlState");
        if (fields.isEmpty() || fields.get(0).number() != 1
                || QueryCodecSupport.uint32(fields.get(0), 1) != STATE_VERSION) {
            throw new IllegalArgumentException("invalid trust-set control state version");
        }
        final List<ActivationMarker> activations = new ArrayList<>();
        final List<IssuanceClosure> closures = new ArrayList<>();
        int phase = 2;
        for (int index = 1; index < fields.size(); index++) {
            final CanonicalProtobuf.Reader.Field field = fields.get(index);
            if (field.number() == 2 && phase == 2) {
                activations.add(ActivationMarker.decode(QueryCodecSupport.nested(field, 2)));
            } else if (field.number() == 3) {
                phase = 3;
                closures.add(IssuanceClosure.decode(QueryCodecSupport.nested(field, 3)));
            } else {
                throw new IllegalArgumentException("trust-set control state fields are out of order");
            }
        }
        final PayloadProofTrustSetControlState result =
                new PayloadProofTrustSetControlState(activations, closures);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(),
                "PayloadProofTrustSetControlState");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadProofTrustSetControlState that
                && activations.equals(that.activations) && closures.equals(that.closures);
    }

    @Override
    public int hashCode() {
        return Objects.hash(activations, closures);
    }

    private ActivationMarker activationFor(final PayloadProofTrustSetRefV1 trustSet) {
        for (ActivationMarker activation : activations) {
            if (activation.trustSet().equals(trustSet)) {
                return activation;
            }
        }
        return null;
    }

    private IssuanceClosure closureFor(final PayloadProofTrustSetRefV1 trustSet, final int proofKeyVersion) {
        for (IssuanceClosure closure : closures) {
            if (closure.trustSet().equals(trustSet) && closure.proofKeyVersion() == proofKeyVersion) {
                return closure;
            }
        }
        return null;
    }

    private static List<ActivationMarker> validateActivations(final List<ActivationMarker> values) {
        Objects.requireNonNull(values, "activations");
        final List<ActivationMarker> result = new ArrayList<>(values.size());
        PayloadProofTrustSetRefV1 previousRef = null;
        SourcePosition previousPosition = null;
        for (ActivationMarker value : values) {
            Objects.requireNonNull(value, "activation marker");
            if (previousPosition != null) {
                final int order = compare(value.sourcePosition(), previousPosition);
                if (order <= 0 || value.trustSet().version() <= previousRef.version()) {
                    throw new IllegalArgumentException("trust-set activations are not source/version ordered");
                }
            }
            result.add(value);
            previousRef = value.trustSet();
            previousPosition = value.sourcePosition();
        }
        return List.copyOf(result);
    }

    private static List<IssuanceClosure> validateClosures(final List<IssuanceClosure> values) {
        Objects.requireNonNull(values, "closures");
        final List<IssuanceClosure> result = new ArrayList<>(values.size());
        values.stream().sorted(closureComparator()).forEach(value -> {
            Objects.requireNonNull(value, "issuance closure");
            if (!result.isEmpty()) {
                final IssuanceClosure previous = result.get(result.size() - 1);
                if (previous.trustSet().equals(value.trustSet())
                        && previous.proofKeyVersion() == value.proofKeyVersion()) {
                    throw new IllegalArgumentException("duplicate trust-set issuance closure");
                }
            }
            result.add(value);
        });
        return List.copyOf(result);
    }

    private static Comparator<IssuanceClosure> closureComparator() {
        return Comparator.comparing((IssuanceClosure value) -> value.trustSet().canonicalBytes(),
                        PayloadProofTrustSetControlState::compareBytes)
                .thenComparing((left, right) -> Integer.compareUnsigned(left.proofKeyVersion(),
                        right.proofKeyVersion()));
    }

    private static int compare(final SourcePosition left, final SourcePosition right) {
        try {
            final int order = left.compareTo(right);
            if (order == 0 && !Bytes.constantTimeEquals(left.canonicalBytes(), right.canonicalBytes())) {
                throw new IllegalArgumentException(
                        "trust-set marker source position has conflicting canonical identity");
            }
            return order;
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("trust-set markers use different source identities", exception);
        }
    }

    private static int compareBytes(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int result = Byte.toUnsignedInt(left[index]) - Byte.toUnsignedInt(right[index]);
            if (result != 0) {
                return result;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    public record ActivationMarker(PayloadProofTrustSetRefV1 trustSet, SourcePosition sourcePosition) {
        public ActivationMarker {
            Objects.requireNonNull(trustSet, "trustSet");
            Objects.requireNonNull(sourcePosition, "sourcePosition");
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, trustSet.canonicalBytes());
                CanonicalProtobuf.bytes(output, 2, sourcePosition.canonicalBytes());
            });
        }

        private static ActivationMarker decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                    "PayloadProofTrustSetActivationMarker");
            QueryCodecSupport.requireNumbers(fields, new int[]{1, 2},
                    "PayloadProofTrustSetActivationMarker");
            final ActivationMarker result = new ActivationMarker(
                    PayloadProofTrustSetRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                    decodePosition(fields.get(1)));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(),
                    "PayloadProofTrustSetActivationMarker");
            return result;
        }
    }

    public record IssuanceClosure(PayloadProofTrustSetRefV1 trustSet, int proofKeyVersion,
                                  SourcePosition sourcePosition, ControlReasonV1 reason) {
        public IssuanceClosure {
            Objects.requireNonNull(trustSet, "trustSet");
            if (proofKeyVersion == 0) {
                throw new IllegalArgumentException("proofKeyVersion must be a non-zero uint32");
            }
            Objects.requireNonNull(sourcePosition, "sourcePosition");
            Objects.requireNonNull(reason, "reason");
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, trustSet.canonicalBytes());
                CanonicalProtobuf.uint32Bits(output, 2, proofKeyVersion);
                CanonicalProtobuf.bytes(output, 3, sourcePosition.canonicalBytes());
                CanonicalProtobuf.bytes(output, 4, reason.canonicalBytes());
            });
        }

        private static IssuanceClosure decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                    "PayloadProofTrustSetIssuanceClosure");
            QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4},
                    "PayloadProofTrustSetIssuanceClosure");
            final IssuanceClosure result = new IssuanceClosure(
                    PayloadProofTrustSetRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                    QueryCodecSupport.uint32Bits(fields.get(1), 2), decodePosition(fields.get(2)),
                    ControlReasonV1.decode(QueryCodecSupport.nested(fields.get(3), 4)));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(),
                    "PayloadProofTrustSetIssuanceClosure");
            return result;
        }
    }

    private static SourcePosition decodePosition(final CanonicalProtobuf.Reader.Field field) {
        final byte[] encoded = QueryCodecSupport.bytes(field, field.number());
        final SourcePosition result = SourcePositionCodec.decode(encoded);
        if (!Arrays.equals(encoded, result.canonicalBytes())) {
            throw new IllegalArgumentException("source position is not canonical");
        }
        return result;
    }

    private static List<CanonicalProtobuf.Reader.Field> readRepeated(final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException(name + " is empty");
        }
        return fields;
    }
}
