package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Durable source-ordered projection of Protocol Version activation markers.
 *
 * <p>The projection records the exact marker evidence that was applied by a
 * shard. It is not an Oxia Route or Worker capability authority; those
 * authorities are represented by the authenticated snapshot and the marker's
 * compatible-reader-set evidence hash.</p>
 */
public final class ProtocolActivationState {
    public static final int VERSION = 1;
    public static final int DIGEST_LENGTH = 32;
    private static final int MAX_ACTIVATIONS = 32;
    private static final int MAX_CANONICAL_BYTES = 1 << 20;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-protocol-activation-state\0");

    private final ShardSubject shard;
    private final List<Activation> activations;
    private final byte[] stateDigest;

    public ProtocolActivationState(final ShardSubject shard, final List<Activation> activations) {
        this(shard, activations, null);
    }

    private ProtocolActivationState(
            final ShardSubject shard, final List<Activation> activations, final byte[] stateDigest) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.activations = sortedActivations(activations);
        final byte[] expected = digest(fieldsOneToThree());
        if (stateDigest != null && !Bytes.constantTimeEquals(stateDigest, expected)) {
            throw new IllegalArgumentException("protocol activation state digest mismatch");
        }
        this.stateDigest = expected;
    }

    public ShardSubject shard() {
        return shard;
    }

    public List<Activation> activations() {
        return activations;
    }

    public byte[] stateDigest() {
        return Bytes.copy(stateDigest);
    }

    public boolean isMarkedActivated(final ProtocolTuple tuple) {
        Objects.requireNonNull(tuple, "tuple");
        return activation(tuple) != null;
    }

    public Activation activation(final ProtocolTuple tuple) {
        Objects.requireNonNull(tuple, "tuple");
        for (Activation activation : activations) {
            if (activation.tuple().equals(tuple)) {
                return activation;
            }
        }
        return null;
    }

    /** Returns a new state after one first-seen source-ordered marker. */
    public ProtocolActivationState activate(
            final ProtocolTuple tuple,
            final byte[] canonicalSchemaHash,
            final byte[] compatibleReaderSetEvidenceHash,
            final SourcePosition sourcePosition,
            final byte[] mutationId) {
        Objects.requireNonNull(tuple, "tuple");
        Objects.requireNonNull(sourcePosition, "sourcePosition");
        if (!shard.shardId().equals(sourcePosition.shardId())) {
            throw new IllegalArgumentException("protocol activation source position belongs to another shard");
        }
        if (activation(tuple) != null) {
            throw new IllegalArgumentException("protocol tuple is already marked activated");
        }
        final List<Activation> next = new ArrayList<>(activations);
        next.add(new Activation(
                tuple,
                canonicalSchemaHash,
                compatibleReaderSetEvidenceHash,
                sourcePosition.canonicalBytes(),
                mutationId));
        return new ProtocolActivationState(shard, next);
    }

    public byte[] canonicalBytes() {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, shard.canonicalBytes());
            for (Activation activation : activations) {
                CanonicalProtobuf.bytes(output, 3, activation.canonicalBytes());
            }
            CanonicalProtobuf.bytes(output, 4, stateDigest);
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("protocol activation state is too large");
        }
        return encoded;
    }

    public static ProtocolActivationState decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length == 0 || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("invalid protocol activation state length");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ProtocolActivationState");
        if (fields.size() < 3 || fields.get(0).number() != 1 || fields.get(1).number() != 2) {
            throw new IllegalArgumentException("protocol activation state is missing required fields");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported protocol activation state version");
        }
        final ShardSubject shard = ShardSubject.decode(QueryCodecSupport.nested(fields.get(1), 2));
        final List<Activation> activations = new ArrayList<>();
        int index = 2;
        while (index < fields.size() - 1 && fields.get(index).number() == 3) {
            activations.add(Activation.decode(QueryCodecSupport.nested(fields.get(index++), 3)));
        }
        if (index + 1 != fields.size() || fields.get(index).number() != 4) {
            throw new IllegalArgumentException("protocol activation state fields are incomplete or out of order");
        }
        final byte[] stateDigest = QueryCodecSupport.fixed(fields.get(index), 4, DIGEST_LENGTH);
        final ProtocolActivationState result = new ProtocolActivationState(shard, activations, stateDigest);
        for (Activation activation : result.activations) {
            if (!shard.shardId().equals(activation.sourcePosition().shardId())) {
                throw new IllegalArgumentException("protocol activation source position belongs to another shard");
            }
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ProtocolActivationState");
        return result;
    }

    private byte[] fieldsOneToThree() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, shard.canonicalBytes());
            for (Activation activation : activations) {
                CanonicalProtobuf.bytes(output, 3, activation.canonicalBytes());
            }
        });
    }

    private static byte[] digest(final byte[] fields) {
        return Bytes.sha256(DIGEST_DOMAIN, fields);
    }

    private static List<Activation> sortedActivations(final List<Activation> values) {
        Objects.requireNonNull(values, "activations");
        if (values.size() > MAX_ACTIVATIONS) {
            throw new IllegalArgumentException("too many protocol activation markers");
        }
        final List<Activation> result = new ArrayList<>(values);
        result.sort(Comparator.comparing(
                activation -> activation.tuple().canonicalBytes(), ProtocolActivationState::compareBytes));
        for (int index = 1; index < result.size(); index++) {
            if (result.get(index - 1).tuple().equals(result.get(index).tuple())) {
                throw new IllegalArgumentException("duplicate protocol activation tuple");
            }
        }
        return List.copyOf(result);
    }

    private static int compareBytes(final byte[] left, final byte[] right) {
        final int length = Math.min(left.length, right.length);
        for (int index = 0; index < length; index++) {
            final int comparison = Byte.toUnsignedInt(left[index]) - Byte.toUnsignedInt(right[index]);
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(left.length, right.length);
    }

    /** Exact evidence attached to one authenticated activation marker. */
    public static final class Activation {
        private final ProtocolTuple tuple;
        private final byte[] canonicalSchemaHash;
        private final byte[] compatibleReaderSetEvidenceHash;
        private final byte[] sourcePosition;
        private final byte[] mutationId;

        public Activation(
                final ProtocolTuple tuple,
                final byte[] canonicalSchemaHash,
                final byte[] compatibleReaderSetEvidenceHash,
                final byte[] sourcePosition,
                final byte[] mutationId) {
            this.tuple = Objects.requireNonNull(tuple, "tuple");
            this.canonicalSchemaHash = fixed(canonicalSchemaHash, "canonicalSchemaHash");
            this.compatibleReaderSetEvidenceHash =
                    fixed(compatibleReaderSetEvidenceHash, "compatibleReaderSetEvidenceHash");
            this.sourcePosition = SourcePositionCodec.decode(Objects.requireNonNull(sourcePosition, "sourcePosition"))
                    .canonicalBytes();
            this.mutationId = fixed(mutationId, "mutationId");
        }

        public ProtocolTuple tuple() {
            return tuple;
        }

        public byte[] canonicalSchemaHash() {
            return Bytes.copy(canonicalSchemaHash);
        }

        public byte[] compatibleReaderSetEvidenceHash() {
            return Bytes.copy(compatibleReaderSetEvidenceHash);
        }

        public SourcePosition sourcePosition() {
            return SourcePositionCodec.decode(sourcePosition);
        }

        public byte[] mutationId() {
            return Bytes.copy(mutationId);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(output -> {
                CanonicalProtobuf.bytes(output, 1, tuple.canonicalBytes());
                CanonicalProtobuf.bytes(output, 2, canonicalSchemaHash);
                CanonicalProtobuf.bytes(output, 3, compatibleReaderSetEvidenceHash);
                CanonicalProtobuf.bytes(output, 4, sourcePosition);
                CanonicalProtobuf.bytes(output, 5, mutationId);
            });
        }

        public static Activation decode(final byte[] encoded) {
            final List<CanonicalProtobuf.Reader.Field> fields =
                    QueryCodecSupport.read(encoded, "ProtocolActivationState.Activation");
            QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3, 4, 5}, "ProtocolActivationState.Activation");
            return new Activation(
                    ProtocolTuple.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                    QueryCodecSupport.fixed(fields.get(1), 2, DIGEST_LENGTH),
                    QueryCodecSupport.fixed(fields.get(2), 3, DIGEST_LENGTH),
                    QueryCodecSupport.nested(fields.get(3), 4),
                    QueryCodecSupport.fixed(fields.get(4), 5, DIGEST_LENGTH));
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Activation that
                    && tuple.equals(that.tuple)
                    && Arrays.equals(canonicalSchemaHash, that.canonicalSchemaHash)
                    && Arrays.equals(compatibleReaderSetEvidenceHash, that.compatibleReaderSetEvidenceHash)
                    && Arrays.equals(sourcePosition, that.sourcePosition)
                    && Arrays.equals(mutationId, that.mutationId);
        }

        @Override
        public int hashCode() {
            int result = tuple.hashCode();
            result = 31 * result + Arrays.hashCode(canonicalSchemaHash);
            result = 31 * result + Arrays.hashCode(compatibleReaderSetEvidenceHash);
            result = 31 * result + Arrays.hashCode(sourcePosition);
            result = 31 * result + Arrays.hashCode(mutationId);
            return result;
        }

        private static byte[] fixed(final byte[] value, final String name) {
            Bytes.requireLength(value, DIGEST_LENGTH, name);
            return Bytes.copy(value);
        }
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ProtocolActivationState that
                && shard.equals(that.shard)
                && activations.equals(that.activations)
                && Arrays.equals(stateDigest, that.stateDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shard, activations, Arrays.hashCode(stateDigest));
    }
}
