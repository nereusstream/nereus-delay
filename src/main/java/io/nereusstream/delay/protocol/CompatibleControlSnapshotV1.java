package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * Canonical local projection of the complete route-control snapshot required
 * before a shard can be activated for commands.
 *
 * <p>The bytes are an authenticated input from the control authority. This
 * class only verifies the closed representation and binds it to one shard;
 * it does not make a local copy of the snapshot an Oxia authority.</p>
 */
public final class CompatibleControlSnapshotV1 {
    public static final int VERSION = 1;
    public static final int DIGEST_LENGTH = 32;
    private static final int MAX_TUPLES = 32;
    private static final int MAX_PROFILES = 4096;
    private static final int MAX_CANONICAL_BYTES = 1 << 20;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-compatible-control-snapshot-v1\0");

    private final ShardSubjectV1 shard;
    private final List<ProtocolTupleV1> protocolTuples;
    private final List<ProfileRefV1> profiles;
    private final QuotaGrantRefV1 initialQuotaGrant;
    private final byte[] snapshotDigest;

    public CompatibleControlSnapshotV1(final ShardSubjectV1 shard, final List<ProtocolTupleV1> protocolTuples,
                                       final List<ProfileRefV1> profiles,
                                       final QuotaGrantRefV1 initialQuotaGrant) {
        this(shard, protocolTuples, profiles, initialQuotaGrant, null);
    }

    private CompatibleControlSnapshotV1(final ShardSubjectV1 shard, final List<ProtocolTupleV1> protocolTuples,
                                         final List<ProfileRefV1> profiles,
                                         final QuotaGrantRefV1 initialQuotaGrant, final byte[] snapshotDigest) {
        this.shard = Objects.requireNonNull(shard, "shard");
        this.protocolTuples = sortedTuples(protocolTuples);
        this.profiles = sortedProfiles(profiles);
        this.initialQuotaGrant = Objects.requireNonNull(initialQuotaGrant, "initialQuotaGrant");
        final byte[] expected = digest(fieldsOneToFive());
        if (snapshotDigest != null && !Bytes.constantTimeEquals(snapshotDigest, expected)) {
            throw new IllegalArgumentException("compatible control snapshot digest mismatch");
        }
        this.snapshotDigest = expected;
    }

    public ShardSubjectV1 shard() {
        return shard;
    }

    public List<ProtocolTupleV1> protocolTuples() {
        return protocolTuples;
    }

    public List<ProfileRefV1> profiles() {
        return profiles;
    }

    public QuotaGrantRefV1 initialQuotaGrant() {
        return initialQuotaGrant;
    }

    public byte[] snapshotDigest() {
        return Bytes.copy(snapshotDigest);
    }

    public byte[] canonicalBytes() {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToFive());
            CanonicalProtobuf.bytes(output, 6, snapshotDigest);
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("compatible control snapshot is too large");
        }
        return encoded;
    }

    public static CompatibleControlSnapshotV1 decode(final byte[] encoded) {
        Objects.requireNonNull(encoded, "encoded");
        if (encoded.length == 0 || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("invalid compatible control snapshot length");
        }
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded, true);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() < 5 || fields.get(0).number() != 1 || fields.get(1).number() != 2) {
            throw new IllegalArgumentException("compatible control snapshot is missing required fields");
        }
        if (QueryCodecSupport.uint(fields.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported compatible control snapshot version");
        }
        final ShardSubjectV1 shard = ShardSubjectV1.decode(QueryCodecSupport.nested(fields.get(1), 2));
        final List<ProtocolTupleV1> tuples = new ArrayList<>();
        final List<ProfileRefV1> profiles = new ArrayList<>();
        int index = 2;
        while (index < fields.size() && fields.get(index).number() == 3) {
            tuples.add(ProtocolTupleV1.decode(QueryCodecSupport.nested(fields.get(index++), 3)));
        }
        while (index < fields.size() && fields.get(index).number() == 4) {
            profiles.add(ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(index++), 4)));
        }
        if (index + 2 != fields.size() || fields.get(index).number() != 5
                || fields.get(index + 1).number() != 6) {
            throw new IllegalArgumentException("compatible control snapshot fields are incomplete or out of order");
        }
        final QuotaGrantRefV1 grant = QuotaGrantRefV1.decode(QueryCodecSupport.nested(fields.get(index), 5));
        final byte[] digest = QueryCodecSupport.fixed(fields.get(index + 1), 6, DIGEST_LENGTH);
        final CompatibleControlSnapshotV1 result = new CompatibleControlSnapshotV1(shard, tuples, profiles, grant,
                digest);
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "CompatibleControlSnapshotV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof CompatibleControlSnapshotV1 that
                && shard.equals(that.shard)
                && protocolTuples.equals(that.protocolTuples)
                && profiles.equals(that.profiles)
                && initialQuotaGrant.equals(that.initialQuotaGrant)
                && Arrays.equals(snapshotDigest, that.snapshotDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shard, protocolTuples, profiles, initialQuotaGrant, Arrays.hashCode(snapshotDigest));
    }

    private byte[] fieldsOneToFive() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, shard.canonicalBytes());
            for (ProtocolTupleV1 tuple : protocolTuples) {
                CanonicalProtobuf.bytes(output, 3, tuple.canonicalBytes());
            }
            for (ProfileRefV1 profile : profiles) {
                CanonicalProtobuf.bytes(output, 4, profile.canonicalBytes());
            }
            CanonicalProtobuf.bytes(output, 5, initialQuotaGrant.canonicalBytes());
        });
    }

    private static byte[] digest(final byte[] fields) {
        return Bytes.sha256(DIGEST_DOMAIN, fields);
    }

    private static List<ProtocolTupleV1> sortedTuples(final List<ProtocolTupleV1> values) {
        Objects.requireNonNull(values, "protocolTuples");
        if (values.isEmpty() || values.size() > MAX_TUPLES) {
            throw new IllegalArgumentException("compatible control snapshot must contain bounded protocol tuples");
        }
        final List<ProtocolTupleV1> result = new ArrayList<>(values);
        result.sort(Comparator.comparing(ProtocolTupleV1::canonicalBytes, CompatibleControlSnapshotV1::compareBytes));
        for (int index = 1; index < result.size(); index++) {
            if (Arrays.equals(result.get(index - 1).canonicalBytes(), result.get(index).canonicalBytes())) {
                throw new IllegalArgumentException("duplicate protocol tuple");
            }
        }
        return List.copyOf(result);
    }

    private static List<ProfileRefV1> sortedProfiles(final List<ProfileRefV1> values) {
        Objects.requireNonNull(values, "profiles");
        if (values.size() > MAX_PROFILES) {
            throw new IllegalArgumentException("too many compatible control snapshot profiles");
        }
        final List<ProfileRefV1> result = new ArrayList<>(values);
        result.sort((left, right) -> {
            final int idOrder = compareBytes(left.profileId(), right.profileId());
            return idOrder != 0 ? idOrder : Long.compareUnsigned(left.version(), right.version());
        });
        for (int index = 1; index < result.size(); index++) {
            final ProfileRefV1 previous = result.get(index - 1);
            final ProfileRefV1 current = result.get(index);
            if (Arrays.equals(previous.profileId(), current.profileId()) && previous.version() == current.version()) {
                throw new IllegalArgumentException("duplicate compatible control snapshot profile");
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
}
