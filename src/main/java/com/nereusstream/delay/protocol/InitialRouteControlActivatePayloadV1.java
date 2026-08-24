package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** ControlPayload field 14: the immutable initial control set for a Route. */
public final class InitialRouteControlActivatePayloadV1 {
    private static final int HASH_LENGTH = 32;
    private static final int MAX_TUPLES = 32;
    private static final int MAX_PROFILES = 4096;
    private static final int MAX_CANONICAL_BYTES = 1 << 20;

    private final List<ProtocolTupleV1> protocolTuples;
    private final List<ProfileRefV1> profiles;
    private final QuotaGrantRefV1 initialQuotaGrant;
    private final byte[] initialControlSnapshotHash;

    public InitialRouteControlActivatePayloadV1(
            final List<ProtocolTupleV1> protocolTuples,
            final List<ProfileRefV1> profiles,
            final QuotaGrantRefV1 initialQuotaGrant,
            final byte[] initialControlSnapshotHash) {
        this.protocolTuples = sortedTuples(protocolTuples);
        this.profiles = sortedProfiles(profiles);
        this.initialQuotaGrant = Objects.requireNonNull(initialQuotaGrant, "initialQuotaGrant");
        Bytes.requireLength(initialControlSnapshotHash, HASH_LENGTH, "initialControlSnapshotHash");
        this.initialControlSnapshotHash = Bytes.copy(initialControlSnapshotHash);
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

    public byte[] initialControlSnapshotHash() {
        return Bytes.copy(initialControlSnapshotHash);
    }

    public byte[] canonicalBytes() {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            for (ProtocolTupleV1 tuple : protocolTuples) {
                CanonicalProtobuf.bytes(output, 1, tuple.canonicalBytes());
            }
            for (ProfileRefV1 profile : profiles) {
                CanonicalProtobuf.bytes(output, 2, profile.canonicalBytes());
            }
            CanonicalProtobuf.bytes(output, 3, initialQuotaGrant.canonicalBytes());
            CanonicalProtobuf.bytes(output, 4, initialControlSnapshotHash);
        });
        if (encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("initial route control activation is too large");
        }
        return encoded;
    }

    public static InitialRouteControlActivatePayloadV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "InitialRouteControlActivatePayloadV1", true);
        final List<ProtocolTupleV1> tuples = new ArrayList<>();
        final List<ProfileRefV1> profiles = new ArrayList<>();
        int index = 0;
        while (index < fields.size() && fields.get(index).number() == 1) {
            tuples.add(ProtocolTupleV1.decode(QueryCodecSupport.nested(fields.get(index++), 1)));
        }
        while (index < fields.size() && fields.get(index).number() == 2) {
            profiles.add(ProfileRefV1.decode(QueryCodecSupport.nested(fields.get(index++), 2)));
        }
        if (index + 2 != fields.size()
                || fields.get(index).number() != 3
                || fields.get(index + 1).number() != 4) {
            throw new IllegalArgumentException(
                    "InitialRouteControlActivatePayloadV1 fields are incomplete or out of order");
        }
        final InitialRouteControlActivatePayloadV1 result = new InitialRouteControlActivatePayloadV1(
                tuples,
                profiles,
                QuotaGrantRefV1.decode(QueryCodecSupport.nested(fields.get(index), 3)),
                QueryCodecSupport.fixed(fields.get(index + 1), 4, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "InitialRouteControlActivatePayloadV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof InitialRouteControlActivatePayloadV1 that
                && protocolTuples.equals(that.protocolTuples)
                && profiles.equals(that.profiles)
                && initialQuotaGrant.equals(that.initialQuotaGrant)
                && Arrays.equals(initialControlSnapshotHash, that.initialControlSnapshotHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(protocolTuples, profiles, initialQuotaGrant, Arrays.hashCode(initialControlSnapshotHash));
    }

    private static List<ProtocolTupleV1> sortedTuples(final List<ProtocolTupleV1> values) {
        Objects.requireNonNull(values, "protocolTuples");
        if (values.size() > MAX_TUPLES) {
            throw new IllegalArgumentException("too many initial route protocol tuples");
        }
        final List<ProtocolTupleV1> result = new ArrayList<>(values);
        result.sort(Comparator.comparing(
                ProtocolTupleV1::canonicalBytes, InitialRouteControlActivatePayloadV1::compareBytes));
        for (int index = 1; index < result.size(); index++) {
            if (Arrays.equals(
                    result.get(index - 1).canonicalBytes(), result.get(index).canonicalBytes())) {
                throw new IllegalArgumentException("duplicate initial route protocol tuple");
            }
        }
        return List.copyOf(result);
    }

    private static List<ProfileRefV1> sortedProfiles(final List<ProfileRefV1> values) {
        Objects.requireNonNull(values, "profiles");
        if (values.size() > MAX_PROFILES) {
            throw new IllegalArgumentException("too many initial route profiles");
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
                throw new IllegalArgumentException("duplicate initial route profile");
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
