package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** ControlPayload field 14: the immutable initial control set for a Route. */
public final class InitialRouteControlActivatePayload {
    private static final int HASH_LENGTH = 32;
    private static final int MAX_TUPLES = 32;
    private static final int MAX_PROFILES = 4096;
    private static final int MAX_CANONICAL_BYTES = 1 << 20;

    private final List<ProtocolTuple> protocolTuples;
    private final List<ProfileRef> profiles;
    private final QuotaGrantRef initialQuotaGrant;
    private final byte[] initialControlSnapshotHash;

    public InitialRouteControlActivatePayload(
            final List<ProtocolTuple> protocolTuples,
            final List<ProfileRef> profiles,
            final QuotaGrantRef initialQuotaGrant,
            final byte[] initialControlSnapshotHash) {
        this.protocolTuples = sortedTuples(protocolTuples);
        this.profiles = sortedProfiles(profiles);
        this.initialQuotaGrant = Objects.requireNonNull(initialQuotaGrant, "initialQuotaGrant");
        Bytes.requireLength(initialControlSnapshotHash, HASH_LENGTH, "initialControlSnapshotHash");
        this.initialControlSnapshotHash = Bytes.copy(initialControlSnapshotHash);
    }

    public List<ProtocolTuple> protocolTuples() {
        return protocolTuples;
    }

    public List<ProfileRef> profiles() {
        return profiles;
    }

    public QuotaGrantRef initialQuotaGrant() {
        return initialQuotaGrant;
    }

    public byte[] initialControlSnapshotHash() {
        return Bytes.copy(initialControlSnapshotHash);
    }

    public byte[] canonicalBytes() {
        final byte[] encoded = CanonicalProtobuf.message(output -> {
            for (ProtocolTuple tuple : protocolTuples) {
                CanonicalProtobuf.bytes(output, 1, tuple.canonicalBytes());
            }
            for (ProfileRef profile : profiles) {
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

    public static InitialRouteControlActivatePayload decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "InitialRouteControlActivatePayload", true);
        final List<ProtocolTuple> tuples = new ArrayList<>();
        final List<ProfileRef> profiles = new ArrayList<>();
        int index = 0;
        while (index < fields.size() && fields.get(index).number() == 1) {
            tuples.add(ProtocolTuple.decode(QueryCodecSupport.nested(fields.get(index++), 1)));
        }
        while (index < fields.size() && fields.get(index).number() == 2) {
            profiles.add(ProfileRef.decode(QueryCodecSupport.nested(fields.get(index++), 2)));
        }
        if (index + 2 != fields.size()
                || fields.get(index).number() != 3
                || fields.get(index + 1).number() != 4) {
            throw new IllegalArgumentException(
                    "InitialRouteControlActivatePayload fields are incomplete or out of order");
        }
        final InitialRouteControlActivatePayload result = new InitialRouteControlActivatePayload(
                tuples,
                profiles,
                QuotaGrantRef.decode(QueryCodecSupport.nested(fields.get(index), 3)),
                QueryCodecSupport.fixed(fields.get(index + 1), 4, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "InitialRouteControlActivatePayload");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof InitialRouteControlActivatePayload that
                && protocolTuples.equals(that.protocolTuples)
                && profiles.equals(that.profiles)
                && initialQuotaGrant.equals(that.initialQuotaGrant)
                && Arrays.equals(initialControlSnapshotHash, that.initialControlSnapshotHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(protocolTuples, profiles, initialQuotaGrant, Arrays.hashCode(initialControlSnapshotHash));
    }

    private static List<ProtocolTuple> sortedTuples(final List<ProtocolTuple> values) {
        Objects.requireNonNull(values, "protocolTuples");
        if (values.size() > MAX_TUPLES) {
            throw new IllegalArgumentException("too many initial route protocol tuples");
        }
        final List<ProtocolTuple> result = new ArrayList<>(values);
        result.sort(
                Comparator.comparing(ProtocolTuple::canonicalBytes, InitialRouteControlActivatePayload::compareBytes));
        for (int index = 1; index < result.size(); index++) {
            if (Arrays.equals(
                    result.get(index - 1).canonicalBytes(), result.get(index).canonicalBytes())) {
                throw new IllegalArgumentException("duplicate initial route protocol tuple");
            }
        }
        return List.copyOf(result);
    }

    private static List<ProfileRef> sortedProfiles(final List<ProfileRef> values) {
        Objects.requireNonNull(values, "profiles");
        if (values.size() > MAX_PROFILES) {
            throw new IllegalArgumentException("too many initial route profiles");
        }
        final List<ProfileRef> result = new ArrayList<>(values);
        result.sort((left, right) -> {
            final int idOrder = compareBytes(left.profileId(), right.profileId());
            return idOrder != 0 ? idOrder : Long.compareUnsigned(left.version(), right.version());
        });
        for (int index = 1; index < result.size(); index++) {
            final ProfileRef previous = result.get(index - 1);
            final ProfileRef current = result.get(index);
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
