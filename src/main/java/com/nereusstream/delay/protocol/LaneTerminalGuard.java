package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Canonical terminal guard for an irreversibly retired Lane. This value is
 * independent of the current legacy LaneRecord storage adapter; callers must
 * still atomically replace the active LANE value at the same key.
 */
public final class LaneTerminalGuard {
    private static final int VERSION = 1;
    private static final int HASH_LENGTH = 32;
    private static final int INCARNATION_LENGTH = 16;
    private static final int MAX_TUPLE_BYTES = 1 << 20;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-lane-terminal-guard\0");

    private final DestinationLaneId laneId;
    private final byte[] laneIncarnation;
    private final long laneControlVersion;
    private final SourcePosition terminalSourcePosition;
    private final ProfileRef destinationProfile;
    private final ProfileRef capabilityProfile;
    private final byte[] canonicalLaneTuple;
    private final byte[] canonicalLaneTupleSha256;
    private final byte[] retirementIntentId;
    private final long retirementMutationSequence;
    private final byte[] guardDigest;

    public LaneTerminalGuard(
            final byte[] laneIncarnation,
            final long laneControlVersion,
            final SourcePosition terminalSourcePosition,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final byte[] canonicalLaneTuple,
            final byte[] retirementIntentId,
            final long retirementMutationSequence) {
        this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "laneIncarnation");
        if (laneControlVersion <= 0) {
            throw new IllegalArgumentException("laneControlVersion must be positive");
        }
        this.laneControlVersion = laneControlVersion;
        this.terminalSourcePosition = Objects.requireNonNull(terminalSourcePosition, "terminalSourcePosition");
        this.destinationProfile = requireProfile(destinationProfile, ProfileKind.DESTINATION, "destinationProfile");
        this.capabilityProfile =
                requireProfile(capabilityProfile, ProfileKind.DELIVERY_CAPABILITY, "capabilityProfile");
        this.canonicalLaneTuple = tuple(canonicalLaneTuple);
        CanonicalLaneTuple.requireProfileProjection(
                this.canonicalLaneTuple, this.destinationProfile, this.capabilityProfile);
        this.laneId = DestinationLaneId.derive(this.canonicalLaneTuple);
        this.canonicalLaneTupleSha256 = Bytes.sha256(this.canonicalLaneTuple);
        this.retirementIntentId = nonZero(retirementIntentId, "retirementIntentId");
        if (retirementMutationSequence == 0) {
            throw new IllegalArgumentException("retirementMutationSequence must be non-zero");
        }
        this.retirementMutationSequence = retirementMutationSequence;
        this.guardDigest = Bytes.sha256(DIGEST_DOMAIN, fieldsOneToTwelve());
    }

    private LaneTerminalGuard(
            final DestinationLaneId laneId,
            final byte[] laneIncarnation,
            final long laneControlVersion,
            final SourcePosition terminalSourcePosition,
            final ProfileRef destinationProfile,
            final ProfileRef capabilityProfile,
            final byte[] canonicalLaneTuple,
            final byte[] tupleDigest,
            final byte[] retirementIntentId,
            final long retirementMutationSequence,
            final byte[] guardDigest) {
        this.laneId = Objects.requireNonNull(laneId, "laneId");
        this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "laneIncarnation");
        if (laneControlVersion <= 0) {
            throw new IllegalArgumentException("laneControlVersion must be positive");
        }
        this.laneControlVersion = laneControlVersion;
        this.terminalSourcePosition = Objects.requireNonNull(terminalSourcePosition, "terminalSourcePosition");
        this.destinationProfile = requireProfile(destinationProfile, ProfileKind.DESTINATION, "destinationProfile");
        this.capabilityProfile =
                requireProfile(capabilityProfile, ProfileKind.DELIVERY_CAPABILITY, "capabilityProfile");
        this.canonicalLaneTuple = tuple(canonicalLaneTuple);
        CanonicalLaneTuple.requireProfileProjection(
                this.canonicalLaneTuple, this.destinationProfile, this.capabilityProfile);
        Bytes.requireLength(tupleDigest, HASH_LENGTH, "canonicalLaneTupleSha256");
        this.canonicalLaneTupleSha256 = Bytes.copy(tupleDigest);
        this.retirementIntentId = nonZero(retirementIntentId, "retirementIntentId");
        if (retirementMutationSequence == 0) {
            throw new IllegalArgumentException("retirementMutationSequence must be non-zero");
        }
        this.retirementMutationSequence = retirementMutationSequence;
        Bytes.requireLength(guardDigest, HASH_LENGTH, "guardDigest");
        this.guardDigest = Bytes.copy(guardDigest);
    }

    public DestinationLaneId laneId() {
        return laneId;
    }

    public byte[] laneIncarnation() {
        return Bytes.copy(laneIncarnation);
    }

    public long laneControlVersion() {
        return laneControlVersion;
    }

    public SourcePosition terminalSourcePosition() {
        return terminalSourcePosition;
    }

    public ProfileRef destinationProfile() {
        return destinationProfile;
    }

    public ProfileRef capabilityProfile() {
        return capabilityProfile;
    }

    public byte[] canonicalLaneTuple() {
        return Bytes.copy(canonicalLaneTuple);
    }

    public byte[] canonicalLaneTupleSha256() {
        return Bytes.copy(canonicalLaneTupleSha256);
    }

    public byte[] retirementIntentId() {
        return Bytes.copy(retirementIntentId);
    }

    public long retirementMutationSequence() {
        return retirementMutationSequence;
    }

    public byte[] guardDigest() {
        return Bytes.copy(guardDigest);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            output.writeBytes(fieldsOneToTwelve());
            CanonicalProtobuf.bytes(output, 13, guardDigest);
        });
    }

    public static LaneTerminalGuard decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "LaneTerminalGuard");
        QueryCodecSupport.requireNumbers(
                fields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13}, "LaneTerminalGuard");
        if (QueryCodecSupport.uint32(fields.get(0), 1) != VERSION || QueryCodecSupport.uint32(fields.get(3), 4) != 5) {
            throw new IllegalArgumentException("LaneTerminalGuard has invalid version or final gate");
        }
        final byte[] tuple = tuple(QueryCodecSupport.bytes(fields.get(8), 9));
        final DestinationLaneId laneId =
                new DestinationLaneId(QueryCodecSupport.fixed(fields.get(1), 2, DestinationLaneId.LENGTH));
        if (!laneId.equals(DestinationLaneId.derive(tuple))) {
            throw new IllegalArgumentException("LaneTerminalGuard lane identity does not match tuple");
        }
        final byte[] tupleDigest = QueryCodecSupport.fixed(fields.get(9), 10, HASH_LENGTH);
        if (!Bytes.constantTimeEquals(tupleDigest, Bytes.sha256(tuple))) {
            throw new IllegalArgumentException("LaneTerminalGuard tuple digest mismatch");
        }
        final LaneTerminalGuard result = new LaneTerminalGuard(
                laneId,
                QueryCodecSupport.fixed(fields.get(2), 3, INCARNATION_LENGTH),
                QueryCodecSupport.uint(fields.get(4), 5),
                QueryCodecSupport.decodeSourcePosition(QueryCodecSupport.nested(fields.get(5), 6)),
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(6), 7)),
                ProfileRef.decode(QueryCodecSupport.nested(fields.get(7), 8)),
                tuple,
                tupleDigest,
                QueryCodecSupport.fixed(fields.get(10), 11, HASH_LENGTH),
                QueryCodecSupport.uint64Bits(fields.get(11), 12),
                QueryCodecSupport.fixed(fields.get(12), 13, HASH_LENGTH));
        if (!Bytes.constantTimeEquals(result.guardDigest, Bytes.sha256(DIGEST_DOMAIN, result.fieldsOneToTwelve()))) {
            throw new IllegalArgumentException("LaneTerminalGuard guard digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "LaneTerminalGuard");
        return result;
    }

    private byte[] fieldsOneToTwelve() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, VERSION);
            CanonicalProtobuf.bytes(output, 2, laneId.bytes());
            CanonicalProtobuf.bytes(output, 3, laneIncarnation);
            CanonicalProtobuf.uint32(output, 4, 5);
            CanonicalProtobuf.uint64(output, 5, laneControlVersion);
            CanonicalProtobuf.bytes(output, 6, QueryCodecSupport.encodeSourcePosition(terminalSourcePosition));
            CanonicalProtobuf.bytes(output, 7, destinationProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 8, capabilityProfile.canonicalBytes());
            CanonicalProtobuf.bytes(output, 9, canonicalLaneTuple);
            CanonicalProtobuf.bytes(output, 10, canonicalLaneTupleSha256);
            CanonicalProtobuf.bytes(output, 11, retirementIntentId);
            CanonicalProtobuf.uint64Bits(output, 12, retirementMutationSequence);
        });
    }

    private static byte[] tuple(final byte[] value) {
        Objects.requireNonNull(value, "canonicalLaneTuple");
        if (value.length == 0 || value.length > MAX_TUPLE_BYTES) {
            throw new IllegalArgumentException("canonicalLaneTuple has invalid length");
        }
        return Bytes.copy(value);
    }

    private static ProfileRef requireProfile(final ProfileRef value, final ProfileKind expected, final String name) {
        ProfileRef result = Objects.requireNonNull(value, name);
        if (result.profileKind() != expected) {
            throw new IllegalArgumentException(name + " has wrong ProfileKind");
        }
        return result;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        final byte[] result = fixed(value, HASH_LENGTH, name);
        boolean nonZero = false;
        for (byte current : result) {
            nonZero |= current != 0;
        }
        if (!nonZero) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof LaneTerminalGuard that
                && laneControlVersion == that.laneControlVersion
                && retirementMutationSequence == that.retirementMutationSequence
                && laneId.equals(that.laneId)
                && Arrays.equals(laneIncarnation, that.laneIncarnation)
                && Objects.equals(terminalSourcePosition, that.terminalSourcePosition)
                && destinationProfile.equals(that.destinationProfile)
                && capabilityProfile.equals(that.capabilityProfile)
                && Arrays.equals(canonicalLaneTuple, that.canonicalLaneTuple)
                && Arrays.equals(canonicalLaneTupleSha256, that.canonicalLaneTupleSha256)
                && Arrays.equals(retirementIntentId, that.retirementIntentId)
                && Arrays.equals(guardDigest, that.guardDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                laneId,
                Arrays.hashCode(laneIncarnation),
                laneControlVersion,
                terminalSourcePosition,
                destinationProfile,
                capabilityProfile,
                Arrays.hashCode(canonicalLaneTuple),
                Arrays.hashCode(canonicalLaneTupleSha256),
                Arrays.hashCode(retirementIntentId),
                retirementMutationSequence,
                Arrays.hashCode(guardDigest));
    }
}
