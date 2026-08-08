package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Semantic parser for the replay-stable subset of a CLAIM_RESULT_V1 body.
 *
 * <p>The generic system-body codec validates the operation field numbers and
 * wire types.  This parser validates the permanent-result contract: the
 * result kind/code, the duplicated Claim identity, the complete
 * {@code ClaimPreconditionV1}, Trusted UTC evidence, and the charge transfer.
 * A Claim Result is allowed to omit materialization only when the failure
 * happened before complete materialization, as specified by the registry.</p>
 */
public final class ClaimResultBody {
    private static final int HASH_LENGTH = 32;
    private static final int INCARNATION_LENGTH = 16;
    private static final int MESSAGE_ID_LENGTH = DelayMessageId.LENGTH;

    private final byte[] claimId;
    private final byte[] messageId;
    private final int generation;
    private final byte[] laneId;
    private final byte[] laneIncarnation;
    private final ClaimPrecondition precondition;
    private final int resultKind;
    private final StableCode stableCode;
    private final TrustedUtcIntervalEvidence observedAt;
    private final byte[] transfer;

    private ClaimResultBody(final byte[] claimId, final byte[] messageId, final int generation,
                            final byte[] laneId, final byte[] laneIncarnation,
                            final ClaimPrecondition precondition, final int resultKind,
                            final StableCode stableCode, final TrustedUtcIntervalEvidence observedAt,
                            final byte[] transfer) {
        this.claimId = fixed(claimId, HASH_LENGTH, "claimId");
        this.messageId = fixed(messageId, MESSAGE_ID_LENGTH, "delayMessageId");
        this.generation = generation;
        this.laneId = fixed(laneId, HASH_LENGTH, "destinationLaneId");
        this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "laneIncarnation");
        this.precondition = Objects.requireNonNull(precondition, "precondition");
        this.resultKind = resultKind;
        this.stableCode = Objects.requireNonNull(stableCode, "stableCode");
        this.observedAt = Objects.requireNonNull(observedAt, "observedAt");
        this.transfer = copy(transfer);
        if (resultKind != 1 || stableCode != StableCode.CLAIM_PERMANENT_FAILURE) {
            throw new IllegalArgumentException("unsupported Claim Result kind/code");
        }
        if (!Arrays.equals(this.claimId, precondition.claimId())
                || !Arrays.equals(this.messageId, precondition.messageId())
                || generation != precondition.generation()
                || !Arrays.equals(this.laneId, precondition.destinationLaneId())
                || !Arrays.equals(this.laneIncarnation, precondition.laneIncarnation())) {
            throw new IllegalArgumentException("Claim Result identity does not match Claim precondition");
        }
        // A permanent pre-send result releases the reversible Claim reservation.  The
        // transfer is therefore the same canonical ChargeVector that the Claim froze;
        // accepting a different projection would let a signed callback rewrite quota
        // accounting while the source-ordered apply path has no independent authority.
        if (!Arrays.equals(this.transfer, precondition.claimedCharge())) {
            throw new IllegalArgumentException("Claim Result charge transfer does not match claimed charge");
        }
    }

    /** Parses and validates a canonical CLAIM_RESULT_V1 body. */
    public static ClaimResultBody decode(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.CLAIM_RESULT, canonicalBody);
        final byte[] claimId = fixed(field(fields, 10), 10, HASH_LENGTH);
        final byte[] messageId = fixed(field(fields, 11), 11, MESSAGE_ID_LENGTH);
        final int generation = intValue(field(fields, 12), 12);
        final byte[] laneId = fixed(field(fields, 13), 13, HASH_LENGTH);
        final byte[] laneIncarnation = fixed(field(fields, 14), 14, INCARNATION_LENGTH);
        final ClaimPrecondition precondition = decodePrecondition(nested(field(fields, 15), 15));
        final int resultKind = intValue(field(fields, 16), 16);
        final StableCode stableCode = StableCode.fromWire(intValue(field(fields, 17), 17));
        final TrustedUtcIntervalEvidence observedAt = TrustedUtcIntervalEvidence.decode(
                nested(field(fields, 18), 18));
        final byte[] transfer = nested(field(fields, 20), 20);
        validateChargeVector(transfer);
        SystemMutationBodyCodec.requireMessageShard(fields, new DelayMessageId(messageId), "Claim Result");
        return new ClaimResultBody(claimId, messageId, generation, laneId, laneIncarnation, precondition,
                resultKind, stableCode, observedAt, transfer);
    }

    public byte[] claimId() {
        return copy(claimId);
    }

    public byte[] messageId() {
        return copy(messageId);
    }

    public int generation() {
        return generation;
    }

    public byte[] laneId() {
        return copy(laneId);
    }

    public byte[] laneIncarnation() {
        return copy(laneIncarnation);
    }

    public ClaimPrecondition precondition() {
        return precondition;
    }

    public int resultKind() {
        return resultKind;
    }

    public StableCode stableCode() {
        return stableCode;
    }

    public TrustedUtcIntervalEvidence observedAt() {
        return observedAt;
    }

    public byte[] transfer() {
        return copy(transfer);
    }

    /** Decodes the canonical ClaimPreconditionV1 nested value for local Claim persistence. */
    public static ClaimPrecondition decodePrecondition(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ClaimPrecondition");
        if (fields.size() < 18 || fields.size() > 20) {
            throw new IllegalArgumentException("ClaimPrecondition has unexpected field count");
        }
        for (int number = 1; number <= 9; number++) {
            field(fields, number);
        }
        for (int number = 12; number <= 20; number++) {
            field(fields, number);
        }
        final boolean hasMaterialization = has(fields, 10);
        if (hasMaterialization != has(fields, 11)) {
            throw new IllegalArgumentException("Claim materialization fields must be paired");
        }
        final byte[] materialization = hasMaterialization ? nested(field(fields, 10), 10) : new byte[0];
        if (hasMaterialization) {
            validateMaterialization(materialization);
            final byte[] materializationDigest = fixed(field(fields, 11), 11, HASH_LENGTH);
            final byte[] expected = Bytes.sha256(Bytes.utf8("nereus-delay-claim-materialization-v1\0"),
                    materialization);
            if (!Arrays.equals(materializationDigest, expected)) {
                throw new IllegalArgumentException("Claim materialization digest mismatch");
            }
        }
        final byte[] owner = nested(field(fields, 14), 14);
        AuthorIdentity.decode(owner).requireFor(SystemMutationType.CLAIM_RESULT);
        final int sourceWorkKind = intValue(field(fields, 16), 16);
        if (sourceWorkKind < 1 || sourceWorkKind > 3) {
            throw new IllegalArgumentException("invalid Claim source work kind");
        }
        validateChargeVector(nested(field(fields, 12), 12));
        final ClaimPrecondition result = new ClaimPrecondition(encoded, fixed(field(fields, 1), 1, HASH_LENGTH),
                fixed(field(fields, 2), 2, MESSAGE_ID_LENGTH), intValue(field(fields, 3), 3),
                bodyUnsigned(field(fields, 4), 4), fixed(field(fields, 5), 5, HASH_LENGTH),
                fixed(field(fields, 6), 6, INCARNATION_LENGTH), bodyUnsigned(field(fields, 7), 7),
                bodyUnsigned(field(fields, 8), 8), fixed(field(fields, 9), 9, HASH_LENGTH), materialization,
                nested(field(fields, 12), 12), bodyUnsigned(field(fields, 13), 13), owner,
                fixed(field(fields, 15), 15, INCARNATION_LENGTH), sourceWorkKind,
                intValue(field(fields, 17), 17), intValue(field(fields, 18), 18),
                fixed(field(fields, 19), 19, HASH_LENGTH), fixed(field(fields, 20), 20, HASH_LENGTH));
        if (!Arrays.equals(encoded, canonical(fields))) {
            throw new IllegalArgumentException("non-canonical ClaimPrecondition");
        }
        return result;
    }

    private static void validateMaterialization(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ClaimMaterialization");
        requireExact(fields, 11, "ClaimMaterialization");
        validateProfileRef(nested(field(fields, 1), 1));
        validateProfileRef(nested(field(fields, 2), 2));
        validateBrokerResource(nested(field(fields, 3), 3));
        bodyUnsigned(field(fields, 4), 4);
        fixed(field(fields, 5), 5, MESSAGE_ID_LENGTH);
        intValue(field(fields, 6), 6);
        validatePayload(nested(field(fields, 7), 7));
        validateAdapterMetadata(nested(field(fields, 8), 8));
        bodyUnsigned(field(fields, 9), 9);
        bodyUnsigned(field(fields, 10), 10);
        bodyUnsigned(field(fields, 11), 11);
        if (!Arrays.equals(encoded, canonical(fields))) {
            throw new IllegalArgumentException("non-canonical ClaimMaterialization");
        }
    }

    private static void validateProfileRef(final byte[] encoded) {
        ProfileRefV1.decode(encoded);
    }

    private static void validateBrokerResource(final byte[] encoded) {
        BrokerResourceIdentityV1.decode(encoded);
    }

    private static void validatePayload(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "PayloadForPublish");
        if (fields.size() != 3 || !has(fields, 1) || !has(fields, 2)
                || has(fields, 3) == has(fields, 4)) {
            throw new IllegalArgumentException("invalid PayloadForPublish shape");
        }
        final long length = bodyUnsigned(field(fields, 1), 1);
        final byte[] hash = fixed(field(fields, 2), 2, HASH_LENGTH);
        if (has(fields, 3)) {
            final byte[] inline = bytes(field(fields, 3), 3);
            if (inline.length != length || !Arrays.equals(hash, Bytes.sha256(inline))) {
                throw new IllegalArgumentException("inline payload length/hash mismatch");
            }
        } else {
            final CommittedPayloadDescriptorV1 object = CommittedPayloadDescriptorV1.decode(
                    nested(field(fields, 4), 4));
            if (length != object.length() || !Arrays.equals(hash, object.payloadSha256())) {
                throw new IllegalArgumentException("object payload length/hash mismatch");
            }
        }
        if (!Arrays.equals(encoded, canonical(fields))) {
            throw new IllegalArgumentException("non-canonical PayloadForPublish");
        }
    }

    private static void validateAdapterMetadata(final byte[] encoded) {
        AdapterMetadataV1.decode(encoded);
    }

    private static void validateChargeVector(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ChargeVector");
        requireExact(fields, 17, "ChargeVector");
        for (int number = 1; number <= 17; number++) {
            bodyUnsigned(field(fields, number), number);
        }
        if (!Arrays.equals(encoded, canonical(fields))) {
            throw new IllegalArgumentException("non-canonical ChargeVector");
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        if (encoded.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static byte[] canonical(final List<CanonicalProtobuf.Reader.Field> fields) {
        return CanonicalProtobuf.message(output -> {
            for (CanonicalProtobuf.Reader.Field field : fields) {
                if (field.wireType() == 0) {
                    CanonicalProtobuf.uint64Bits(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
    }

    private static void requireExact(final List<CanonicalProtobuf.Reader.Field> fields, final int count,
                                     final String name) {
        if (fields.size() != count) {
            throw new IllegalArgumentException(name + " has unexpected field count");
        }
    }

    private static CanonicalProtobuf.Reader.Field field(final List<CanonicalProtobuf.Reader.Field> fields,
                                                        final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return field;
            }
        }
        throw new IllegalArgumentException("missing nested field " + number);
    }

    private static boolean has(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        return fields.stream().anyMatch(field -> field.number() == number);
    }

    private static long bodyUnsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid Claim Result scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static int intValue(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = bodyUnsigned(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Claim Result uint32 exceeds runtime range: " + number);
        }
        return (int) value;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid Claim Result bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        read(value, "nested Claim Result field " + number);
        return value;
    }

    private static byte[] fixed(final CanonicalProtobuf.Reader.Field field, final int number, final int length) {
        return fixed(bytes(field, number), length, "Claim Result field " + number);
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return copy(value);
    }

    private static byte[] copy(final byte[] value) {
        return Bytes.copy(Objects.requireNonNull(value, "value"));
    }

    /** Closed semantic projection of ClaimPreconditionV1. */
    public static final class ClaimPrecondition {
        private final byte[] canonicalBytes;
        private final byte[] claimId;
        private final byte[] messageId;
        private final int generation;
        private final long stateVersion;
        private final byte[] destinationLaneId;
        private final byte[] laneIncarnation;
        private final long laneControlVersion;
        private final long runtimeLaneVersion;
        private final byte[] originalTimelineKeySha256;
        private final byte[] materialization;
        private final byte[] claimedCharge;
        private final long claimDeadline;
        private final byte[] ownerIdentity;
        private final byte[] storeIncarnation;
        private final int sourceWorkKind;
        private final int expectedAdmissionsUsed;
        private final int expectedUncertainRetryAdmissionsUsed;
        private final byte[] expectedObligationSetDigest;
        private final byte[] sourceTimelineSemanticDigest;

        private ClaimPrecondition(final byte[] canonicalBytes, final byte[] claimId, final byte[] messageId,
                                  final int generation, final long stateVersion, final byte[] destinationLaneId,
                                  final byte[] laneIncarnation, final long laneControlVersion,
                                  final long runtimeLaneVersion, final byte[] originalTimelineKeySha256,
                                  final byte[] materialization, final byte[] claimedCharge, final long claimDeadline,
                                  final byte[] ownerIdentity, final byte[] storeIncarnation, final int sourceWorkKind,
                                  final int expectedAdmissionsUsed, final int expectedUncertainRetryAdmissionsUsed,
                                  final byte[] expectedObligationSetDigest,
                                  final byte[] sourceTimelineSemanticDigest) {
            this.canonicalBytes = copy(canonicalBytes);
            this.claimId = fixed(claimId, HASH_LENGTH, "precondition claimId");
            this.messageId = fixed(messageId, MESSAGE_ID_LENGTH, "precondition messageId");
            this.generation = generation;
            this.stateVersion = stateVersion;
            this.destinationLaneId = fixed(destinationLaneId, HASH_LENGTH, "precondition laneId");
            this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "precondition laneIncarnation");
            this.laneControlVersion = laneControlVersion;
            this.runtimeLaneVersion = runtimeLaneVersion;
            this.originalTimelineKeySha256 = fixed(originalTimelineKeySha256, HASH_LENGTH,
                    "originalTimelineKeySha256");
            this.materialization = copy(materialization);
            this.claimedCharge = copy(claimedCharge);
            this.claimDeadline = claimDeadline;
            this.ownerIdentity = copy(ownerIdentity);
            this.storeIncarnation = fixed(storeIncarnation, INCARNATION_LENGTH, "precondition storeIncarnation");
            this.sourceWorkKind = sourceWorkKind;
            this.expectedAdmissionsUsed = expectedAdmissionsUsed;
            this.expectedUncertainRetryAdmissionsUsed = expectedUncertainRetryAdmissionsUsed;
            this.expectedObligationSetDigest = fixed(expectedObligationSetDigest, HASH_LENGTH,
                    "expectedObligationSetDigest");
            this.sourceTimelineSemanticDigest = fixed(sourceTimelineSemanticDigest, HASH_LENGTH,
                    "sourceTimelineSemanticDigest");
        }

        public byte[] canonicalBytes() { return copy(canonicalBytes); }
        public byte[] claimId() { return copy(claimId); }
        public byte[] messageId() { return copy(messageId); }
        public int generation() { return generation; }
        public long stateVersion() { return stateVersion; }
        public byte[] destinationLaneId() { return copy(destinationLaneId); }
        public byte[] laneIncarnation() { return copy(laneIncarnation); }
        public long laneControlVersion() { return laneControlVersion; }
        public long runtimeLaneVersion() { return runtimeLaneVersion; }
        public byte[] originalTimelineKeySha256() { return copy(originalTimelineKeySha256); }
        public boolean hasMaterialization() { return materialization.length != 0; }
        public byte[] materialization() { return copy(materialization); }
        public byte[] claimedCharge() { return copy(claimedCharge); }
        public long claimDeadline() { return claimDeadline; }
        public byte[] ownerIdentity() { return copy(ownerIdentity); }
        public byte[] storeIncarnation() { return copy(storeIncarnation); }
        public int sourceWorkKind() { return sourceWorkKind; }
        public int expectedAdmissionsUsed() { return expectedAdmissionsUsed; }
        public int expectedUncertainRetryAdmissionsUsed() { return expectedUncertainRetryAdmissionsUsed; }
        public byte[] expectedObligationSetDigest() { return copy(expectedObligationSetDigest); }
        public byte[] sourceTimelineSemanticDigest() { return copy(sourceTimelineSemanticDigest); }
    }
}
