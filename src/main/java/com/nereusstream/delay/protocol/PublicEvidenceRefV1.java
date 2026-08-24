package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Safe evidence reference without endpoint, object or signature material. */
public final class PublicEvidenceRefV1 {
    private final PublishEvidenceKindV1 evidenceType;
    private final byte[] evidenceId;
    private final EvidenceVerificationStatusV1 verificationStatus;

    public PublicEvidenceRefV1(
            final PublishEvidenceKindV1 evidenceType,
            final byte[] evidenceId,
            final EvidenceVerificationStatusV1 verificationStatus) {
        this.evidenceType = Objects.requireNonNull(evidenceType, "evidenceType");
        Bytes.requireLength(evidenceId, 32, "evidenceId");
        this.evidenceId = Bytes.copy(evidenceId);
        this.verificationStatus = Objects.requireNonNull(verificationStatus, "verificationStatus");
    }

    public PublishEvidenceKindV1 evidenceType() {
        return evidenceType;
    }

    public byte[] evidenceId() {
        return Bytes.copy(evidenceId);
    }

    public EvidenceVerificationStatusV1 verificationStatus() {
        return verificationStatus;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, evidenceType.wireValue());
            CanonicalProtobuf.bytes(output, 2, evidenceId);
            CanonicalProtobuf.uint32(output, 3, verificationStatus.wireValue());
        });
    }

    public static PublicEvidenceRefV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "PublicEvidenceRefV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "PublicEvidenceRefV1");
        final PublicEvidenceRefV1 result = new PublicEvidenceRefV1(
                PublishEvidenceKindV1.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, 32),
                EvidenceVerificationStatusV1.fromWire(QueryCodecSupport.uint(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PublicEvidenceRefV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PublicEvidenceRefV1 that)) {
            return false;
        }
        return evidenceType == that.evidenceType
                && verificationStatus == that.verificationStatus
                && Arrays.equals(evidenceId, that.evidenceId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(evidenceType, Arrays.hashCode(evidenceId), verificationStatus);
    }
}
