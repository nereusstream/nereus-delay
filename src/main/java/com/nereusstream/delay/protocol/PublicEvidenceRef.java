package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Safe evidence reference without endpoint, object or signature material. */
public final class PublicEvidenceRef {
    private final PublishEvidenceKind evidenceType;
    private final byte[] evidenceId;
    private final EvidenceVerificationStatus verificationStatus;

    public PublicEvidenceRef(
            final PublishEvidenceKind evidenceType,
            final byte[] evidenceId,
            final EvidenceVerificationStatus verificationStatus) {
        this.evidenceType = Objects.requireNonNull(evidenceType, "evidenceType");
        Bytes.requireLength(evidenceId, 32, "evidenceId");
        this.evidenceId = Bytes.copy(evidenceId);
        this.verificationStatus = Objects.requireNonNull(verificationStatus, "verificationStatus");
    }

    public PublishEvidenceKind evidenceType() {
        return evidenceType;
    }

    public byte[] evidenceId() {
        return Bytes.copy(evidenceId);
    }

    public EvidenceVerificationStatus verificationStatus() {
        return verificationStatus;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, evidenceType.wireValue());
            CanonicalProtobuf.bytes(output, 2, evidenceId);
            CanonicalProtobuf.uint32(output, 3, verificationStatus.wireValue());
        });
    }

    public static PublicEvidenceRef decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "PublicEvidenceRef");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "PublicEvidenceRef");
        final PublicEvidenceRef result = new PublicEvidenceRef(
                PublishEvidenceKind.fromWire(QueryCodecSupport.uint(fields.get(0), 1)),
                QueryCodecSupport.fixed(fields.get(1), 2, 32),
                EvidenceVerificationStatus.fromWire(QueryCodecSupport.uint(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PublicEvidenceRef");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof PublicEvidenceRef that)) {
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
