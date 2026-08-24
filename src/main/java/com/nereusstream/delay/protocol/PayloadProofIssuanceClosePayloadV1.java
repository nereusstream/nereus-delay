package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** ControlPayload field 13: close first-seen proof issuance for one key. */
public final class PayloadProofIssuanceClosePayloadV1 {
    private final PayloadProofTrustSetRefV1 trustSet;
    private final int proofKeyVersion;
    private final ControlReasonV1 reason;

    public PayloadProofIssuanceClosePayloadV1(
            final PayloadProofTrustSetRefV1 trustSet, final int proofKeyVersion, final ControlReasonV1 reason) {
        this.trustSet = Objects.requireNonNull(trustSet, "trustSet");
        if (proofKeyVersion == 0) {
            throw new IllegalArgumentException("proofKeyVersion must be a non-zero uint32");
        }
        this.proofKeyVersion = proofKeyVersion;
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public PayloadProofTrustSetRefV1 trustSet() {
        return trustSet;
    }

    public int proofKeyVersion() {
        return proofKeyVersion;
    }

    public ControlReasonV1 reason() {
        return reason;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, trustSet.canonicalBytes());
            CanonicalProtobuf.uint32Bits(output, 2, proofKeyVersion);
            CanonicalProtobuf.bytes(output, 3, reason.canonicalBytes());
        });
    }

    public static PayloadProofIssuanceClosePayloadV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "PayloadProofIssuanceClosePayloadV1");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "PayloadProofIssuanceClosePayloadV1");
        final PayloadProofIssuanceClosePayloadV1 result = new PayloadProofIssuanceClosePayloadV1(
                PayloadProofTrustSetRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.uint32Bits(fields.get(1), 2),
                ControlReasonV1.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadProofIssuanceClosePayloadV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadProofIssuanceClosePayloadV1 that
                && proofKeyVersion == that.proofKeyVersion
                && trustSet.equals(that.trustSet)
                && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trustSet, proofKeyVersion, reason);
    }
}
