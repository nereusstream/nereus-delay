package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** ControlPayload field 13: close first-seen proof issuance for one key. */
public final class PayloadProofIssuanceClosePayload {
    private final PayloadProofTrustSetRef trustSet;
    private final int proofKeyVersion;
    private final ControlReason reason;

    public PayloadProofIssuanceClosePayload(
            final PayloadProofTrustSetRef trustSet, final int proofKeyVersion, final ControlReason reason) {
        this.trustSet = Objects.requireNonNull(trustSet, "trustSet");
        if (proofKeyVersion == 0) {
            throw new IllegalArgumentException("proofKeyVersion must be a non-zero uint32");
        }
        this.proofKeyVersion = proofKeyVersion;
        this.reason = Objects.requireNonNull(reason, "reason");
    }

    public PayloadProofTrustSetRef trustSet() {
        return trustSet;
    }

    public int proofKeyVersion() {
        return proofKeyVersion;
    }

    public ControlReason reason() {
        return reason;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, trustSet.canonicalBytes());
            CanonicalProtobuf.uint32Bits(output, 2, proofKeyVersion);
            CanonicalProtobuf.bytes(output, 3, reason.canonicalBytes());
        });
    }

    public static PayloadProofIssuanceClosePayload decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "PayloadProofIssuanceClosePayload");
        QueryCodecSupport.requireNumbers(fields, new int[] {1, 2, 3}, "PayloadProofIssuanceClosePayload");
        final PayloadProofIssuanceClosePayload result = new PayloadProofIssuanceClosePayload(
                PayloadProofTrustSetRef.decode(QueryCodecSupport.nested(fields.get(0), 1)),
                QueryCodecSupport.uint32Bits(fields.get(1), 2),
                ControlReason.decode(QueryCodecSupport.nested(fields.get(2), 3)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadProofIssuanceClosePayload");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadProofIssuanceClosePayload that
                && proofKeyVersion == that.proofKeyVersion
                && trustSet.equals(that.trustSet)
                && reason.equals(that.reason);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trustSet, proofKeyVersion, reason);
    }
}
