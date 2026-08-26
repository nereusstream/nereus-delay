package com.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** ControlPayload field 12: activate a source-ordered payload proof trust set. */
public final class PayloadProofTrustSetActivatePayload {
    private final PayloadProofTrustSetRef trustSet;

    public PayloadProofTrustSetActivatePayload(final PayloadProofTrustSetRef trustSet) {
        this.trustSet = Objects.requireNonNull(trustSet, "trustSet");
    }

    public PayloadProofTrustSetRef trustSet() {
        return trustSet;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, trustSet.canonicalBytes()));
    }

    public static PayloadProofTrustSetActivatePayload decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                QueryCodecSupport.read(encoded, "PayloadProofTrustSetActivatePayload");
        QueryCodecSupport.requireNumbers(fields, new int[] {1}, "PayloadProofTrustSetActivatePayload");
        final PayloadProofTrustSetActivatePayload result = new PayloadProofTrustSetActivatePayload(
                PayloadProofTrustSetRef.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "PayloadProofTrustSetActivatePayload");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadProofTrustSetActivatePayload that && trustSet.equals(that.trustSet);
    }

    @Override
    public int hashCode() {
        return trustSet.hashCode();
    }
}
