package io.nereusstream.delay.protocol;

import java.util.List;
import java.util.Objects;

/** ControlPayload field 12: activate a source-ordered payload proof trust set. */
public final class PayloadProofTrustSetActivatePayloadV1 {
    private final PayloadProofTrustSetRefV1 trustSet;

    public PayloadProofTrustSetActivatePayloadV1(final PayloadProofTrustSetRefV1 trustSet) {
        this.trustSet = Objects.requireNonNull(trustSet, "trustSet");
    }

    public PayloadProofTrustSetRefV1 trustSet() {
        return trustSet;
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, trustSet.canonicalBytes()));
    }

    public static PayloadProofTrustSetActivatePayloadV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "PayloadProofTrustSetActivatePayloadV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1}, "PayloadProofTrustSetActivatePayloadV1");
        final PayloadProofTrustSetActivatePayloadV1 result = new PayloadProofTrustSetActivatePayloadV1(
                PayloadProofTrustSetRefV1.decode(QueryCodecSupport.nested(fields.get(0), 1)));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(),
                "PayloadProofTrustSetActivatePayloadV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof PayloadProofTrustSetActivatePayloadV1 that && trustSet.equals(that.trustSet);
    }

    @Override
    public int hashCode() {
        return trustSet.hashCode();
    }
}
