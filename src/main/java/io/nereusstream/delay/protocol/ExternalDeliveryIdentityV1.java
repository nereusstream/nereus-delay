package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Closed owner identity carried by a PublishEvidenceV1 branch. */
public final class ExternalDeliveryIdentityV1 {
    public static final int LENGTH = 32;

    public enum Kind {
        PUBLISH_ATTEMPT,
        DLQ_EXPORT
    }

    private final Kind kind;
    private final byte[] identity;

    private ExternalDeliveryIdentityV1(final Kind kind, final byte[] identity) {
        this.kind = Objects.requireNonNull(kind, "kind");
        Bytes.requireLength(identity, LENGTH, "externalDeliveryIdentity");
        if (isZero(identity)) {
            throw new IllegalArgumentException("externalDeliveryIdentity must be non-zero");
        }
        this.identity = Bytes.copy(identity);
    }

    public static ExternalDeliveryIdentityV1 publishAttempt(final byte[] publishAttemptId) {
        return new ExternalDeliveryIdentityV1(Kind.PUBLISH_ATTEMPT, publishAttemptId);
    }

    public static ExternalDeliveryIdentityV1 dlqExport(final byte[] dlqExportId) {
        return new ExternalDeliveryIdentityV1(Kind.DLQ_EXPORT, dlqExportId);
    }

    public Kind kind() {
        return kind;
    }

    public byte[] identity() {
        return Bytes.copy(identity);
    }

    public byte[] publishAttemptId() {
        if (kind != Kind.PUBLISH_ATTEMPT) {
            throw new IllegalStateException("external identity is not a Publish Attempt");
        }
        return identity();
    }

    public byte[] dlqExportId() {
        if (kind != Kind.DLQ_EXPORT) {
            throw new IllegalStateException("external identity is not a DLQ Export");
        }
        return identity();
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output,
                kind == Kind.PUBLISH_ATTEMPT ? 1 : 2, identity));
    }

    public static ExternalDeliveryIdentityV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "ExternalDeliveryIdentityV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{fields.get(0).number()},
                "ExternalDeliveryIdentityV1");
        final ExternalDeliveryIdentityV1 result = switch (fields.get(0).number()) {
            case 1 -> publishAttempt(QueryCodecSupport.fixed(fields.get(0), 1, LENGTH));
            case 2 -> dlqExport(QueryCodecSupport.fixed(fields.get(0), 2, LENGTH));
            default -> throw new IllegalArgumentException("unknown ExternalDeliveryIdentityV1 branch");
        };
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ExternalDeliveryIdentityV1");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ExternalDeliveryIdentityV1 that
                && kind == that.kind && Arrays.equals(identity, that.identity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(identity));
    }

    private static boolean isZero(final byte[] value) {
        for (byte current : value) {
            if (current != 0) {
                return false;
            }
        }
        return true;
    }
}
