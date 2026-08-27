package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Closed owner identity carried by a PublishEvidence branch. */
public final class ExternalDeliveryIdentity {
    public static final int LENGTH = 32;

    public enum Kind {
        PUBLISH_ATTEMPT,
        DLQ_EXPORT,
        NATIVE_DELIVERY
    }

    private final Kind kind;
    private final byte[] identity;

    private ExternalDeliveryIdentity(final Kind kind, final byte[] identity) {
        this.kind = Objects.requireNonNull(kind, "kind");
        Bytes.requireLength(identity, LENGTH, "externalDeliveryIdentity");
        if (isZero(identity)) {
            throw new IllegalArgumentException("externalDeliveryIdentity must be non-zero");
        }
        this.identity = Bytes.copy(identity);
    }

    public static ExternalDeliveryIdentity publishAttempt(final byte[] publishAttemptId) {
        return new ExternalDeliveryIdentity(Kind.PUBLISH_ATTEMPT, publishAttemptId);
    }

    public static ExternalDeliveryIdentity dlqExport(final byte[] dlqExportId) {
        return new ExternalDeliveryIdentity(Kind.DLQ_EXPORT, dlqExportId);
    }

    public static ExternalDeliveryIdentity nativeDelivery(final byte[] nativeDeliveryId) {
        return new ExternalDeliveryIdentity(Kind.NATIVE_DELIVERY, nativeDeliveryId);
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

    public byte[] nativeDeliveryId() {
        if (kind != Kind.NATIVE_DELIVERY) {
            throw new IllegalStateException("external identity is not a Native Delivery");
        }
        return identity();
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(
                output, kind == Kind.PUBLISH_ATTEMPT ? 1 : kind == Kind.DLQ_EXPORT ? 2 : 3, identity));
    }

    public static ExternalDeliveryIdentity decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "ExternalDeliveryIdentity");
        if (fields.size() != 1) {
            throw new IllegalArgumentException("ExternalDeliveryIdentity must select one branch");
        }
        final ExternalDeliveryIdentity result =
                switch (fields.get(0).number()) {
                    case 1 -> publishAttempt(QueryCodecSupport.fixed(fields.get(0), 1, LENGTH));
                    case 2 -> dlqExport(QueryCodecSupport.fixed(fields.get(0), 2, LENGTH));
                    case 3 -> nativeDelivery(QueryCodecSupport.fixed(fields.get(0), 3, LENGTH));
                    default -> throw new IllegalArgumentException("unknown ExternalDeliveryIdentity branch");
                };
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ExternalDeliveryIdentity");
        return result;
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ExternalDeliveryIdentity that
                && kind == that.kind
                && Arrays.equals(identity, that.identity);
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
