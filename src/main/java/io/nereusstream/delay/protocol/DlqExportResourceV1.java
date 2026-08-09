package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Canonical typed value for the Registry DlqExportResourceV1 branch. */
public final class DlqExportResourceV1 {
    private static final int HASH_LENGTH = 32;

    private final byte[] exportId;
    private final BrokerResourceIdentityV1 targetResource;
    private final byte[] objectOrMessageIdentity;
    private final byte[] payloadSha256;

    public DlqExportResourceV1(final byte[] exportId, final BrokerResourceIdentityV1 targetResource,
                               final byte[] objectOrMessageIdentity, final byte[] payloadSha256) {
        this.exportId = fixed(exportId, HASH_LENGTH, "exportId");
        this.targetResource = Objects.requireNonNull(targetResource, "targetResource");
        this.objectOrMessageIdentity = nonEmpty(objectOrMessageIdentity, "objectOrMessageIdentity");
        this.payloadSha256 = fixed(payloadSha256, HASH_LENGTH, "payloadSha256");
    }

    public byte[] exportId() {
        return Bytes.copy(exportId);
    }

    public BrokerResourceIdentityV1 targetResource() {
        return targetResource;
    }

    public byte[] objectOrMessageIdentity() {
        return Bytes.copy(objectOrMessageIdentity);
    }

    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, exportId);
            CanonicalProtobuf.bytes(output, 2, targetResource.canonicalBytes());
            CanonicalProtobuf.bytes(output, 3, objectOrMessageIdentity);
            CanonicalProtobuf.bytes(output, 4, payloadSha256);
        });
    }

    public byte[] exactResourceCanonicalBytes() {
        return CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, ResourceKind.DLQ_EXPORT_OBJECT.wireValue(), canonicalBytes()));
    }

    public static DlqExportResourceV1 decode(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded,
                "DlqExportResourceV1");
        QueryCodecSupport.requireNumbers(fields, new int[]{1, 2, 3, 4}, "DlqExportResourceV1");
        final DlqExportResourceV1 result = new DlqExportResourceV1(
                QueryCodecSupport.fixed(fields.get(0), 1, HASH_LENGTH),
                BrokerResourceIdentityV1.decode(QueryCodecSupport.nested(fields.get(1), 2)),
                QueryCodecSupport.bytes(fields.get(2), 3),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH));
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "DlqExportResourceV1");
        return result;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return Bytes.copy(value);
    }

    private static byte[] nonEmpty(final byte[] value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return Bytes.copy(value);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DlqExportResourceV1 that
                && Arrays.equals(exportId, that.exportId)
                && targetResource.equals(that.targetResource)
                && Arrays.equals(objectOrMessageIdentity, that.objectOrMessageIdentity)
                && Arrays.equals(payloadSha256, that.payloadSha256);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(exportId), targetResource, Arrays.hashCode(objectOrMessageIdentity),
                Arrays.hashCode(payloadSha256));
    }
}
