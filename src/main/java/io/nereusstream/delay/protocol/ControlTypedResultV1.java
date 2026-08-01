package io.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Closed typed-result oneof envelope.  Each branch is decoded by its exact
 * control-result codec before the canonical bytes are retained; a branch tag
 * can therefore never be paired with an unrelated protobuf payload.
 */
public final class ControlTypedResultV1 {
    private final ControlResultKindV1 kind;
    private final byte[] payload;

    public ControlTypedResultV1(final ControlResultKindV1 kind, final byte[] payload) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.payload = canonicalPayload(kind, payload);
    }

    public ControlResultKindV1 kind() {
        return kind;
    }

    public byte[] payload() {
        return Bytes.copy(payload);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output ->
                CanonicalProtobuf.bytes(output, kind.wireValue(), payload));
    }

    public static ControlTypedResultV1 decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ControlTypedResultV1");
        if (fields.size() != 1 || fields.get(0).wireType() != 2) {
            throw new IllegalArgumentException("ControlTypedResultV1 must select exactly one branch");
        }
        final ControlResultKindV1 kind = ControlResultKindV1.fromWire(fields.get(0).number());
        final ControlTypedResultV1 result = new ControlTypedResultV1(kind, fields.get(0).rawValue());
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlTypedResultV1");
        return result;
    }

    private static byte[] canonicalPayload(final ControlResultKindV1 kind, final byte[] value) {
        Objects.requireNonNull(value, "payload");
        if (value.length == 0) {
            throw new IllegalArgumentException("typed control result payload must not be empty");
        }
        return switch (kind) {
            case LANE -> LaneControlResultV1.decode(value).canonicalBytes();
            case SHARD -> ShardControlResultV1.decode(value).canonicalBytes();
            case CHECKPOINT -> CheckpointControlResultV1.decode(value).canonicalBytes();
            case PROFILE -> ProfileControlResultV1.decode(value).canonicalBytes();
            case QUOTA -> QuotaControlResultV1.decode(value).canonicalBytes();
            case MESSAGE -> MessageControlResultV1.decode(value).canonicalBytes();
            case CHECKPOINT_CATALOG -> CheckpointCatalogResultV1.decode(value).canonicalBytes();
            case ROUTE -> RouteControlResultV1.decode(value).canonicalBytes();
            case SECRET_ROTATION -> SecretRotationResultV1.decode(value).canonicalBytes();
        };
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlTypedResultV1 that && kind == that.kind
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(payload));
    }
}
