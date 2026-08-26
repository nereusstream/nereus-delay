package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/**
 * Closed typed-result oneof envelope. Each branch is decoded by its exact
 * control-result codec before the canonical bytes are retained; a branch tag
 * can therefore never be paired with an unrelated protobuf payload.
 */
public final class ControlTypedResult {
    private final ControlResultKind kind;
    private final byte[] payload;

    public ControlTypedResult(final ControlResultKind kind, final byte[] payload) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.payload = canonicalPayload(kind, payload);
    }

    public ControlResultKind kind() {
        return kind;
    }

    public byte[] payload() {
        return Bytes.copy(payload);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, kind.wireValue(), payload));
    }

    public static ControlTypedResult decode(final byte[] encoded) {
        final var fields = QueryCodecSupport.read(encoded, "ControlTypedResult");
        if (fields.size() != 1 || fields.get(0).wireType() != 2) {
            throw new IllegalArgumentException("ControlTypedResult must select exactly one branch");
        }
        final ControlResultKind kind = ControlResultKind.fromWire(fields.get(0).number());
        final ControlTypedResult result =
                new ControlTypedResult(kind, fields.get(0).rawValue());
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "ControlTypedResult");
        return result;
    }

    private static byte[] canonicalPayload(final ControlResultKind kind, final byte[] value) {
        Objects.requireNonNull(value, "payload");
        if (value.length == 0) {
            throw new IllegalArgumentException("typed control result payload must not be empty");
        }
        return switch (kind) {
            case LANE -> LaneControlResult.decode(value).canonicalBytes();
            case SHARD -> ShardControlResult.decode(value).canonicalBytes();
            case CHECKPOINT -> CheckpointControlResult.decode(value).canonicalBytes();
            case PROFILE -> ProfileControlResult.decode(value).canonicalBytes();
            case QUOTA -> QuotaControlResult.decode(value).canonicalBytes();
            case MESSAGE -> MessageControlResult.decode(value).canonicalBytes();
            case CHECKPOINT_CATALOG -> CheckpointCatalogResult.decode(value).canonicalBytes();
            case ROUTE -> RouteControlResult.decode(value).canonicalBytes();
            case SECRET_ROTATION -> SecretRotationResult.decode(value).canonicalBytes();
        };
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof ControlTypedResult that && kind == that.kind && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, Arrays.hashCode(payload));
    }
}
