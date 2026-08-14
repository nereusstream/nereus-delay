package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.semantic.RouteSelectionHint;

import java.util.Objects;

/** Transport-neutral Gateway PrepareLargeSchedule request. */
public record GatewayPrepareLargeScheduleRequestV1(
        byte[] idempotencyKey,
        RouteSelectionHint route,
        ScheduleIntentV1 scheduleIntent,
        long expectedPayloadLength,
        byte[] payloadSha256,
        long reservationTtlMs,
        PayloadProofTrustSetRefV1 trustSet,
        ProfileRefV1 objectStoreProfile,
        long retryUntilEpochMs) {
    public GatewayPrepareLargeScheduleRequestV1 {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        if (idempotencyKey.length < 16 || idempotencyKey.length > 64) {
            throw new IllegalArgumentException("idempotencyKey must be 16..64 bytes");
        }
        idempotencyKey = Bytes.copy(idempotencyKey);
        route = Objects.requireNonNull(route, "route");
        scheduleIntent = Objects.requireNonNull(scheduleIntent, "scheduleIntent");
        if (scheduleIntent.hasPayloadBranch()) {
            throw new IllegalArgumentException("PrepareLargeSchedule intent must not contain a payload branch");
        }
        if (expectedPayloadLength < 0 || reservationTtlMs <= 0 || retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("invalid large Gateway request bounds");
        }
        Bytes.requireLength(payloadSha256, 32, "payloadSha256");
        payloadSha256 = Bytes.copy(payloadSha256);
        trustSet = Objects.requireNonNull(trustSet, "trustSet");
        objectStoreProfile = Objects.requireNonNull(objectStoreProfile, "objectStoreProfile");
    }

    @Override
    public byte[] idempotencyKey() {
        return Bytes.copy(idempotencyKey);
    }

    @Override
    public byte[] payloadSha256() {
        return Bytes.copy(payloadSha256);
    }

    /** Canonical request fields 2..N used by Gateway bodyHash. */
    public byte[] canonicalBodyBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 2, CanonicalProtobuf.message(routeOutput -> {
                CanonicalProtobuf.uint32(routeOutput, 1, route.adapterKind().wireValue());
                CanonicalProtobuf.bytes(routeOutput, 2, route.routeAliasUtf8Nfc());
            }));
            CanonicalProtobuf.bytes(output, 3, scheduleIntent.canonicalBytes());
            CanonicalProtobuf.uint64(output, 4, expectedPayloadLength);
            CanonicalProtobuf.bytes(output, 5, payloadSha256);
            CanonicalProtobuf.uint64(output, 6, reservationTtlMs);
            CanonicalProtobuf.bytes(output, 7, trustSet.canonicalBytes());
            CanonicalProtobuf.bytes(output, 8, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.int64(output, 9, retryUntilEpochMs);
        });
    }
}
