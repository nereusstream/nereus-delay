package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.ScheduleIntentV1;
import io.nereusstream.delay.protocol.SubmissionModeV1;
import io.nereusstream.delay.semantic.RouteSelectionHint;

import java.util.Objects;

/** Transport-neutral Gateway Schedule request; tenant authority is out of band. */
public record GatewayScheduleRequestV1(byte[] idempotencyKey, RouteSelectionHint route,
                                       ScheduleIntentV1 scheduleIntent, long retryUntilEpochMs,
                                       SubmissionModeV1 submissionMode) {
    public GatewayScheduleRequestV1 {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        if (idempotencyKey.length < 16 || idempotencyKey.length > 64) {
            throw new IllegalArgumentException("idempotencyKey must be 16..64 bytes");
        }
        idempotencyKey = Bytes.copy(idempotencyKey);
        route = Objects.requireNonNull(route, "route");
        scheduleIntent = Objects.requireNonNull(scheduleIntent, "scheduleIntent");
        if (retryUntilEpochMs < 0) {
            throw new IllegalArgumentException("retryUntilEpochMs must be non-negative");
        }
        submissionMode = Objects.requireNonNull(submissionMode, "submissionMode");
    }

    @Override
    public byte[] idempotencyKey() {
        return Bytes.copy(idempotencyKey);
    }

    /** Canonical request fields 2..N used by Gateway bodyHash. */
    public byte[] canonicalBodyBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 2, CanonicalProtobuf.message(routeOutput -> {
                CanonicalProtobuf.uint32(routeOutput, 1, route.adapterKind().wireValue());
                CanonicalProtobuf.bytes(routeOutput, 2, route.routeAliasUtf8Nfc());
            }));
            CanonicalProtobuf.bytes(output, 3, scheduleIntent.canonicalBytes());
            CanonicalProtobuf.int64(output, 4, retryUntilEpochMs);
            CanonicalProtobuf.uint32(output, 5, submissionMode.wireValue());
        });
    }
}
