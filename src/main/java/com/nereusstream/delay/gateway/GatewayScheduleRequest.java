package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalScheduleIntent;
import com.nereusstream.delay.protocol.SubmissionMode;
import com.nereusstream.delay.semantic.RouteSelectionHint;
import java.util.Objects;

/** Transport-neutral Gateway Schedule request; tenant authority is out of band. */
public record GatewayScheduleRequest(
        byte[] idempotencyKey,
        RouteSelectionHint route,
        CanonicalScheduleIntent scheduleIntent,
        long retryUntilEpochMs,
        SubmissionMode submissionMode) {
    public GatewayScheduleRequest {
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
