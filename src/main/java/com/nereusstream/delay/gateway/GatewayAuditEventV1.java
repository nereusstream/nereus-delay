package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.transport.Digest32;
import java.util.Objects;

/**
 * Digest-only Gateway audit event. The event is intentionally not a request
 * or idempotency record and cannot reconstruct prepared or credential bytes.
 */
public record GatewayAuditEventV1(
        GatewayIngressOperationV1 operation,
        Digest32 gatewayKeyHash,
        Digest32 requestBodyHash,
        GatewayAuditPhaseV1 phase,
        Digest32 outcomeHash,
        long observedAtEpochMs) {
    public GatewayAuditEventV1 {
        Objects.requireNonNull(operation, "operation");
        Objects.requireNonNull(gatewayKeyHash, "gatewayKeyHash");
        Objects.requireNonNull(requestBodyHash, "requestBodyHash");
        Objects.requireNonNull(phase, "phase");
        if ((phase == GatewayAuditPhaseV1.COMPLETED) != (outcomeHash != null)) {
            throw new IllegalArgumentException("Gateway audit outcome hash must match the completed phase");
        }
        if (observedAtEpochMs < 0) {
            throw new IllegalArgumentException("observedAtEpochMs must be non-negative");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 1);
            CanonicalProtobuf.uint32(output, 2, operation.ordinal() + 1);
            CanonicalProtobuf.bytes(output, 3, gatewayKeyHash.bytes());
            CanonicalProtobuf.bytes(output, 4, requestBodyHash.bytes());
            CanonicalProtobuf.uint32(output, 5, phase.ordinal() + 1);
            if (outcomeHash != null) {
                CanonicalProtobuf.bytes(output, 6, outcomeHash.bytes());
            }
            CanonicalProtobuf.int64(output, 7, observedAtEpochMs);
        });
    }

    public static GatewayAuditEventV1 completed(
            final GatewayIngressOperationV1 operation,
            final Digest32 gatewayKeyHash,
            final Digest32 requestBodyHash,
            final byte[] outcomeBytes,
            final long observedAtEpochMs) {
        return new GatewayAuditEventV1(
                operation,
                gatewayKeyHash,
                requestBodyHash,
                GatewayAuditPhaseV1.COMPLETED,
                new Digest32(Bytes.sha256(outcomeBytes)),
                observedAtEpochMs);
    }
}
