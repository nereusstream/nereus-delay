package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;

import java.util.Objects;

/** Transport-neutral Gateway bounded await locator. */
public record GatewayAwaitAppliedRequestV1(CommandQueuedReceiptV1 receipt) {
    public GatewayAwaitAppliedRequestV1 {
        Objects.requireNonNull(receipt, "receipt");
    }

    public byte[] canonicalBodyBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, receipt.payload()));
    }
}
