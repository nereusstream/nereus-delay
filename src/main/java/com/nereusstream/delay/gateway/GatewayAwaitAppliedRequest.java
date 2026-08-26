package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import java.util.Objects;

/** Transport-neutral Gateway bounded await locator. */
public record GatewayAwaitAppliedRequest(CanonicalCommandQueuedReceipt receipt) {
    public GatewayAwaitAppliedRequest {
        Objects.requireNonNull(receipt, "receipt");
    }

    public byte[] canonicalBodyBytes() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1, receipt.payload()));
    }
}
