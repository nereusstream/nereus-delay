package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.CanonicalCommandQueuedReceipt;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.DelayMessageId;

/** Transport-neutral Gateway message query locator. */
public record GatewayGetMessageRequest(DelayMessageId delayMessageId, CanonicalCommandQueuedReceipt receipt) {
    public GatewayGetMessageRequest {
        if ((delayMessageId == null) == (receipt == null)) {
            throw new IllegalArgumentException("exactly one message query locator is required");
        }
    }

    public byte[] canonicalBodyBytes() {
        return CanonicalProtobuf.message(output -> {
            if (delayMessageId != null) {
                CanonicalProtobuf.bytes(output, 1, delayMessageId.bytes());
            } else {
                CanonicalProtobuf.bytes(output, 2, receipt.payload());
            }
        });
    }
}
