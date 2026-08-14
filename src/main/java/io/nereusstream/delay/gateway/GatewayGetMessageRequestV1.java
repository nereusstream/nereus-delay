package io.nereusstream.delay.gateway;

import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import io.nereusstream.delay.protocol.DelayMessageId;

/** Transport-neutral Gateway message query locator. */
public record GatewayGetMessageRequestV1(DelayMessageId delayMessageId, CommandQueuedReceiptV1 receipt) {
    public GatewayGetMessageRequestV1 {
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
