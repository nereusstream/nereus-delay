package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.CommandQueuedReceiptV1;
import java.util.Objects;

/** Transport-neutral Gateway command query locator. */
public record GatewayGetCommandResultRequestV1(CommandQueuedReceiptV1 receipt, CommandId commandId) {
    public GatewayGetCommandResultRequestV1 {
        if ((receipt == null) == (commandId == null)) {
            throw new IllegalArgumentException("exactly one command query locator is required");
        }
    }

    public byte[] canonicalBodyBytes() {
        return CanonicalProtobuf.message(output -> {
            if (receipt != null) {
                CanonicalProtobuf.bytes(output, 1, receipt.payload());
            } else {
                CanonicalProtobuf.bytes(
                        output,
                        2,
                        Objects.requireNonNull(commandId, "commandId").bytes());
            }
        });
    }
}
