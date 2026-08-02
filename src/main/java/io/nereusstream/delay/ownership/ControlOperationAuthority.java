package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.ControlOperationQueryResponseV1;
import io.nereusstream.delay.protocol.ControlOperationReceiptV1;
import io.nereusstream.delay.protocol.CurrentControlOperationV1;

/**
 * Authority boundary for the registered Control Operation query state.
 *
 * <p>The operation receipt is the only public locator.  Implementations must
 * compare the complete receipt and advance one operation revision at a time;
 * a caller must never reconstruct a target set after a response loss.</p>
 */
public interface ControlOperationAuthority {
    ControlOperationQueryResponseV1 register(ControlOperationReceiptV1 receipt,
                                              CurrentControlOperationV1 initial);

    ControlOperationQueryResponseV1 advance(ControlOperationReceiptV1 receipt, long expectedRevision,
                                             CurrentControlOperationV1 next);

    ControlOperationQueryResponseV1 query(ControlOperationReceiptV1 receipt, long nowEpochMs);
}
