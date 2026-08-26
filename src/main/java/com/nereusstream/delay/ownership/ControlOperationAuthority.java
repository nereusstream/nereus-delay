package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.ControlOperationQueryResponse;
import com.nereusstream.delay.protocol.ControlOperationReceipt;
import com.nereusstream.delay.protocol.CurrentControlOperation;

/**
 * Authority boundary for the registered Control Operation query state.
 *
 * <p>The operation receipt is the only public locator. Implementations must
 * compare the complete receipt and advance one operation revision at a time;
 * a caller must never reconstruct a target set after a response loss.</p>
 */
public interface ControlOperationAuthority {
    ControlOperationQueryResponse register(ControlOperationReceipt receipt, CurrentControlOperation initial);

    ControlOperationQueryResponse advance(
            ControlOperationReceipt receipt, long expectedRevision, CurrentControlOperation next);

    ControlOperationQueryResponse query(ControlOperationReceipt receipt, long nowEpochMs);
}
