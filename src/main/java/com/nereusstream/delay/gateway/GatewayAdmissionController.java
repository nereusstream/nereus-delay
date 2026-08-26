package com.nereusstream.delay.gateway;

import com.nereusstream.delay.protocol.StableCode;
import java.util.Objects;

/** Admission boundary used before Semantic Core preparation or a retry attempt. */
public interface GatewayAdmissionController {
    Decision reserve(GatewayAdmissionRequest request);

    enum State {
        ACCEPTED,
        REJECTED
    }

    record Decision(State state, GatewayAdmissionLease lease, StableCode rejectionCode) {
        public Decision {
            Objects.requireNonNull(state, "state");
            if (state == State.ACCEPTED) {
                Objects.requireNonNull(lease, "lease");
                if (rejectionCode != null) {
                    throw new IllegalArgumentException("accepted admission cannot carry rejection code");
                }
            } else {
                if (lease != null || rejectionCode == null) {
                    throw new IllegalArgumentException("rejected admission must carry only a rejection code");
                }
            }
        }

        public static Decision accepted(final GatewayAdmissionLease lease) {
            return new Decision(State.ACCEPTED, lease, null);
        }

        public static Decision rejected(final StableCode code) {
            return new Decision(State.REJECTED, null, Objects.requireNonNull(code, "code"));
        }
    }
}
