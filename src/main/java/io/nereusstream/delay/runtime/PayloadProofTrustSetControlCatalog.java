package io.nereusstream.delay.runtime;

import io.nereusstream.delay.protocol.PayloadProofTrustSetRefV1;
import io.nereusstream.delay.protocol.PayloadProofTrustSetSemanticV1;

/**
 * Source-side catalog seam for immutable payload-proof trust-set values.
 * Implementations must resolve the exact semantic reference or return null;
 * the shard never accepts an unverified marker as a business rejection.
 */
@FunctionalInterface
public interface PayloadProofTrustSetControlCatalog {
    PayloadProofTrustSetSemanticV1 resolve(PayloadProofTrustSetRefV1 reference);
}
