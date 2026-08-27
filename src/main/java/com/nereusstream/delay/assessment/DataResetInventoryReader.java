package com.nereusstream.delay.assessment;

import com.nereusstream.delay.runtime.TrustedUtcInterval;

/**
 * Environment-specific G0 read boundary.
 *
 * <p>Implementations may only observe the supplied scope. They must not expose or invoke Producer, Writer,
 * cleanup, drain, migration, resource-mutation, Worker-rollout or policy-issuance operations.</p>
 */
@FunctionalInterface
public interface DataResetInventoryReader {
    DataResetInventory read(DataResetAssessmentScope scope, TrustedUtcInterval observationTime);
}
