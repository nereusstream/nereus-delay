package com.nereusstream.delay.assessment;

import com.nereusstream.delay.runtime.TrustedUtcInterval;
import java.util.Objects;

/** Read-once G0 orchestration; local receipt persistence is an explicit separate step. */
public final class DataResetAssessmentRunner {
    private final DataResetInventoryReader reader;

    public DataResetAssessmentRunner(final DataResetInventoryReader reader) {
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    public DataResetAssessmentReceipt assess(
            final DataResetAssessmentScope scope,
            final TrustedUtcInterval observationTime,
            final String ndipPackageDigest,
            final String sourceBaselineCommit) {
        final DataResetInventory inventory = Objects.requireNonNull(
                reader.read(
                        Objects.requireNonNull(scope, "scope"),
                        Objects.requireNonNull(observationTime, "observationTime")),
                "reader result");
        return DataResetAssessmentEvaluator.evaluate(scope, inventory, ndipPackageDigest, sourceBaselineCommit);
    }
}
