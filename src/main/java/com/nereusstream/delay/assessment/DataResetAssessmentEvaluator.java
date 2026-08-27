package com.nereusstream.delay.assessment;

import com.nereusstream.delay.assessment.DataResetAssessmentFinding.Code;
import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceRef;
import com.nereusstream.delay.assessment.DataResetInventory.AccessStatus;
import com.nereusstream.delay.assessment.DataResetInventory.ExternalRetentionRequirement;
import com.nereusstream.delay.assessment.DataResetInventory.ObligationDisposition;
import com.nereusstream.delay.assessment.DataResetInventory.ReplacementDisposition;
import com.nereusstream.delay.assessment.DataResetInventory.ResourceObservation;
import com.nereusstream.delay.assessment.DataResetInventory.WorkerObservation;
import com.nereusstream.delay.assessment.DataResetInventory.WorkerUpgradeStatus;
import com.nereusstream.delay.protocol.Bytes;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Pure closed evaluator for the G0 reset, retain, or migrate decision. */
public final class DataResetAssessmentEvaluator {
    private DataResetAssessmentEvaluator() {}

    public static DataResetAssessmentReceipt evaluate(
            final DataResetAssessmentScope scope,
            final DataResetInventory inventory,
            final String ndipPackageDigest,
            final String sourceBaselineCommit) {
        final List<DataResetAssessmentFinding> findings = new ArrayList<>();
        if (!Bytes.constantTimeEquals(scope.scopeDigest(), inventory.scopeDigest())) {
            add(findings, Code.SCOPE_DIGEST_MISMATCH, "scope");
        }
        if (!inventory.scopeEnumerationComplete()) {
            add(findings, Code.SCOPE_ENUMERATION_INCOMPLETE, "scope");
        }
        if (!inventory.observationTime().qualified()) {
            add(findings, Code.OBSERVATION_TIME_UNQUALIFIED, "observation-time");
        }

        final Set<ResourceRef> expectedResources = new HashSet<>(scope.resources());
        final Set<ResourceRef> observedResources = new HashSet<>();
        for (ResourceObservation observation : inventory.resourceObservations()) {
            observedResources.add(observation.resource());
            assessResource(observation, findings);
        }
        addSetDifference(findings, Code.RESOURCE_SET_MISMATCH, "missing:", expectedResources, observedResources);
        addSetDifference(findings, Code.RESOURCE_SET_MISMATCH, "unexpected:", observedResources, expectedResources);

        if (!inventory.obligationEnumerationComplete()) {
            add(findings, Code.OBLIGATION_ENUMERATION_INCOMPLETE, "obligations");
        }
        inventory.obligations().forEach(obligation -> {
            if (obligation.disposition() == ObligationDisposition.PRESERVE_ACROSS_GENERATION) {
                add(findings, Code.OBLIGATION_PRESERVATION_REQUIRED, obligation.kind() + ":" + obligation.identity());
            } else if (obligation.disposition() == ObligationDisposition.UNKNOWN) {
                add(findings, Code.OBLIGATION_DISPOSITION_UNKNOWN, obligation.kind() + ":" + obligation.identity());
            }
        });

        if (!inventory.workerEnumerationComplete()) {
            add(findings, Code.WORKER_ENUMERATION_INCOMPLETE, "workers");
        }
        final Set<String> expectedWorkers = new HashSet<>(scope.eligibleWorkerIds());
        final Set<String> observedWorkers = new HashSet<>();
        for (WorkerObservation worker : inventory.workers()) {
            observedWorkers.add(worker.workerId());
            if (worker.upgradeStatus() == WorkerUpgradeStatus.NOT_UPGRADEABLE) {
                add(findings, Code.WORKER_NOT_UPGRADEABLE, worker.workerId());
            } else if (worker.upgradeStatus() == WorkerUpgradeStatus.UNKNOWN) {
                add(findings, Code.WORKER_STATUS_UNKNOWN, worker.workerId());
            }
        }
        addTextSetDifference(findings, "missing:", expectedWorkers, observedWorkers);
        addTextSetDifference(findings, "unexpected:", observedWorkers, expectedWorkers);

        final boolean migrationRequired =
                findings.stream().anyMatch(finding -> finding.code().migrationRequired());
        final boolean retainCompatible = inventory.resourceObservations().stream()
                .anyMatch(observation ->
                        observation.replacementDisposition() == ReplacementDisposition.RETAIN_COMPATIBLE);
        final DataResetAssessmentOutcome outcome = migrationRequired
                ? DataResetAssessmentOutcome.MIGRATION_REQUIRED
                : findings.isEmpty()
                        ? retainCompatible
                                ? DataResetAssessmentOutcome.PASS_RETAIN
                                : DataResetAssessmentOutcome.PASS_DIRECT_REPLACE
                        : DataResetAssessmentOutcome.INCOMPLETE;
        return new DataResetAssessmentReceipt(
                outcome, ndipPackageDigest, sourceBaselineCommit, scope, inventory, findings);
    }

    private static void assessResource(
            final ResourceObservation observation, final List<DataResetAssessmentFinding> findings) {
        final String subject = observation.resource().subject();
        if (observation.accessStatus() != AccessStatus.COMPLETE) {
            add(findings, Code.RESOURCE_ACCESS_INCOMPLETE, subject);
        }
        if (observation.externalRetention() == ExternalRetentionRequirement.REQUIRED
                && observation.replacementDisposition() != ReplacementDisposition.RETAIN_COMPATIBLE) {
            add(findings, Code.EXTERNAL_RETENTION_NOT_RETAINABLE, subject);
        } else if (observation.externalRetention() == ExternalRetentionRequirement.UNKNOWN) {
            add(findings, Code.EXTERNAL_RETENTION_UNKNOWN, subject);
        }
        if (observation.replacementDisposition() == ReplacementDisposition.MIGRATION_REQUIRED) {
            add(findings, Code.RESOURCE_MIGRATION_REQUIRED, subject);
        } else if (observation.replacementDisposition() == ReplacementDisposition.UNKNOWN) {
            add(findings, Code.RESOURCE_DISPOSITION_UNKNOWN, subject);
        }
    }

    private static void addSetDifference(
            final List<DataResetAssessmentFinding> findings,
            final Code code,
            final String prefix,
            final Set<ResourceRef> left,
            final Set<ResourceRef> right) {
        left.stream()
                .filter(resource -> !right.contains(resource))
                .sorted()
                .forEach(resource -> add(findings, code, prefix + resource.subject()));
    }

    private static void addTextSetDifference(
            final List<DataResetAssessmentFinding> findings,
            final String prefix,
            final Set<String> left,
            final Set<String> right) {
        left.stream()
                .filter(worker -> !right.contains(worker))
                .sorted(AssessmentCanonical.UTF8_ORDER)
                .forEach(worker -> add(findings, Code.WORKER_SET_MISMATCH, prefix + worker));
    }

    private static void add(final List<DataResetAssessmentFinding> findings, final Code code, final String subject) {
        findings.add(new DataResetAssessmentFinding(code, subject));
    }
}
