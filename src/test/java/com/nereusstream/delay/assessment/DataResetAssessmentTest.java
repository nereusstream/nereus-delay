package com.nereusstream.delay.assessment;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.assessment.DataResetAssessmentFinding.Code;
import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceKind;
import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceRef;
import com.nereusstream.delay.assessment.DataResetInventory.AccessStatus;
import com.nereusstream.delay.assessment.DataResetInventory.ExternalRetentionRequirement;
import com.nereusstream.delay.assessment.DataResetInventory.ObligationDisposition;
import com.nereusstream.delay.assessment.DataResetInventory.ObligationKind;
import com.nereusstream.delay.assessment.DataResetInventory.ObligationObservation;
import com.nereusstream.delay.assessment.DataResetInventory.ReplacementDisposition;
import com.nereusstream.delay.assessment.DataResetInventory.ResourceObservation;
import com.nereusstream.delay.assessment.DataResetInventory.WorkerObservation;
import com.nereusstream.delay.assessment.DataResetInventory.WorkerUpgradeStatus;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.TrustedUtcInterval;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DataResetAssessmentTest {
    private static final String PACKAGE_DIGEST = "0728ae89515d75858dbe96f8426a18bb5a59fb453d6a930737be37289979532f";
    private static final String SOURCE_BASELINE = "1fcc887f6553f6ed5a5f299109b760402d94573b";

    @Test
    void directReplacementPassAllowsDiscardableCurrentObligationsWithoutClaimingCutoverReadiness() {
        final DataResetAssessmentScope scope = scope();
        final DataResetInventory inventory = inventory(
                scope,
                true,
                completeResources(scope),
                List.of(new ObligationObservation(
                        "attempt-1", ObligationKind.UNCERTAIN, ObligationDisposition.DISCARDABLE_INTERNAL, digest(41))),
                true,
                completeWorkers());

        final DataResetAssessmentReceipt receipt =
                DataResetAssessmentEvaluator.evaluate(scope, inventory, PACKAGE_DIGEST, SOURCE_BASELINE);
        final DataResetAssessmentReceipt repeated =
                DataResetAssessmentEvaluator.evaluate(scope, inventory, PACKAGE_DIGEST, SOURCE_BASELINE);

        assertEquals(DataResetAssessmentOutcome.PASS_DIRECT_REPLACE, receipt.outcome());
        assertTrue(receipt.findings().isEmpty());
        assertArrayEquals(receipt.assessmentDigest(), repeated.assessmentDigest());
        assertEquals(
                "d418910edac77b9a31a4c19aff441e7b6cc6900ccc3cec8694fff570f8436899",
                Bytes.hex(receipt.assessmentDigest()));
        assertEquals(receipt.canonicalJson(), repeated.canonicalJson());
        assertTrue(receipt.canonicalJson().startsWith("{\"assessmentDigest\":"));
        assertTrue(receipt.canonicalJson().contains("\"assessmentSchema\":\"nereus-delay.data-reset-assessment\""));
        assertEquals(receipt.canonicalJson(), new String(receipt.canonicalJsonBytes(), StandardCharsets.UTF_8));
    }

    @Test
    void knownRetentionRequirementWinsOverOtherIncompleteEvidence() {
        final DataResetAssessmentScope scope = scope();
        final List<ResourceObservation> resources = new ArrayList<>(completeResources(scope));
        final ResourceObservation first = resources.get(0);
        resources.set(
                0,
                new ResourceObservation(
                        first.resource(),
                        AccessStatus.EVIDENCE_INCOMPLETE,
                        ExternalRetentionRequirement.REQUIRED,
                        ReplacementDisposition.MIGRATION_REQUIRED,
                        digest(51)));
        final DataResetInventory inventory = inventory(scope, false, resources, List.of(), false, List.of());

        final DataResetAssessmentReceipt receipt =
                DataResetAssessmentEvaluator.evaluate(scope, inventory, PACKAGE_DIGEST, SOURCE_BASELINE);

        assertEquals(DataResetAssessmentOutcome.MIGRATION_REQUIRED, receipt.outcome());
        assertTrue(receipt.findings().stream()
                .anyMatch(finding -> finding.code() == Code.EXTERNAL_RETENTION_NOT_RETAINABLE));
        assertTrue(receipt.findings().stream().anyMatch(finding -> finding.code() == Code.RESOURCE_ACCESS_INCOMPLETE));
        assertTrue(
                receipt.findings().stream().anyMatch(finding -> finding.code() == Code.OBSERVATION_TIME_UNQUALIFIED));
    }

    @Test
    void compatibleRetentionProducesAClosedPassWithoutGrantingGateC() {
        final DataResetAssessmentScope scope = scope();
        final List<ResourceObservation> resources = new ArrayList<>(completeResources(scope));
        final ResourceObservation first = resources.get(0);
        resources.set(
                0,
                new ResourceObservation(
                        first.resource(),
                        AccessStatus.COMPLETE,
                        ExternalRetentionRequirement.REQUIRED,
                        ReplacementDisposition.RETAIN_COMPATIBLE,
                        digest(52)));

        final DataResetAssessmentReceipt receipt = DataResetAssessmentEvaluator.evaluate(
                scope,
                inventory(scope, true, resources, List.of(), true, completeWorkers()),
                PACKAGE_DIGEST,
                SOURCE_BASELINE);

        assertEquals(DataResetAssessmentOutcome.PASS_RETAIN, receipt.outcome());
        assertTrue(receipt.outcome().decisionReady());
        assertTrue(receipt.findings().isEmpty());
    }

    @Test
    void missingScopeEvidenceAndUnknownWorkerRemainIncomplete() {
        final DataResetAssessmentScope scope = scope();
        final List<ResourceObservation> resources = new ArrayList<>(completeResources(scope));
        resources.remove(0);
        final DataResetInventory inventory = inventory(
                scope,
                true,
                resources,
                List.of(new ObligationObservation(
                        "attempt-2", ObligationKind.PUBLISHING, ObligationDisposition.UNKNOWN, digest(61))),
                true,
                List.of(new WorkerObservation("worker-a", WorkerUpgradeStatus.UNKNOWN, digest(62))));

        final DataResetAssessmentReceipt receipt =
                DataResetAssessmentEvaluator.evaluate(scope, inventory, PACKAGE_DIGEST, SOURCE_BASELINE);

        assertEquals(DataResetAssessmentOutcome.INCOMPLETE, receipt.outcome());
        assertTrue(receipt.findings().stream().anyMatch(finding -> finding.code() == Code.RESOURCE_SET_MISMATCH));
        assertTrue(
                receipt.findings().stream().anyMatch(finding -> finding.code() == Code.OBLIGATION_DISPOSITION_UNKNOWN));
        assertTrue(receipt.findings().stream().anyMatch(finding -> finding.code() == Code.WORKER_STATUS_UNKNOWN));
        assertTrue(receipt.findings().stream().anyMatch(finding -> finding.code() == Code.WORKER_SET_MISMATCH));
    }

    @Test
    void runnerReadsInventoryExactlyOnceAndDoesNotPersistImplicitly() {
        final DataResetAssessmentScope scope = scope();
        final TrustedUtcInterval observationTime = time(true);
        final DataResetInventory inventory =
                inventory(scope, true, completeResources(scope), List.of(), true, completeWorkers());
        final AtomicInteger reads = new AtomicInteger();
        final DataResetAssessmentRunner runner = new DataResetAssessmentRunner((requestedScope, requestedTime) -> {
            reads.incrementAndGet();
            assertEquals(scope, requestedScope);
            assertEquals(observationTime, requestedTime);
            return inventory;
        });

        final DataResetAssessmentReceipt receipt =
                runner.assess(scope, observationTime, PACKAGE_DIGEST, SOURCE_BASELINE);

        assertEquals(1, reads.get());
        assertEquals(DataResetAssessmentOutcome.PASS_DIRECT_REPLACE, receipt.outcome());
    }

    @Test
    void scopeEnumerationMustBeExplicitlyComplete() {
        final DataResetAssessmentScope scope = scope();
        final DataResetInventory inventory = new DataResetInventory(
                scope.scopeDigest(),
                false,
                digest(90),
                time(true),
                completeResources(scope),
                true,
                digest(91),
                List.of(),
                true,
                digest(92),
                completeWorkers());

        final DataResetAssessmentReceipt receipt =
                DataResetAssessmentEvaluator.evaluate(scope, inventory, PACKAGE_DIGEST, SOURCE_BASELINE);

        assertEquals(DataResetAssessmentOutcome.INCOMPLETE, receipt.outcome());
        assertTrue(
                receipt.findings().stream().anyMatch(finding -> finding.code() == Code.SCOPE_ENUMERATION_INCOMPLETE));
    }

    @Test
    void closedInputsRejectMissingKindsUnknownCompleteDecisionsAndDuplicateObservations() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DataResetAssessmentScope(
                        "env",
                        EnvironmentClassification.EXISTING,
                        "deployment",
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of(),
                        List.of()));
        final DataResetAssessmentScope scope = scope();
        assertThrows(
                IllegalArgumentException.class,
                () -> new DataResetAssessmentScope(
                        "env",
                        EnvironmentClassification.DISPOSABLE_LOCAL,
                        "deployment",
                        scope.tenantIds(),
                        scope.routeIds(),
                        scope.shardIds(),
                        scope.resources(),
                        scope.eligibleWorkerIds()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new DataResetAssessmentScope(
                        "env",
                        EnvironmentClassification.UNKNOWN,
                        "deployment",
                        scope.tenantIds(),
                        scope.routeIds(),
                        scope.shardIds(),
                        scope.resources(),
                        scope.eligibleWorkerIds()));
        final ResourceRef resource = scope.resources().get(0);
        assertThrows(
                IllegalArgumentException.class,
                () -> new ResourceObservation(
                        resource,
                        AccessStatus.COMPLETE,
                        ExternalRetentionRequirement.UNKNOWN,
                        ReplacementDisposition.DISCARDABLE,
                        digest(71)));
        final ResourceObservation duplicate = completeResources(scope).get(0);
        assertThrows(
                IllegalArgumentException.class,
                () -> inventory(scope, true, List.of(duplicate, duplicate), List.of(), true, completeWorkers()));
    }

    private static DataResetAssessmentScope scope() {
        final List<ResourceRef> resources = Arrays.stream(ResourceKind.values())
                .map(kind -> new ResourceRef(kind, "resource/" + kind.name().toLowerCase(java.util.Locale.ROOT)))
                .toList();
        return new DataResetAssessmentScope(
                "environment-a",
                EnvironmentClassification.EXISTING,
                "deployment-a",
                List.of("tenant-b", "tenant-a"),
                List.of("route-a"),
                List.of("route-a/0"),
                resources,
                List.of("worker-b", "worker-a"));
    }

    private static List<ResourceObservation> completeResources(final DataResetAssessmentScope scope) {
        return scope.resources().stream()
                .map(resource -> new ResourceObservation(
                        resource,
                        AccessStatus.COMPLETE,
                        ExternalRetentionRequirement.NONE,
                        resource.kind() == ResourceKind.RESOURCE_INCARNATION_REGISTRY
                                ? ReplacementDisposition.REINCARNATE
                                : ReplacementDisposition.DISCARDABLE,
                        digest(20 + resource.kind().ordinal())))
                .toList();
    }

    private static List<WorkerObservation> completeWorkers() {
        return List.of(
                new WorkerObservation("worker-a", WorkerUpgradeStatus.UPGRADEABLE, digest(31)),
                new WorkerObservation("worker-b", WorkerUpgradeStatus.UPGRADEABLE, digest(32)));
    }

    private static DataResetInventory inventory(
            final DataResetAssessmentScope scope,
            final boolean qualifiedTime,
            final List<ResourceObservation> resources,
            final List<ObligationObservation> obligations,
            final boolean workerEnumerationComplete,
            final List<WorkerObservation> workers) {
        return new DataResetInventory(
                scope.scopeDigest(),
                true,
                digest(90),
                time(qualifiedTime),
                resources,
                true,
                digest(91),
                obligations,
                workerEnumerationComplete,
                digest(92),
                workers);
    }

    private static TrustedUtcInterval time(final boolean qualified) {
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(
                1_000,
                1_002,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                new byte[] {1},
                1,
                2,
                3,
                digest(93),
                0,
                null);
        return new TrustedUtcInterval(1_000, 1_002, qualified, evidence);
    }

    private static byte[] digest(final int value) {
        final byte[] result = new byte[32];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
