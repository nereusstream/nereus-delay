package com.nereusstream.delay.assessment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceKind;
import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceRef;
import com.nereusstream.delay.assessment.DataResetInventory.AccessStatus;
import com.nereusstream.delay.assessment.DataResetInventory.ExternalRetentionRequirement;
import com.nereusstream.delay.assessment.DataResetInventory.ReplacementDisposition;
import com.nereusstream.delay.assessment.DataResetInventory.ResourceObservation;
import com.nereusstream.delay.assessment.DataResetInventoryCollector.ObligationEnumeration;
import com.nereusstream.delay.assessment.DataResetInventoryCollector.ScopeEnumeration;
import com.nereusstream.delay.assessment.DataResetInventoryCollector.WorkerEnumeration;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.TrustedUtcInterval;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DataResetInventoryCollectorTest {
    @Test
    void readsEveryScopedResourceExactlyOnceThroughItsRegisteredKindAdapter() {
        final DataResetAssessmentScope scope = scope();
        final AtomicInteger resourceReads = new AtomicInteger();
        final EnumMap<ResourceKind, DataResetInventoryCollector.ResourceInventoryReader> readers =
                new EnumMap<>(ResourceKind.class);
        for (ResourceKind kind : ResourceKind.values()) {
            readers.put(kind, (resource, requestedScope, time) -> {
                resourceReads.incrementAndGet();
                assertEquals(kind, resource.kind());
                assertEquals(scope, requestedScope);
                return complete(resource);
            });
        }
        final DataResetInventoryCollector collector = new DataResetInventoryCollector(
                (requestedScope, time) -> new ScopeEnumeration(requestedScope.scopeDigest(), true, digest(1)),
                readers,
                (requestedScope, time) -> new ObligationEnumeration(true, digest(2), List.of()),
                (requestedScope, time) -> new WorkerEnumeration(true, digest(3), List.of()));

        final DataResetInventory inventory = collector.read(scope, time());

        assertEquals(scope.resources().size(), resourceReads.get());
        assertEquals(scope.resources().size(), inventory.resourceObservations().size());
        assertEquals(true, inventory.scopeEnumerationComplete());
    }

    @Test
    void requiresOneReaderForEveryClosedResourceKind() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new DataResetInventoryCollector(
                        (scope, time) -> new ScopeEnumeration(scope.scopeDigest(), true, digest(1)),
                        Map.of(),
                        (scope, time) -> new ObligationEnumeration(true, digest(2), List.of()),
                        (scope, time) -> new WorkerEnumeration(true, digest(3), List.of())));
    }

    @Test
    void rejectsReaderIdentitySubstitution() {
        final DataResetAssessmentScope scope = scope();
        final EnumMap<ResourceKind, DataResetInventoryCollector.ResourceInventoryReader> readers =
                new EnumMap<>(ResourceKind.class);
        for (ResourceKind kind : ResourceKind.values()) {
            readers.put(kind, (resource, requestedScope, time) -> complete(resource));
        }
        final ResourceRef first = scope.resources().get(0);
        readers.put(
                first.kind(),
                (resource, requestedScope, time) ->
                        complete(new ResourceRef(resource.kind(), resource.identity() + "/substituted")));
        final DataResetInventoryCollector collector = new DataResetInventoryCollector(
                (requestedScope, time) -> new ScopeEnumeration(requestedScope.scopeDigest(), true, digest(1)),
                readers,
                (requestedScope, time) -> new ObligationEnumeration(true, digest(2), List.of()),
                (requestedScope, time) -> new WorkerEnumeration(true, digest(3), List.of()));

        assertThrows(IllegalArgumentException.class, () -> collector.read(scope, time()));
    }

    private static DataResetAssessmentScope scope() {
        final List<ResourceRef> resources = Arrays.stream(ResourceKind.values())
                .map(kind -> new ResourceRef(kind, "resource/" + kind.name()))
                .toList();
        return new DataResetAssessmentScope(
                "environment",
                EnvironmentClassification.EXISTING,
                "deployment",
                List.of(),
                List.of(),
                List.of(),
                resources,
                List.of());
    }

    private static ResourceObservation complete(final ResourceRef resource) {
        return new ResourceObservation(
                resource,
                AccessStatus.COMPLETE,
                ExternalRetentionRequirement.NONE,
                ReplacementDisposition.DISCARDABLE,
                digest(10 + resource.kind().ordinal()));
    }

    private static TrustedUtcInterval time() {
        final TrustedUtcIntervalEvidence evidence = new TrustedUtcIntervalEvidence(
                100,
                101,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                new byte[] {1},
                1,
                1,
                1,
                digest(50),
                0,
                null);
        return new TrustedUtcInterval(100, 101, true, evidence);
    }

    private static byte[] digest(final int value) {
        final byte[] result = new byte[32];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
