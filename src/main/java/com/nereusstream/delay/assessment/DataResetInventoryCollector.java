package com.nereusstream.delay.assessment;

import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceKind;
import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceRef;
import com.nereusstream.delay.assessment.DataResetInventory.ObligationObservation;
import com.nereusstream.delay.assessment.DataResetInventory.ResourceObservation;
import com.nereusstream.delay.assessment.DataResetInventory.WorkerObservation;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.runtime.TrustedUtcInterval;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Composes resource-kind-specific read-only adapters into one exact G0 inventory.
 *
 * <p>Adapters must convert inaccessible or incomplete observations into closed UNKNOWN/incomplete projections with
 * evidence. Throwing aborts receipt production and therefore fails closed.</p>
 */
public final class DataResetInventoryCollector implements DataResetInventoryReader {
    private final ScopeEnumerationReader scopeReader;
    private final Map<ResourceKind, ResourceInventoryReader> resourceReaders;
    private final ObligationInventoryReader obligationReader;
    private final WorkerInventoryReader workerReader;

    public DataResetInventoryCollector(
            final ScopeEnumerationReader scopeReader,
            final Map<ResourceKind, ResourceInventoryReader> resourceReaders,
            final ObligationInventoryReader obligationReader,
            final WorkerInventoryReader workerReader) {
        this.scopeReader = Objects.requireNonNull(scopeReader, "scopeReader");
        Objects.requireNonNull(resourceReaders, "resourceReaders");
        final EnumMap<ResourceKind, ResourceInventoryReader> closedReaders = new EnumMap<>(ResourceKind.class);
        resourceReaders.forEach((kind, reader) -> closedReaders.put(
                Objects.requireNonNull(kind, "resource reader kind"),
                Objects.requireNonNull(reader, "resource reader")));
        if (closedReaders.size() != ResourceKind.values().length) {
            final java.util.EnumSet<ResourceKind> missing = java.util.EnumSet.allOf(ResourceKind.class);
            missing.removeAll(closedReaders.keySet());
            throw new IllegalArgumentException("missing resource inventory readers: " + missing);
        }
        this.resourceReaders = Map.copyOf(closedReaders);
        this.obligationReader = Objects.requireNonNull(obligationReader, "obligationReader");
        this.workerReader = Objects.requireNonNull(workerReader, "workerReader");
    }

    @Override
    public DataResetInventory read(final DataResetAssessmentScope scope, final TrustedUtcInterval observationTime) {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(observationTime, "observationTime");
        final ScopeEnumeration scopeEnumeration =
                Objects.requireNonNull(scopeReader.read(scope, observationTime), "scope enumeration");
        final List<ResourceObservation> resources =
                new ArrayList<>(scope.resources().size());
        for (ResourceRef resource : scope.resources()) {
            final ResourceObservation observation = Objects.requireNonNull(
                    resourceReaders.get(resource.kind()).read(resource, scope, observationTime),
                    "resource observation");
            if (!resource.equals(observation.resource())) {
                throw new IllegalArgumentException("resource reader returned another identity: " + resource.subject());
            }
            resources.add(observation);
        }
        final ObligationEnumeration obligations =
                Objects.requireNonNull(obligationReader.read(scope, observationTime), "obligation enumeration");
        final WorkerEnumeration workers =
                Objects.requireNonNull(workerReader.read(scope, observationTime), "worker enumeration");
        return new DataResetInventory(
                scopeEnumeration.observedScopeDigest(),
                scopeEnumeration.complete(),
                scopeEnumeration.evidenceSha256(),
                observationTime,
                resources,
                obligations.complete(),
                obligations.evidenceSha256(),
                obligations.observations(),
                workers.complete(),
                workers.evidenceSha256(),
                workers.observations());
    }

    @FunctionalInterface
    public interface ScopeEnumerationReader {
        ScopeEnumeration read(DataResetAssessmentScope scope, TrustedUtcInterval observationTime);
    }

    @FunctionalInterface
    public interface ResourceInventoryReader {
        ResourceObservation read(
                ResourceRef resource, DataResetAssessmentScope scope, TrustedUtcInterval observationTime);
    }

    @FunctionalInterface
    public interface ObligationInventoryReader {
        ObligationEnumeration read(DataResetAssessmentScope scope, TrustedUtcInterval observationTime);
    }

    @FunctionalInterface
    public interface WorkerInventoryReader {
        WorkerEnumeration read(DataResetAssessmentScope scope, TrustedUtcInterval observationTime);
    }

    public record ScopeEnumeration(byte[] observedScopeDigest, boolean complete, byte[] evidenceSha256) {
        public ScopeEnumeration {
            observedScopeDigest = AssessmentCanonical.digest(observedScopeDigest, "observedScopeDigest");
            evidenceSha256 = AssessmentCanonical.digest(evidenceSha256, "scope enumeration evidenceSha256");
        }

        @Override
        public byte[] observedScopeDigest() {
            return Bytes.copy(observedScopeDigest);
        }

        @Override
        public byte[] evidenceSha256() {
            return Bytes.copy(evidenceSha256);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof ScopeEnumeration that
                    && complete == that.complete
                    && Arrays.equals(observedScopeDigest, that.observedScopeDigest)
                    && Arrays.equals(evidenceSha256, that.evidenceSha256);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(observedScopeDigest), complete, Arrays.hashCode(evidenceSha256));
        }
    }

    public record ObligationEnumeration(
            boolean complete, byte[] evidenceSha256, List<ObligationObservation> observations) {
        public ObligationEnumeration {
            evidenceSha256 = AssessmentCanonical.digest(evidenceSha256, "obligation enumeration evidenceSha256");
            observations = List.copyOf(Objects.requireNonNull(observations, "obligation observations"));
            observations.forEach(observation -> Objects.requireNonNull(observation, "obligation observation"));
        }

        @Override
        public byte[] evidenceSha256() {
            return Bytes.copy(evidenceSha256);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof ObligationEnumeration that
                    && complete == that.complete
                    && Arrays.equals(evidenceSha256, that.evidenceSha256)
                    && observations.equals(that.observations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(complete, Arrays.hashCode(evidenceSha256), observations);
        }
    }

    public record WorkerEnumeration(boolean complete, byte[] evidenceSha256, List<WorkerObservation> observations) {
        public WorkerEnumeration {
            evidenceSha256 = AssessmentCanonical.digest(evidenceSha256, "worker enumeration evidenceSha256");
            observations = List.copyOf(Objects.requireNonNull(observations, "worker observations"));
            observations.forEach(observation -> Objects.requireNonNull(observation, "worker observation"));
        }

        @Override
        public byte[] evidenceSha256() {
            return Bytes.copy(evidenceSha256);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof WorkerEnumeration that
                    && complete == that.complete
                    && Arrays.equals(evidenceSha256, that.evidenceSha256)
                    && observations.equals(that.observations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(complete, Arrays.hashCode(evidenceSha256), observations);
        }
    }
}
