package com.nereusstream.delay.assessment;

import com.nereusstream.delay.assessment.DataResetAssessmentScope.ResourceRef;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.TrustedUtcInterval;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Immutable output of environment-specific read-only inventory adapters. */
public record DataResetInventory(
        byte[] scopeDigest,
        boolean scopeEnumerationComplete,
        byte[] scopeEvidenceSha256,
        TrustedUtcInterval observationTime,
        List<ResourceObservation> resourceObservations,
        boolean obligationEnumerationComplete,
        byte[] obligationEvidenceSha256,
        List<ObligationObservation> obligations,
        boolean workerEnumerationComplete,
        byte[] workerEvidenceSha256,
        List<WorkerObservation> workers) {
    public DataResetInventory {
        scopeDigest = AssessmentCanonical.digest(scopeDigest, "scopeDigest");
        scopeEvidenceSha256 = AssessmentCanonical.digest(scopeEvidenceSha256, "scopeEvidenceSha256");
        Objects.requireNonNull(observationTime, "observationTime");
        final TrustedUtcIntervalEvidence timeEvidence = observationTime.evidence();
        if (observationTime.earliestEpochMs() != timeEvidence.earliestEpochMs()
                || observationTime.latestEpochMs() != timeEvidence.latestEpochMs()) {
            throw new IllegalArgumentException("observation time does not match trusted evidence interval");
        }
        resourceObservations = sortedUnique(
                resourceObservations, Comparator.comparing(ResourceObservation::resource), "resource observation");
        obligationEvidenceSha256 = AssessmentCanonical.digest(obligationEvidenceSha256, "obligationEvidenceSha256");
        obligations = sortedUnique(obligations, Comparator.naturalOrder(), "obligation observation");
        workerEvidenceSha256 = AssessmentCanonical.digest(workerEvidenceSha256, "workerEvidenceSha256");
        workers = sortedUnique(workers, Comparator.naturalOrder(), "worker observation");
    }

    @Override
    public byte[] scopeDigest() {
        return Bytes.copy(scopeDigest);
    }

    @Override
    public byte[] obligationEvidenceSha256() {
        return Bytes.copy(obligationEvidenceSha256);
    }

    @Override
    public byte[] scopeEvidenceSha256() {
        return Bytes.copy(scopeEvidenceSha256);
    }

    @Override
    public byte[] workerEvidenceSha256() {
        return Bytes.copy(workerEvidenceSha256);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof DataResetInventory that
                && Arrays.equals(scopeDigest, that.scopeDigest)
                && scopeEnumerationComplete == that.scopeEnumerationComplete
                && Arrays.equals(scopeEvidenceSha256, that.scopeEvidenceSha256)
                && observationTime.equals(that.observationTime)
                && resourceObservations.equals(that.resourceObservations)
                && obligationEnumerationComplete == that.obligationEnumerationComplete
                && Arrays.equals(obligationEvidenceSha256, that.obligationEvidenceSha256)
                && obligations.equals(that.obligations)
                && workerEnumerationComplete == that.workerEnumerationComplete
                && Arrays.equals(workerEvidenceSha256, that.workerEvidenceSha256)
                && workers.equals(that.workers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                Arrays.hashCode(scopeDigest),
                scopeEnumerationComplete,
                Arrays.hashCode(scopeEvidenceSha256),
                observationTime,
                resourceObservations,
                obligationEnumerationComplete,
                Arrays.hashCode(obligationEvidenceSha256),
                obligations,
                workerEnumerationComplete,
                Arrays.hashCode(workerEvidenceSha256),
                workers);
    }

    String canonicalJson() {
        final StringBuilder json = new StringBuilder(4096);
        json.append("{\"obligationEnumerationComplete\":").append(obligationEnumerationComplete);
        json.append(",\"obligationEvidenceSha256\":")
                .append(AssessmentCanonical.quote(AssessmentCanonical.hex(obligationEvidenceSha256)));
        json.append(",\"obligations\":[");
        append(
                json,
                obligations.stream().map(ObligationObservation::canonicalJson).toList());
        json.append(']');
        json.append(",\"observationTime\":").append(observationTimeJson());
        json.append(",\"resourceObservations\":[");
        append(
                json,
                resourceObservations.stream()
                        .map(ResourceObservation::canonicalJson)
                        .toList());
        json.append(']');
        json.append(",\"scopeDigest\":").append(AssessmentCanonical.quote(AssessmentCanonical.hex(scopeDigest)));
        json.append(",\"scopeEnumerationComplete\":").append(scopeEnumerationComplete);
        json.append(",\"scopeEvidenceSha256\":")
                .append(AssessmentCanonical.quote(AssessmentCanonical.hex(scopeEvidenceSha256)));
        json.append(",\"workerEnumerationComplete\":").append(workerEnumerationComplete);
        json.append(",\"workerEvidenceSha256\":")
                .append(AssessmentCanonical.quote(AssessmentCanonical.hex(workerEvidenceSha256)));
        json.append(",\"workers\":[");
        append(json, workers.stream().map(WorkerObservation::canonicalJson).toList());
        return json.append("]}").toString();
    }

    private String observationTimeJson() {
        final TrustedUtcIntervalEvidence evidence = observationTime.evidence();
        return "{\"earliestEpochMs\":" + AssessmentCanonical.quote(Long.toString(observationTime.earliestEpochMs()))
                + ",\"evidenceSha256\":"
                + AssessmentCanonical.quote(AssessmentCanonical.hex(Bytes.sha256(evidence.canonicalBytes())))
                + ",\"latestEpochMs\":"
                + AssessmentCanonical.quote(Long.toString(observationTime.latestEpochMs()))
                + ",\"qualified\":" + observationTime.qualified() + ",\"source\":"
                + AssessmentCanonical.quote(evidence.source().name()) + '}';
    }

    private static void append(final StringBuilder target, final List<String> values) {
        for (int index = 0; index < values.size(); index++) {
            if (index != 0) {
                target.append(',');
            }
            target.append(values.get(index));
        }
    }

    private static <T> List<T> sortedUnique(
            final List<T> values, final Comparator<? super T> comparator, final String name) {
        Objects.requireNonNull(values, name + 's');
        final List<T> result = new ArrayList<>(values);
        result.forEach(value -> Objects.requireNonNull(value, name));
        result.sort(comparator);
        for (int index = 1; index < result.size(); index++) {
            if (comparator.compare(result.get(index - 1), result.get(index)) == 0) {
                throw new IllegalArgumentException("duplicate " + name);
            }
        }
        return List.copyOf(result);
    }

    public enum AccessStatus {
        COMPLETE,
        INACCESSIBLE,
        IDENTITY_UNVERIFIED,
        EVIDENCE_INCOMPLETE
    }

    public enum ExternalRetentionRequirement {
        NONE,
        REQUIRED,
        UNKNOWN
    }

    public enum ReplacementDisposition {
        DISCARDABLE,
        REINCARNATE,
        RETAIN_COMPATIBLE,
        MIGRATION_REQUIRED,
        UNKNOWN
    }

    public enum ObligationKind {
        PUBLISHING,
        UNCERTAIN
    }

    public enum ObligationDisposition {
        DISCARDABLE_INTERNAL,
        PRESERVE_ACROSS_GENERATION,
        UNKNOWN
    }

    public enum WorkerUpgradeStatus {
        UPGRADEABLE,
        NOT_UPGRADEABLE,
        UNKNOWN
    }

    public record ResourceObservation(
            ResourceRef resource,
            AccessStatus accessStatus,
            ExternalRetentionRequirement externalRetention,
            ReplacementDisposition replacementDisposition,
            byte[] evidenceSha256) {
        public ResourceObservation {
            Objects.requireNonNull(resource, "resource");
            Objects.requireNonNull(accessStatus, "accessStatus");
            Objects.requireNonNull(externalRetention, "externalRetention");
            Objects.requireNonNull(replacementDisposition, "replacementDisposition");
            evidenceSha256 = AssessmentCanonical.digest(evidenceSha256, "resource evidenceSha256");
            if (accessStatus == AccessStatus.COMPLETE
                    && (externalRetention == ExternalRetentionRequirement.UNKNOWN
                            || replacementDisposition == ReplacementDisposition.UNKNOWN)) {
                throw new IllegalArgumentException("complete resource observation cannot contain unknown decisions");
            }
        }

        @Override
        public byte[] evidenceSha256() {
            return Bytes.copy(evidenceSha256);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof ResourceObservation that
                    && resource.equals(that.resource)
                    && accessStatus == that.accessStatus
                    && externalRetention == that.externalRetention
                    && replacementDisposition == that.replacementDisposition
                    && Arrays.equals(evidenceSha256, that.evidenceSha256);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    resource, accessStatus, externalRetention, replacementDisposition, Arrays.hashCode(evidenceSha256));
        }

        String canonicalJson() {
            return "{\"accessStatus\":" + AssessmentCanonical.quote(accessStatus.name())
                    + ",\"evidenceSha256\":" + AssessmentCanonical.quote(AssessmentCanonical.hex(evidenceSha256))
                    + ",\"externalRetention\":" + AssessmentCanonical.quote(externalRetention.name())
                    + ",\"identity\":" + AssessmentCanonical.quote(resource.identity()) + ",\"kind\":"
                    + AssessmentCanonical.quote(resource.kind().name()) + ",\"replacementDisposition\":"
                    + AssessmentCanonical.quote(replacementDisposition.name()) + '}';
        }
    }

    public record ObligationObservation(
            String identity, ObligationKind kind, ObligationDisposition disposition, byte[] evidenceSha256)
            implements Comparable<ObligationObservation> {
        public ObligationObservation {
            identity = AssessmentCanonical.text(identity, "obligation identity");
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(disposition, "disposition");
            evidenceSha256 = AssessmentCanonical.digest(evidenceSha256, "obligation evidenceSha256");
        }

        @Override
        public byte[] evidenceSha256() {
            return Bytes.copy(evidenceSha256);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof ObligationObservation that
                    && identity.equals(that.identity)
                    && kind == that.kind
                    && disposition == that.disposition
                    && Arrays.equals(evidenceSha256, that.evidenceSha256);
        }

        @Override
        public int hashCode() {
            return Objects.hash(identity, kind, disposition, Arrays.hashCode(evidenceSha256));
        }

        String canonicalJson() {
            return "{\"disposition\":" + AssessmentCanonical.quote(disposition.name())
                    + ",\"evidenceSha256\":" + AssessmentCanonical.quote(AssessmentCanonical.hex(evidenceSha256))
                    + ",\"identity\":" + AssessmentCanonical.quote(identity) + ",\"kind\":"
                    + AssessmentCanonical.quote(kind.name()) + '}';
        }

        @Override
        public int compareTo(final ObligationObservation other) {
            final int kindComparison = kind.compareTo(other.kind);
            return kindComparison != 0
                    ? kindComparison
                    : AssessmentCanonical.UTF8_ORDER.compare(identity, other.identity);
        }
    }

    public record WorkerObservation(String workerId, WorkerUpgradeStatus upgradeStatus, byte[] evidenceSha256)
            implements Comparable<WorkerObservation> {
        public WorkerObservation {
            workerId = AssessmentCanonical.text(workerId, "workerId");
            Objects.requireNonNull(upgradeStatus, "upgradeStatus");
            evidenceSha256 = AssessmentCanonical.digest(evidenceSha256, "worker evidenceSha256");
        }

        @Override
        public byte[] evidenceSha256() {
            return Bytes.copy(evidenceSha256);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof WorkerObservation that
                    && workerId.equals(that.workerId)
                    && upgradeStatus == that.upgradeStatus
                    && Arrays.equals(evidenceSha256, that.evidenceSha256);
        }

        @Override
        public int hashCode() {
            return Objects.hash(workerId, upgradeStatus, Arrays.hashCode(evidenceSha256));
        }

        String canonicalJson() {
            return "{\"evidenceSha256\":" + AssessmentCanonical.quote(AssessmentCanonical.hex(evidenceSha256))
                    + ",\"upgradeStatus\":" + AssessmentCanonical.quote(upgradeStatus.name())
                    + ",\"workerId\":" + AssessmentCanonical.quote(workerId) + '}';
        }

        @Override
        public int compareTo(final WorkerObservation other) {
            return AssessmentCanonical.UTF8_ORDER.compare(workerId, other.workerId);
        }
    }
}
