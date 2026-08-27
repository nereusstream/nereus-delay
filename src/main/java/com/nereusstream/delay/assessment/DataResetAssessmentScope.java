package com.nereusstream.delay.assessment;

import com.nereusstream.delay.protocol.Bytes;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Exact environment scope for one read-only data reset assessment. */
public record DataResetAssessmentScope(
        String environmentId,
        EnvironmentClassification environmentClassification,
        String deploymentId,
        List<String> tenantIds,
        List<String> routeIds,
        List<String> shardIds,
        List<ResourceRef> resources,
        List<String> eligibleWorkerIds) {
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-data-reset-assessment-scope\0");

    public DataResetAssessmentScope {
        environmentId = AssessmentCanonical.text(environmentId, "environmentId");
        Objects.requireNonNull(environmentClassification, "environmentClassification");
        if (!environmentClassification.requiresDeploymentSafetyAssessment()) {
            throw new IllegalArgumentException(
                    "data reset assessment requires an existing, staging, or production environment");
        }
        deploymentId = AssessmentCanonical.text(deploymentId, "deploymentId");
        tenantIds = AssessmentCanonical.sortedUniqueText(tenantIds, "tenantId");
        routeIds = AssessmentCanonical.sortedUniqueText(routeIds, "routeId");
        shardIds = AssessmentCanonical.sortedUniqueText(shardIds, "shardId");
        eligibleWorkerIds = AssessmentCanonical.sortedUniqueText(eligibleWorkerIds, "eligibleWorkerId");
        Objects.requireNonNull(resources, "resources");
        final List<ResourceRef> sortedResources = new ArrayList<>(resources);
        sortedResources.sort(Comparator.naturalOrder());
        final Set<ResourceKind> observedKinds = EnumSet.noneOf(ResourceKind.class);
        final Set<ResourceRef> unique = new HashSet<>();
        for (ResourceRef resource : sortedResources) {
            Objects.requireNonNull(resource, "resource");
            if (!unique.add(resource)) {
                throw new IllegalArgumentException("duplicate assessment resource: " + resource.subject());
            }
            observedKinds.add(resource.kind());
        }
        if (!observedKinds.equals(EnumSet.allOf(ResourceKind.class))) {
            final Set<ResourceKind> missing = EnumSet.allOf(ResourceKind.class);
            missing.removeAll(observedKinds);
            throw new IllegalArgumentException("assessment scope is missing resource kinds: " + missing);
        }
        resources = List.copyOf(sortedResources);
    }

    public byte[] scopeDigest() {
        return Bytes.sha256(DIGEST_DOMAIN, canonicalJson().getBytes(StandardCharsets.UTF_8));
    }

    public String canonicalJson() {
        final StringBuilder json = new StringBuilder(1024);
        json.append("{\"deploymentId\":").append(AssessmentCanonical.quote(deploymentId));
        json.append(",\"eligibleWorkerIds\":").append(AssessmentCanonical.strings(eligibleWorkerIds));
        json.append(",\"environmentClassification\":")
                .append(AssessmentCanonical.quote(environmentClassification.name()));
        json.append(",\"environmentId\":").append(AssessmentCanonical.quote(environmentId));
        json.append(",\"resources\":[");
        for (int index = 0; index < resources.size(); index++) {
            if (index != 0) {
                json.append(',');
            }
            json.append(resources.get(index).canonicalJson());
        }
        json.append(']');
        json.append(",\"routeIds\":").append(AssessmentCanonical.strings(routeIds));
        json.append(",\"shardIds\":").append(AssessmentCanonical.strings(shardIds));
        json.append(",\"tenantIds\":").append(AssessmentCanonical.strings(tenantIds));
        return json.append('}').toString();
    }

    public enum ResourceKind {
        COMMAND_TOPIC,
        SYSTEM_TOPIC,
        ROCKSDB_STORE,
        CHECKPOINT_CATALOG,
        PROFILE_OXIA_STATE,
        RUNTIME_POLICY_STATE,
        PAYLOAD_RESERVATION_OBJECT_STATE,
        PULSAR_ATTEMPT_JOURNAL,
        EVIDENCE_TOPIC_CURSOR,
        QUERY_DEDUPE_STATE,
        OBLIGATION_INDEX,
        RESOURCE_INCARNATION_REGISTRY,
        WORKER_REGISTRY
    }

    /** One scoped runtime resource identity; the identity is opaque but canonical. */
    public record ResourceRef(ResourceKind kind, String identity) implements Comparable<ResourceRef> {
        public ResourceRef {
            Objects.requireNonNull(kind, "kind");
            identity = AssessmentCanonical.text(identity, "resource identity");
        }

        public String subject() {
            return kind.name() + ':' + identity;
        }

        String canonicalJson() {
            return "{\"identity\":" + AssessmentCanonical.quote(identity) + ",\"kind\":"
                    + AssessmentCanonical.quote(kind.name()) + '}';
        }

        @Override
        public int compareTo(final ResourceRef other) {
            final int kindComparison = kind.compareTo(other.kind);
            return kindComparison != 0
                    ? kindComparison
                    : AssessmentCanonical.UTF8_ORDER.compare(identity, other.identity);
        }
    }
}
