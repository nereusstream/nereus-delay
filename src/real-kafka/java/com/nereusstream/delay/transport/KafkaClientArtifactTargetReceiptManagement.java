package com.nereusstream.delay.transport;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.DeliveryCapabilitySemantic;
import com.nereusstream.delay.protocol.KafkaReceiptSlotResource;
import com.nereusstream.delay.protocol.OutcomeCapability;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.ProfileSemanticEnvelope;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetChannelControlRequest;
import com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact;
import com.nereusstream.delay.protocol.TargetKafkaReceiptProtection;
import com.nereusstream.delay.protocol.TargetKafkaReceiptManagementImage;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog;
import com.nereusstream.delay.runtime.TargetKafkaReceiptProtectionTrust;
import java.security.PrivateKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

/** Actual operator-only Kafka configuration/ACL backing for Registry receipt protection; no deletion or relaxation API. */
public final class KafkaClientArtifactTargetReceiptManagement implements AutoCloseable {
    private static final Map<String, String> REQUIRED_POLICY = Map.of(
            "cleanup.policy", "delete", "retention.ms", "-1", "retention.bytes", "-1",
            "message.timestamp.type", "LogAppendTime", "unclean.leader.election.enable", "false");
    private static final Set<AclOperation> MANAGEMENT_OPERATIONS = Set.of(
            AclOperation.DELETE, AclOperation.ALTER, AclOperation.ALTER_CONFIGS, AclOperation.CREATE);
    private static final Set<AclOperation> WORKER_OPERATIONS = Set.of(AclOperation.READ, AclOperation.WRITE, AclOperation.DESCRIBE);
    private final Admin admin;
    private final TargetKafkaReceiptManagementImage deployment;
    private final PrivateKey configuredControlKey;
    private final java.util.function.Supplier<ControlAuthorizationContext> currentOperator;
    private OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
    private TargetKafkaReceiptProtectionTrust trust;
    private final Duration timeout;
    private final int maximumAcls;
    private final int maximumBrokers;

    public record Scope(KafkaReceiptSlotResource resource, String physicalTopic, int slotsPerShard,
            int physicalPartition, long evidenceGeneration, ProfileRef capability, byte[] controlScopeRef) {
        public Scope {
            // This is a resource shape only; no sampling, protection or Broker permission is granted here.
            new com.nereusstream.delay.protocol.TargetKafkaReceiptBinding(physicalTopic, resource, physicalTopic,
                    slotsPerShard, physicalPartition, 0, controlScopeRef, controlScopeRef);
            controlScopeRef = Bytes.copy(controlScopeRef);
            if (evidenceGeneration == 0) { throw new IllegalArgumentException("unassigned receipt evidence generation"); }
        }
        @Override public byte[] controlScopeRef() { return Bytes.copy(controlScopeRef); }
    }

    /** Opens the actual Admin using the configured private credentials, bound to the source-bound deployment image. */
    public static KafkaClientArtifactTargetReceiptManagement open(Map<String, Object> adminConfiguration,
            TargetKafkaReceiptManagementImage deployment, PrivateKey configuredControlKey,
            java.util.function.Supplier<ControlAuthorizationContext> currentOperator, Duration timeout,
            int maximumAcls, int maximumBrokers) {
        final var configuration = Map.copyOf(adminConfiguration);
        if (!Arrays.equals(configurationFingerprint(configuration), deployment.configuredCredentialFingerprint())) {
            throw new IllegalArgumentException("actual Admin configuration differs from the authenticated deployment root");
        }
        // Configuration and the source-bound scope are mandatory. An externally constructed Admin cannot self-assert credentials.
        final Admin actual = Admin.create(configuration);
        try { return new KafkaClientArtifactTargetReceiptManagement(actual, deployment, configuredControlKey,
                currentOperator, timeout, maximumAcls, maximumBrokers); }
        catch (RuntimeException | Error failure) { actual.close(timeout); throw failure; }
    }

    public static byte[] configurationFingerprint(Map<String, Object> configuration) {
        final var entries = new TreeMap<String, Object>(configuration);
        final byte[] image = CanonicalProtobuf.message(out -> {
            for (var entry : entries.entrySet()) {
                if (!(entry.getValue() instanceof String || entry.getValue() instanceof Number
                        || entry.getValue() instanceof Boolean)) {
                    throw new IllegalArgumentException("receipt Admin requires canonical scalar configuration values");
                }
                CanonicalProtobuf.bytes(out, 1, CanonicalProtobuf.message(e -> {
                    CanonicalProtobuf.bytes(e, 1, Bytes.utf8(entry.getKey()));
                    CanonicalProtobuf.bytes(e, 2, Bytes.utf8(entry.getValue().toString()));
                }));
            }
        });
        return Bytes.sha256(Bytes.utf8("nereus-delay-target-kafka-receipt-admin-configuration\0"), image);
    }

    private KafkaClientArtifactTargetReceiptManagement(Admin admin, TargetKafkaReceiptManagementImage deployment,
            PrivateKey key, java.util.function.Supplier<ControlAuthorizationContext> currentOperator, Duration timeout,
            int maximumAcls, int maximumBrokers) {
        this.admin = admin; this.deployment = Objects.requireNonNull(deployment, "configured source-bound deployment root");
        this.configuredControlKey = Objects.requireNonNull(key, "actual configured CONTROL private key");
        this.currentOperator = Objects.requireNonNull(currentOperator, "actual authenticated CONTROL identity resolver");
        this.timeout = Objects.requireNonNull(timeout, "finite management deadline");
        if (timeout.isNegative() || timeout.isZero() || timeout.compareTo(Duration.ofSeconds(60)) > 0
                || maximumAcls <= 0 || maximumAcls > 4096 || maximumBrokers <= 0 || maximumBrokers > 256) {
            throw new IllegalArgumentException("receipt management requires finite positive RPC/image bounds");
        }
        this.maximumAcls = maximumAcls; this.maximumBrokers = maximumBrokers;
    }

    /** Closes the startup dependency loop once: trust delegates to this actual backing and the catalog uses that trust. */
    public synchronized void attach(OxiaSyncTargetKafkaReceiptProtectionCatalog actualCatalog,
            TargetKafkaReceiptProtectionTrust actualTrust) {
        if (catalog != null) { throw new IllegalStateException("receipt management catalog is already attached"); }
        catalog = Objects.requireNonNull(actualCatalog, "actual session-fenced catalog");
        trust = Objects.requireNonNull(actualTrust, "registered CONTROL deployment trust");
    }

    /** Runtime entry: establish actual restrictions, authenticate/control-sign, exact durable publication, then expose protection. */
    public CompletableFuture<TargetKafkaReceiptControlArtifact> establish(Scope scope, PreparedControlOperation control,
            SourcePosition at, PrivateKey originalControlKey, TargetKafkaReceiptProtectionTrust trust,
            OxiaSyncTargetKafkaReceiptProtectionCatalog catalog, Executor io) {
        final var result = new CompletableFuture<TargetKafkaReceiptControlArtifact>();
        Objects.requireNonNull(io, "caller bounded management I/O executor").execute(() -> {
            try { result.complete(establishNow(scope, control, at, originalControlKey, trust, catalog)); }
            catch (RuntimeException | Error failure) {
                result.completeExceptionally(failure);
                if (failure instanceof Error fatal) { throw fatal; }
            }
        });
        return result;
    }

    /** The restricted Worker gets only this typed publication operation, never CONTROL private key or Admin mutation access. */
    public KafkaClientArtifactTargetReceiptProvider.AnchorPublisher anchorPublisher(
            TargetKafkaReceiptControlArtifact protection, PrivateKey originalControlKey,
            TargetKafkaReceiptProtectionTrust trust, OxiaSyncTargetKafkaReceiptProtectionCatalog catalog, Executor io) {
        trust.requireIssuerSignature(TargetKafkaReceiptControlArtifact.signProtection(protection.protection(),
                protection.control(), protection.authorizedAt(), originalControlKey));
        requireCurrent(protection.protection(), protection.control(), protection.authorizedAt());
        return (actual, targetPhysicalTopic) -> {
            final var published = new CompletableFuture<com.nereusstream.delay.protocol.TargetKafkaReceiptBinding>();
            io.execute(() -> {
                try {
                    requireCurrent(protection.protection(), protection.control(), protection.authorizedAt());
                    if (!Arrays.equals(actual.protectionDigest(), protection.digest())) {
                        throw new IllegalArgumentException("actual sample belongs to another original protected epoch");
                    }
                    final var binding = actual.binding(targetPhysicalTopic, protection.protection().slotsPerShard());
                    final var signed = TargetKafkaReceiptControlArtifact.signAnchor(actual.canonicalBytes(), protection.digest(),
                            protection.control(), protection.authorizedAt(), originalControlKey);
                    catalog.publishAnchor(signed, protection, binding);
                    requireCurrent(protection.protection(), protection.control(), protection.authorizedAt());
                    published.complete(binding);
                } catch (RuntimeException | Error failure) {
                    published.completeExceptionally(failure);
                    if (failure instanceof Error fatal) { throw fatal; }
                }
            });
            return published;
        };
    }

    private TargetKafkaReceiptControlArtifact establishNow(Scope scope, PreparedControlOperation control, SourcePosition at,
            PrivateKey key, TargetKafkaReceiptProtectionTrust trust, OxiaSyncTargetKafkaReceiptProtectionCatalog catalog) {
        requireAttached(catalog, trust, key);
        requireCapability(scope); requireControlScope(scope, control);
        final var prior = catalog.findManagement(deployment.topic());
        final var management = prior.isEmpty() ? TargetKafkaReceiptControlArtifact.signManagement(deployment, control, at, key)
                : prior.orElseThrow().artifact();
        if (!Arrays.equals(management.image(), deployment.canonicalBytes())) {
            throw new IllegalStateException("configured deployment cannot replace a retained topic-management epoch");
        }
        trust.requireManagementIssuer(management, operator());
        // The durable deployment/physical capacity reservation precedes all Admin effects.
        final var installation = catalog.beginManagement(management, operator());
        final long deadline = System.nanoTime() + timeout.toNanos();
        if (!installation.fresh()) {
            if (installation.management().state() != OxiaSyncTargetKafkaReceiptProtectionCatalog.ACTIVE) {
                throw new IllegalStateException("historical INSTALLING/BROKEN cannot resume uncertain Admin requests");
            }
            final var original = catalog.findProtection(scope.resource(), scope.evidenceGeneration()).orElse(null);
            if (original != null) { requireCurrent(original.protection(), control, at); return original; }
            // Another slot may adopt the original healthy whole-topic epoch, without replaying management actions.
            final var policy = observe(scope, deadline, true);
            if (!Arrays.equals(policy, installation.management().policy())) {
                breakEpoch(management, "ACTIVE topic policy changed");
                throw new IllegalStateException("original ACTIVE topic policy continuity was lost");
            }
            return catalog.publishProtection(TargetKafkaReceiptControlArtifact.signProtection(
                    image(scope, management, policy), control, at, key));
        }
        // Observe UUID and declared isolation before mutation. Any interrupted/uncertain action holds this original epoch.
        try {
        observe(scope, deadline, false);
        // Denies take precedence over legacy broad Worker allows. No existing ACL is deleted or broadened outside receipt use.
        final var acls = requiredWorkerAcls(scope.physicalTopic(), Set.copyOf(deployment.workerPrincipals()));
        await(admin.createAcls(acls).all(), deadline);
        trust.requireManagementIssuer(management, operator());
        final var resource = new ConfigResource(ConfigResource.Type.TOPIC, scope.physicalTopic());
        final var operations = REQUIRED_POLICY.entrySet().stream()
                .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET)).toList();
        await(admin.incrementalAlterConfigs(Map.of(resource, operations)).all(), deadline);
        trust.requireManagementIssuer(management, operator());
        final var after = observe(scope, deadline, true);
        catalog.activateManagement(installation, after, operator());
        final var protectedImage = image(scope, management, after);
        trust.requirePublisher(protectedImage, control, at);
        final var signed = TargetKafkaReceiptControlArtifact.signProtection(protectedImage, control, at, key);
        // Publication trust calls requireCurrent over this same real backing; a CAS/response alone is not success.
        final var published = catalog.publishProtection(signed);
        requireCurrent(published.protection(), control, at);
        return published;
        } catch (RuntimeException | Error failed) {
            try { breakEpoch(management, failed.getClass().getName()); }
            catch (RuntimeException hold) { failed.addSuppressed(hold); }
            throw failed;
        }
    }

    /** Reopens by original management epoch and exact policy/ACL image. It never reapplies settings to mask a historical gap. */
    public void requireCurrent(TargetKafkaReceiptProtection protection, PreparedControlOperation control, SourcePosition at) {
        final var scope = new Scope(protection.resource(), protection.physicalTopic(), protection.slotsPerShard(),
                protection.physicalPartition(), protection.evidenceGeneration(), protection.capability(), protection.controlScopeRef());
        requireAttached(catalog, trust, configuredControlKey); requireCapability(scope); requireControlScope(scope, control);
        final var original = TargetKafkaReceiptControlArtifact.decode(protection.managementAuthorityImage());
        final var managed = catalog.management(deployment.topic());
        if (managed.state() != OxiaSyncTargetKafkaReceiptProtectionCatalog.ACTIVE
                || !Arrays.equals(original.canonicalBytes(), managed.artifact().canonicalBytes())
                || !Arrays.equals(TargetKafkaReceiptManagementImage.decode(original.image()).canonicalBytes(), deployment.canonicalBytes())
                || !Arrays.equals(protection.managementEpoch(), deployment.managementEpoch())) {
            throw new IllegalStateException("receipt management continuity/root/credentials was lost");
        }
        trust.requireManagementIssuer(original, operator());
        try {
            final byte[] actual = observe(scope, System.nanoTime() + timeout.toNanos(), true);
            if (!Arrays.equals(actual, managed.policy()) || !Arrays.equals(actual, protection.brokerPolicyImage())) {
                throw new IllegalStateException("receipt policy/ACL/resource differs from its original continuous protection");
            }
            trust.requireManagementIssuer(original, operator());
        } catch (RuntimeException failure) {
            // Persist a break even if a later privileged action restores the current settings.
            try { breakEpoch(original, failure.getClass().getName()); }
            catch (RuntimeException held) { failure.addSuppressed(held); }
            throw failure;
        }
    }

    private ControlAuthorizationContext operator() {
        return Objects.requireNonNull(currentOperator.get(), "actual current authenticated PLATFORM_OPERATOR");
    }
    private void requireAttached(OxiaSyncTargetKafkaReceiptProtectionCatalog candidate,
            TargetKafkaReceiptProtectionTrust authority, PrivateKey key) {
        if (catalog == null || candidate != catalog || authority != trust || key != configuredControlKey) {
            throw new IllegalStateException("receipt management is not bound to its configured catalog/key/root");
        }
    }
    private void breakEpoch(TargetKafkaReceiptControlArtifact original, String reason) {
        final byte[] failure = Bytes.sha256(Bytes.utf8("nereus-delay-target-kafka-receipt-management-loss\0"),
                original.digest(), Bytes.utf8(reason));
        catalog.breakManagement(original, failure, TargetKafkaReceiptProtectionTrust.signFailure(original, failure,
                configuredControlKey), operator());
    }
    @Override public void close() { admin.close(timeout); }

    private byte[] observe(Scope scope, long deadline, boolean requireHealthy) {
        final String cluster = await(admin.describeCluster().clusterId(), deadline);
        if (!scope.resource().authenticatedClusterId().equals(cluster)) { throw new IllegalStateException("receipt Admin connected to a different cluster"); }
        final var nodes = await(admin.describeCluster().nodes(), deadline);
        if (nodes.isEmpty() || nodes.size() > maximumBrokers) { throw new IllegalStateException("receipt cluster exceeds active management bounds"); }
        final TopicDescription topic = await(admin.describeTopics(List.of(scope.physicalTopic()))
                .allTopicNames(), deadline).get(scope.physicalTopic());
        final var id = scope.resource().receiptTopicUuid();
        if (topic == null || !topic.topicId().equals(new org.apache.kafka.common.Uuid(id.getMostSignificantBits(), id.getLeastSignificantBits()))
                || topic.partitions().size() != capability().evidencePartitionCount()
                || topic.partitions().stream().noneMatch(p -> p.partition() == scope.physicalPartition())) {
            throw new IllegalStateException("receipt topic UUID/partition geometry differs from its original capability");
        }
        final var configResource = new ConfigResource(ConfigResource.Type.TOPIC, scope.physicalTopic());
        final Config configs = await(admin.describeConfigs(List.of(configResource), new DescribeConfigsOptions()
                .includeSynonyms(false)).all(), deadline).get(configResource);
        if (configs == null) { throw new IllegalStateException("receipt Admin omitted actual topic configuration"); }
        final var policy = new TreeMap<String, String>();
        for (var expected : REQUIRED_POLICY.entrySet()) {
            final var observed = configs.get(expected.getKey());
            if (observed == null || observed.value() == null || observed.isSensitive()) {
                throw new IllegalStateException("receipt Broker policy is unavailable: " + expected.getKey());
            }
            if (requireHealthy && !expected.getValue().equals(observed.value())) {
                throw new IllegalStateException("receipt retention/compaction policy is not protected: " + expected.getKey());
            }
            policy.put(expected.getKey(), observed.value());
        }
        final var filter = new AclBindingFilter(new ResourcePatternFilter(ResourceType.TOPIC,
                scope.physicalTopic(), PatternType.MATCH), AccessControlEntryFilter.ANY);
        final var acls = await(admin.describeAcls(filter).values(), deadline);
        if (acls.size() > maximumAcls) { throw new IllegalStateException("receipt ACL observation exceeds bound"); }
        auditAcls(scope.physicalTopic(), acls, requireHealthy);
        final var sorted = new ArrayList<>(acls);
        sorted.sort(Comparator.comparing(a -> Bytes.hex(aclBytes(a))));
        final byte[] image = CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, deployment.topic().canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, Bytes.utf8(scope.physicalTopic()));
            CanonicalProtobuf.uint32(out, 4, topic.partitions().size());
            for (var entry : policy.entrySet()) {
                CanonicalProtobuf.bytes(out, 5, CanonicalProtobuf.message(p -> {
                    CanonicalProtobuf.bytes(p, 1, Bytes.utf8(entry.getKey()));
                    CanonicalProtobuf.bytes(p, 2, Bytes.utf8(entry.getValue()));
                }));
            }
            for (var acl : sorted) { CanonicalProtobuf.bytes(out, 6, aclBytes(acl)); }
            CanonicalProtobuf.bytes(out, 7, capability().brokerPrerequisiteDigest());
        });
        if (image.length > TargetKafkaReceiptProtection.MAX_POLICY_BYTES) { throw new IllegalStateException("receipt Broker/ACL image exceeds bound"); }
        return image;
    }

    private TargetKafkaReceiptProtection image(Scope scope, TargetKafkaReceiptControlArtifact management, byte[] policy) {
        return new TargetKafkaReceiptProtection(scope.resource(), scope.physicalTopic(), scope.slotsPerShard(),
                scope.physicalPartition(), scope.evidenceGeneration(), scope.capability(), scope.controlScopeRef(),
                deployment.managementEpoch(), policy, management.canonicalBytes());
    }
    private DeliveryCapabilitySemantic capability() { return (DeliveryCapabilitySemantic) deployment.capability().body(); }
    private void requireCapability(Scope scope) {
        final var c = capability();
        final var resource = c.evidenceResource();
        if (!deployment.capability().ref().equals(scope.capability()) || !deployment.physicalTopic().equals(scope.physicalTopic())
                || !Arrays.equals(deployment.controlScopeRef(), scope.controlScopeRef())
                || !resource.kafka().authenticatedClusterId().equals(scope.resource().authenticatedClusterId())
                || !resource.kafka().nativeTopicUuid().equals(scope.resource().receiptTopicUuid())) {
            throw new IllegalArgumentException("receipt management lacks its exact configured source-bound deployment/capability");
        }
    }
    private static void requireControlScope(Scope scope, PreparedControlOperation control) {
        if (control.kind() != ControlOperationKind.CREATE_TARGET_CHANNEL && control.kind() != ControlOperationKind.RENEW_TARGET_CHANNEL
                || !(control.request().branch() instanceof TargetChannelControlRequest request)) {
            throw new IllegalArgumentException("receipt management requires an original registered channel CONTROL operation");
        }
        final var context = request.channel().context();
        if (!Arrays.equals(context.controlScopeRef(), scope.controlScopeRef())
                || !context.sourceShard().routeIncarnation().equals(scope.resource().routeIncarnation())
                || context.sourceShard().partition() != scope.resource().shardPartition()
                || !Objects.equals(context.evidenceGeneration(), scope.evidenceGeneration())) {
            throw new IllegalArgumentException("receipt management changes CONTROL source/evidence/scope");
        }
    }
    private static List<AclBinding> requiredWorkerAcls(String topic, Set<String> workers) {
        final var list = new ArrayList<AclBinding>();
        final var resource = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL);
        for (String worker : new TreeSet<>(workers)) {
            for (var operation : MANAGEMENT_OPERATIONS) {
                list.add(new AclBinding(resource, new AccessControlEntry(worker, "*", operation, AclPermissionType.DENY)));
            }
            for (var operation : WORKER_OPERATIONS) {
                list.add(new AclBinding(resource, new AccessControlEntry(worker, "*", operation, AclPermissionType.ALLOW)));
            }
        }
        return list;
    }
    private void auditAcls(String topic, Collection<AclBinding> acls, boolean healthy) {
        for (var acl : acls) {
            final var e = acl.entry();
            if (e.permissionType() == AclPermissionType.ALLOW
                    && (e.operation() == AclOperation.ALL || e.operation() == AclOperation.WRITE
                            || MANAGEMENT_OPERATIONS.contains(e.operation()))
                    && !deployment.managementPrincipals().contains(e.principal()) && !Set.copyOf(deployment.workerPrincipals()).contains(e.principal())) {
                throw new IllegalStateException("receipt topic is writable/mutable outside exclusive managed identities");
            }
        }
        if (healthy && !acls.containsAll(requiredWorkerAcls(topic, Set.copyOf(deployment.workerPrincipals())))) {
            throw new IllegalStateException("receipt Worker credentials are not isolated from policy/delete management");
        }
    }
    private <T> T await(KafkaFuture<T> future, long deadline) {
        final long remaining = deadline - System.nanoTime();
        if (remaining <= 0) { throw new IllegalStateException("receipt management deadline elapsed"); }
        try { return future.get(remaining, TimeUnit.NANOSECONDS); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new IllegalStateException("receipt management interrupted", interrupted);
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException failed) {
            throw new IllegalStateException("receipt management RPC failed or uncertain", failed);
        }
    }
    private static byte[] aclBytes(AclBinding acl) {
        final var p = acl.pattern(); final var e = acl.entry();
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out,1,p.resourceType().code()); CanonicalProtobuf.uint32(out,2,p.patternType().code());
            CanonicalProtobuf.bytes(out,3,Bytes.utf8(p.name())); CanonicalProtobuf.bytes(out,4,Bytes.utf8(e.principal()));
            CanonicalProtobuf.bytes(out,5,Bytes.utf8(e.host())); CanonicalProtobuf.uint32(out,6,e.operation().code());
            CanonicalProtobuf.uint32(out,7,e.permissionType().code());
        });
    }
}
