package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.OwnedCheckpointIo;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetOwnedBackendGroup;
import com.nereusstream.delay.ownership.TargetSdkArtifactIdentity;
import com.nereusstream.delay.ownership.TargetWorkerHostRuntime;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CredentialBinding;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.net.URI;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SizeUnit;
import org.apache.pulsar.client.impl.PulsarClientImpl;

/** One actual owned P1 client generation for a Host; it never adopts a borrowed client or creates one per Target. */
public final class PulsarClientArtifactOwnedBackendGroup implements TargetOwnedBackendGroup {
    public record Settings(String allocationId, String serviceUrl, String authenticatedClusterId,
            int ioThreads, int listenerThreads, int connectionsPerBroker, int maximumConnections,
            int maximumUsers, long memoryBytes, int operationTimeoutMs, int maximumLookupRedirects) {
        public Settings {
            final var uri = URI.create(Objects.requireNonNull(serviceUrl, "native endpoint"));
            if (allocationId.isBlank() || authenticatedClusterId.isBlank()
                    || !List.of("pulsar", "pulsar+ssl").contains(uri.getScheme()) || uri.getHost() == null
                    || ioThreads <= 0 || listenerThreads <= 0 || connectionsPerBroker <= 0
                    || maximumConnections < connectionsPerBroker || maximumUsers <= 0
                    || maximumUsers >= Integer.MAX_VALUE - 1 || memoryBytes <= 0 || memoryBytes == Long.MAX_VALUE
                    || operationTimeoutMs <= 0 || operationTimeoutMs > 60_000 || maximumLookupRedirects <= 0) {
                throw new IllegalArgumentException("owned P1 group requires finite endpoint/resource settings");
            }
        }
    }

    private static final Map<PulsarClient, PulsarClientArtifactOwnedBackendGroup> CLIENTS = new IdentityHashMap<>();
    private static final Map<Object, PulsarClientArtifactOwnedBackendGroup> HANDLES = new IdentityHashMap<>();
    private final Settings settings;
    private final CredentialBinding credential;
    private final byte[] sdkJar;
    private final WorkClassExecutionRegistry classes;
    private final SharedRocksDbResources resources;
    private final TargetDestinationSdkBudget.Client allocation;
    private final ThreadPoolExecutor io;
    private final Object gate = new Object();
    private final Map<Object, NativeUse> users = new IdentityHashMap<>();
    private final Map<Object, com.nereusstream.delay.ownership.SourceRecordConsumer> sourceOwners = new IdentityHashMap<>();
    private final java.util.Set<TargetWorkerShardRuntime> admittedWorkers = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    private final java.util.Set<com.nereusstream.delay.ownership.SourceRecordConsumer> admittedSources =
            java.util.Collections.newSetFromMap(new IdentityHashMap<>());
    private final List<Participant> participants = new ArrayList<>();
    private final Object construction = new Object();
    private final CompletableFuture<Void> terminated = new CompletableFuture<>();
    private OwnedCheckpointIo<PulsarClient> opening;
    private OwnedCheckpointIo<Boolean> teardown;
    private PulsarClient client;
    private TargetWorkerHostRuntime host;
    private boolean ready;
    private volatile boolean frozen, sdkTerminated;
    private boolean teardownStarting;
    private List<Object> frozenRegistrations;
    private TargetWorkerHostRuntime.BackendClosePermit permit;
    private CompletableFuture<Void> firstClientClose;
    private Throwable firstFailure;

    private record Participant(Object identity, BooleanSupplier idle, BooleanSupplier released,
            Consumer<TerminationProof> finish) {}

    /** A native SDK operation and its first close; late success attaches to this pre-existing registration. */
    static final class NativeUse {
        final PulsarClientArtifactOwnedBackendGroup group;
        final TargetDestinationSdkBudget.Client allocation;
        CompletableFuture<?> opening;
        boolean creationObserved;
        boolean creationCallbackExited;
        final CompletableFuture<Void> creationExit = new CompletableFuture<>();
        int activeReads;
        Object value;
        com.nereusstream.delay.ownership.SourceTransportCredentialUse sourceCredentialUse;
        CompletableFuture<Void> firstClose;
        CompletableFuture<Void> sdkClose;
        final CompletableFuture<Void> closeExit = new CompletableFuture<>();
        NativeUse(PulsarClientArtifactOwnedBackendGroup group, TargetDestinationSdkBudget.Client allocation) {
            this.group = group; this.allocation = allocation;
        }
    }

    /** This proof is minted only after actual SDK shutdown and its shutdown thread have both ended. */
    static final class TerminationProof {
        private final PulsarClientArtifactOwnedBackendGroup group;
        private TerminationProof(PulsarClientArtifactOwnedBackendGroup group) { this.group = group; }
        void requireClient(PulsarClient expected) {
            synchronized (group.gate) {
                if (group.client != expected || !group.sdkTerminated
                        || group.permit == null && group.host != null) {
                    throw new IllegalStateException("P1 cleanup lacks actual original backend termination");
                }
            }
            if (group.permit != null) { group.permit.requireOriginalGroup(group, group.frozenRegistrations); }
        }
    }

    private PulsarClientArtifactOwnedBackendGroup(Settings settings, CredentialBinding credential, byte[] sdkJar,
            TargetDestinationSdkBudget budget, CapacityVector cost, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, ThreadPoolExecutor io,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt sourceReceipt) {
        this.settings = Objects.requireNonNull(settings);
        if (credential == null && sourceReceipt == null) { throw new IllegalArgumentException("actual P1 credential is required"); }
        this.credential = credential;
        Bytes.requireLength(sdkJar, 32, "activated P1 JAR");
        this.sdkJar = Bytes.copy(sdkJar);
        this.classes = Objects.requireNonNull(classes);
        this.resources = Objects.requireNonNull(resources);
        this.io = Objects.requireNonNull(io);
        budget.requireWorker(classes, resources);
        final long[] minimum = new long[CapacityDimension.COUNT];
        minimum[CapacityDimension.ADAPTER_THREADS.wireValue() - 1] =
                Math.addExact(Math.multiplyExact(3L, settings.ioThreads()), settings.listenerThreads() + 4L);
        minimum[CapacityDimension.ADAPTER_CONNECTIONS.wireValue() - 1] = settings.maximumConnections();
        minimum[CapacityDimension.OPEN_FILES.wireValue() - 1] = settings.maximumConnections();
        minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = settings.memoryBytes();
        if (!cost.covers(new CapacityVector(minimum))) {
            throw new IllegalArgumentException("owned P1 client cost does not cover configured SDK resources");
        }
        allocation = budget.reserve(settings.allocationId(), cost);
    }

    public static PulsarClientArtifactOwnedBackendGroup open(Settings settings, CredentialBinding credential,
            byte[] sdkJar, Authentication authentication, Runnable actualActivation,
            TargetDestinationSdkBudget budget, CapacityVector cost, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, ThreadPoolExecutor io) {
        return openInternal(settings, credential, sdkJar, authentication, actualActivation, budget, cost, classes,
                resources, io, null);
    }

    private com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt credentialReceipt;
    private com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt sourceCredentialReceipt;
    private volatile java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> sourceCredentialTime;
    private final Map<com.nereusstream.delay.protocol.ShardId, com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt>
            sourceCredentials = new java.util.HashMap<>();

    public static PulsarClientArtifactOwnedBackendGroup openVerifiedSourceMtls(Settings settings, byte[] sdkJar,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            Runnable actualActivation, TargetDestinationSdkBudget budget, CapacityVector cost,
            WorkClassExecutionRegistry classes, SharedRocksDbResources resources, ThreadPoolExecutor io) {
        receipt.requirePulsarEndpoint(settings.serviceUrl());
        final var authorization = receipt.authorization();
        if (authorization.resource().kind() != com.nereusstream.delay.protocol.BrokerResourceIdentity.Kind.PULSAR
                || !authorization.resource().pulsar().authenticatedClusterId().equals(settings.authenticatedClusterId())) {
            throw new IllegalStateException("P1 Source endpoint differs from the signed deployment resource");
        }
        final byte[] cert = receipt.certificateChainPem(), key = receipt.privateKeyPem(), trust = receipt.serverTrustPem();
        final var authentication = new org.apache.pulsar.client.impl.auth.AuthenticationTls(
                () -> new java.io.ByteArrayInputStream(cert), () -> new java.io.ByteArrayInputStream(key),
                () -> new java.io.ByteArrayInputStream(trust));
        final var group = openInternal(settings, null, sdkJar, authentication, () -> {
            actualActivation.run();
            receipt.requireOperation(com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
        }, budget, cost, classes, resources, io, null, receipt);
        group.sourceCredentialTime = Objects.requireNonNull(time);
        return group;
    }

    void requireSourceSubscription(org.apache.pulsar.client.api.TopicResourceGuard guard, String topic, String name,
            org.apache.pulsar.client.api.SubscriptionType type) {
        final com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt;
        synchronized (gate) {
            if (sourceCredentialReceipt == null) return;
            if (frozen || type != org.apache.pulsar.client.api.SubscriptionType.Exclusive) {
                throw new IllegalStateException("P1 Source subscription is outside its deployment authorization");
            }
            receipt = sourceCredentials.values().stream().filter(candidate -> {
                final var resource = candidate.authorization().resource();
                return resource.kind() == com.nereusstream.delay.protocol.BrokerResourceIdentity.Kind.PULSAR
                        && candidate.authorization().physicalTopic().equals(topic)
                        && guard.authenticatedClusterId().equals(resource.pulsar().authenticatedClusterId())
                        && java.util.Arrays.equals(guard.resourceIncarnation(), resource.pulsar().resourceIncarnation())
                        && guard.topicCreationTimestamp() == resource.pulsar().physicalTopicCreationTimestamp();
            }).findFirst().orElseThrow(() -> new IllegalStateException("P1 Source subscription has no signed resource scope"));
        }
        receipt.authorization().requireName(com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.CONSUME, name);
        final var now = Objects.requireNonNull(sourceCredentialTime, "Source deployment clock").get();
        for (var role : java.util.List.of(com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.CONSUME,
                com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.ACK,
                com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.LOOKUP)) { receipt.requireOperation(role, now); }
    }

    @Override public com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt sourceCredentialReceipt() { return sourceCredentialReceipt; }
    @Override public boolean holdsSourceCredential(com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt) {
        synchronized (gate) { return sourceCredentials.get(receipt.authorization().shard()) == receipt; }
    }
    public void addSourceAuthorization(com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt) {
        receipt.requirePulsarEndpoint(settings.serviceUrl());
        synchronized (gate) {
            final var previous = sourceCredentials.get(receipt.authorization().shard());
            if (previous == receipt) return;
            if (frozen || host != null || sourceCredentialReceipt == null || !sourceCredentialReceipt.sameMaterial(receipt)
                    || sourceCredentials.size() >= settings.maximumUsers() || previous != null) {
                throw new IllegalStateException("P1 Source authorization cannot replace its native material generation");
            }
            sourceCredentials.put(receipt.authorization().shard(), receipt);
        }
    }
    @Override public boolean sourceCredentialsReady(TargetWorkerShardRuntime worker) {
        final java.util.List<NativeUse> sources;
        synchronized (gate) {
            if (!workerReady(worker)) return false;
            sources = sourceOwners.entrySet().stream().filter(entry -> worker.ownsSourceConsumer(entry.getValue()))
                    .map(entry -> use(entry.getKey())).toList();
            if (sources.isEmpty() || sources.stream().anyMatch(use -> use.sourceCredentialUse == null)) return false;
        }
        for (var source : sources) {
            source.sourceCredentialUse.require(com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.CONSUME, source.value);
            source.sourceCredentialUse.require(com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation.ACK, source.value);
        }
        return true;
    }
    void bindSourceCredential(Object consumer, TargetWorkerShardRuntime worker,
            com.nereusstream.delay.ownership.SourceAssignment assignment, com.nereusstream.delay.protocol.OwnerIdentity owner,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            java.util.function.LongSupplier clock) {
        final NativeUse nativeUse;
        final com.nereusstream.delay.ownership.SourceRecordConsumer source;
        final com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt;
        synchronized (gate) {
            nativeUse = use(consumer); source = sourceOwners.get(consumer); receipt = sourceCredentials.get(assignment.shardId());
            if (host != null || frozen || nativeUse == null || nativeUse.sourceCredentialUse != null || source == null || receipt == null)
                throw new IllegalStateException("P1 Source credential binding is unavailable");
        }
        final var bound = worker.bindSourceCredential(receipt, source, assignment, owner, consumer, time, clock);
        synchronized (gate) {
            if (frozen || use(consumer) != nativeUse) throw new IllegalStateException("P1 Source changed during credential binding");
            nativeUse.sourceCredentialUse = bound;
        }
    }
    void requireSourceOperation(Object consumer, com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization.Operation role) {
        final com.nereusstream.delay.ownership.SourceTransportCredentialUse bound;
        synchronized (gate) {
            if (sourceCredentialReceipt == null) return;
            final var nativeUse = use(consumer);
            if (frozen || nativeUse == null || nativeUse.firstClose != null || nativeUse.sourceCredentialUse == null)
                throw new IllegalStateException("P1 Source material use is frozen or unbound");
            bound = nativeUse.sourceCredentialUse;
        }
        try { bound.require(role, consumer); }
        catch (RuntimeException | Error failure) {
            recordFailure(failure);
            final TargetWorkerHostRuntime current;
            synchronized (gate) { current = host; }
            if (current != null) {
                try { current.quarantineOwnedBackends(); }
                catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
            }
            throw failure;
        }
    }

    public static PulsarClientArtifactOwnedBackendGroup openVerifiedMtls(Settings settings, CredentialBinding credential,
            byte[] sdkJar, com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt receipt,
            java.util.function.Supplier<com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence> time,
            Runnable actualActivation, TargetDestinationSdkBudget budget, CapacityVector cost,
            WorkClassExecutionRegistry classes, SharedRocksDbResources resources, ThreadPoolExecutor io) {
        receipt.requireBinding(credential);
        receipt.requirePulsarEndpoint(settings.serviceUrl());
        if (!settings.serviceUrl().startsWith("pulsar+ssl://")) {
            throw new IllegalArgumentException("new Target P1 mTLS requires a TLS endpoint");
        }
        final byte[] cert = receipt.certificateChainPem(), key = receipt.privateKeyPem(), trust = receipt.serverTrustPem();
        final var authentication = new org.apache.pulsar.client.impl.auth.AuthenticationTls(
                () -> new java.io.ByteArrayInputStream(cert), () -> new java.io.ByteArrayInputStream(key),
                () -> new java.io.ByteArrayInputStream(trust));
        return openInternal(settings, credential, sdkJar, authentication, () -> {
            actualActivation.run(); receipt.requireActive(time.get());
        }, budget, cost, classes, resources, io, receipt);
    }

    @Override public com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt credentialReceipt() { return credentialReceipt; }

    void requireLoadedMaterial(com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt receipt) {
        if (credentialReceipt == null ? sourceCredentialReceipt == null || !sourceCredentialReceipt.sameMaterial(receipt)
                : !credentialReceipt.sameMaterial(receipt)) {
            throw new IllegalStateException("P1 client did not load this original resolved mTLS snapshot");
        }
    }
    void requireSourceReadMaterial(com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt) {
        receipt.requirePulsarEndpoint(settings.serviceUrl());
        if (sourceCredentialReceipt == null ? credentialReceipt == null || !receipt.sameMaterial(credentialReceipt)
                : !sourceCredentialReceipt.sameMaterial(receipt)) {
            throw new IllegalStateException("P1 reader did not load the signed Source material snapshot");
        }
    }
    void requireSourceReadResource(com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt receipt,
            org.apache.pulsar.client.api.TopicResourceGuard actual) {
        requireSourceReadMaterial(receipt);
        final var resource = receipt.authorization().resource().pulsar();
        if (!new org.apache.pulsar.client.api.TopicResourceGuard(resource.authenticatedClusterId(), resource.resourceIncarnation(),
                resource.physicalTopicCreationTimestamp()).equals(actual)) {
            throw new IllegalStateException("P1 Source reader changes its signed Broker resource");
        }
    }

    private static PulsarClientArtifactOwnedBackendGroup openInternal(Settings settings, CredentialBinding credential,
            byte[] sdkJar, Authentication authentication, Runnable actualActivation,
            TargetDestinationSdkBudget budget, CapacityVector cost, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, ThreadPoolExecutor io,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt receipt) {
        return openInternal(settings, credential, sdkJar, authentication, actualActivation, budget, cost, classes, resources, io, receipt, null);
    }
    private static PulsarClientArtifactOwnedBackendGroup openInternal(Settings settings, CredentialBinding credential,
            byte[] sdkJar, Authentication authentication, Runnable actualActivation,
            TargetDestinationSdkBudget budget, CapacityVector cost, WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources, ThreadPoolExecutor io,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt receipt,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.SourceReceipt sourceReceipt) {
        final var group = new PulsarClientArtifactOwnedBackendGroup(settings, credential, sdkJar, budget, cost,
                classes, resources, io, sourceReceipt);
        group.credentialReceipt = receipt;
        group.sourceCredentialReceipt = sourceReceipt;
        if (sourceReceipt != null) { group.sourceCredentials.put(sourceReceipt.authorization().shard(), sourceReceipt); }
        try {
            group.opening = OwnedCheckpointIo.submit(guard -> {
                guard.run(); actualActivation.run();
                final var builder = PulsarClient.builder();
                if (receipt != null || sourceReceipt != null) { builder.enableTlsHostnameVerification(true).allowTlsInsecureConnection(false); }
                TargetSdkArtifactIdentity.requireExact(builder.getClass(), group.sdkJar);
                group.allocation.beginConstruction();
                final PulsarClient nativeClient;
                try {
                    nativeClient = builder.serviceUrl(settings.serviceUrl()).authentication(authentication)
                        .ioThreads(settings.ioThreads()).listenerThreads(settings.listenerThreads())
                        .connectionsPerBroker(settings.connectionsPerBroker())
                        .memoryLimit(settings.memoryBytes(), SizeUnit.BYTES)
                        .operationTimeout(settings.operationTimeoutMs(), TimeUnit.MILLISECONDS)
                        .lookupTimeout(settings.operationTimeoutMs(), TimeUnit.MILLISECONDS)
                        .connectionTimeout(settings.operationTimeoutMs(), TimeUnit.MILLISECONDS)
                        .maxConcurrentLookupRequests(settings.maximumUsers()).maxLookupRequests(settings.maximumUsers() + 1)
                        .maxLookupRedirects(settings.maximumLookupRedirects()).enableTransaction(false).build();
                } catch (PulsarClientException failure) { throw new java.util.concurrent.CompletionException(failure); }
                synchronized (group.gate) { group.client = nativeClient; }
                synchronized (CLIENTS) { CLIENTS.put(nativeClient, group); }
                TargetSdkArtifactIdentity.requireExact(nativeClient.getClass(), group.sdkJar);
                actualActivation.run();
                return nativeClient;
            }, () -> { synchronized (group.gate) {
                if (group.frozen) { throw new IllegalStateException("P1 group opening is frozen"); }
            } }, io);
            group.opening.receipt().whenComplete((__, failure) -> {
                if (failure != null) { group.recordFailure(failure); }
                if (group.frozen) { group.scheduleProgress(); }
            });
            return group;
        } catch (RuntimeException | Error failure) {
            group.allocation.releaseUnstarted();
            throw failure;
        }
    }

    public boolean finishStartup() {
        synchronized (gate) {
            if (frozen) { throw new IllegalStateException("P1 group is frozen"); }
            if (!opening.ioExited()) { return false; }
            client = opening.completedValue().orElseThrow(() ->
                    new IllegalStateException("P1 client opening failed", openingFailure()));
            ready = true;
            return true;
        }
    }

    static PulsarClientArtifactOwnedBackendGroup find(PulsarClient client) {
        synchronized (CLIENTS) { return CLIENTS.get(client); }
    }

    PulsarClient nativeClientForFactory() {
        synchronized (gate) {
            if (!ready || frozen) { throw new IllegalStateException("owned P1 group is not open for SDK creation"); }
            return client;
        }
    }

    public void requireCredential(CredentialBinding expected) {
        if (credential == null) throw new IllegalStateException("P1 Source authority does not imply a Destination credential binding");
        if (!Bytes.constantTimeEquals(credential.bindingDigest(), expected.bindingDigest())) {
            throw new IllegalStateException("P1 shared client has another actual credential generation");
        }
    }
    void requireTargetCredential(com.nereusstream.delay.protocol.TargetChannelIdentity channel) {
        if (credential == null) throw new IllegalStateException("P1 Target requires its independent resolved Destination credential");
        channel.credentialLease().requireBinding(credential);
    }
    void requireTargetCredential(com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            com.nereusstream.delay.ownership.LocalBrokerMtlsResolver.Receipt receipt) {
        requireLoadedMaterial(receipt);
        if (credential != null) { requireTargetCredential(channel); }
    }
    void requireProducerArtifact(Producer<?> producer) { TargetSdkArtifactIdentity.requireExact(producer.getClass(), sdkJar); }
    byte[] activatedSdkJarSha256() { return Bytes.copy(sdkJar); }

    @Override public void requireWorkerGraph(WorkClassExecutionRegistry expected, SharedRocksDbResources shared) {
        if (classes != expected || resources != shared) { throw new IllegalArgumentException("P1 group belongs to another Worker"); }
    }

    @Override public void requireHostComposition(TargetWorkerHostRuntime expected, List<TargetWorkerShardRuntime> fleet) {
        Objects.requireNonNull(expected, "host");
        for (var worker : fleet) { worker.requireFleetComposition(classes, resources); }
        synchronized (gate) {
            requireHostCompositionLocked(expected, fleet);
        }
    }
    private void requireHostCompositionLocked(TargetWorkerHostRuntime expected, List<TargetWorkerShardRuntime> fleet) {
        if (!ready || host != null && host != expected || frozen) {
            throw new IllegalStateException("P1 client is borrowed, unready or already belongs to another Host");
        }
        for (var source : sourceOwners.values()) {
            if (fleet.stream().noneMatch(worker -> worker.ownsSourceConsumer(source))) {
                throw new IllegalStateException("P1 native Source is used outside this Host fleet");
            }
        }
    }
    @Override public void bindHost(TargetWorkerHostRuntime expected, List<TargetWorkerShardRuntime> fleet) {
        Objects.requireNonNull(expected, "host");
        for (var worker : fleet) { worker.requireFleetComposition(classes, resources); }
        synchronized (gate) {
            requireHostCompositionLocked(expected, fleet);
            admittedSources.addAll(sourceOwners.values());
            host = expected;
            admittedWorkers.addAll(fleet);
        }
    }

    int operationTimeoutMs() { return settings.operationTimeoutMs(); }
    PulsarClient clientForProof() { synchronized (gate) { return client; } }
    String authenticatedClusterId() { return settings.authenticatedClusterId(); }

    static PulsarClientArtifactOwnedBackendGroup findNative(Object handle) {
        synchronized (HANDLES) { return HANDLES.get(handle); }
    }

    void registerSourceOwner(Object handle, com.nereusstream.delay.ownership.SourceRecordConsumer source) {
        synchronized (gate) {
            if (frozen || use(handle) == null || sourceOwners.putIfAbsent(handle, source) != null) {
                throw new IllegalStateException("P1 native Source ownership was not transferred during startup");
            }
        }
    }

    @Override public boolean applicationReady() { synchronized (gate) { return ready && host != null && !frozen; } }
    @Override public boolean workerReady(TargetWorkerShardRuntime worker) {
        synchronized (gate) { return ready && host != null && !frozen && admittedWorkers.contains(worker); }
    }
    @Override public void admitWorker(TargetWorkerHostRuntime expected, TargetWorkerShardRuntime worker) {
        worker.requireFleetComposition(classes, resources);
        synchronized (gate) {
            if (host != expected || frozen || !ready || sourceOwners.values().stream().noneMatch(worker::ownsSourceConsumer)) {
                throw new IllegalStateException("P1 Source replacement is not owned by this Host");
            }
            admittedWorkers.add(worker);
            sourceOwners.values().stream().filter(worker::ownsSourceConsumer).forEach(admittedSources::add);
        }
    }
    boolean sourceApplicationReady(com.nereusstream.delay.ownership.SourceRecordConsumer source) {
        synchronized (gate) {
            return ready && host != null && !frozen && admittedSources.contains(source);
        }
    }

    <T> CompletableFuture<T> createNative(Supplier<CompletableFuture<T>> create, TargetDestinationSdkBudget.Client cost) {
        final NativeUse use;
        synchronized (gate) {
            users.entrySet().removeIf(entry -> entry.getValue().firstClose != null
                    && entry.getValue().activeReads == 0 && entry.getValue().firstClose.isDone()
                    && !entry.getValue().firstClose.isCompletedExceptionally());
            if (!ready || frozen || users.size() >= settings.maximumUsers()) {
                throw new IllegalStateException("P1 native registration window is unavailable");
            }
            use = new NativeUse(this, cost);
            users.put(use, use);
            try {
                if (cost != null) { cost.beginConstruction(); }
                use.opening = Objects.requireNonNull(create.get(), "original P1 creation future");
            }
            catch (RuntimeException | Error failure) { use.opening = CompletableFuture.failedFuture(failure); }
        }
        @SuppressWarnings("unchecked") final CompletableFuture<T> original = (CompletableFuture<T>) use.opening;
        final var transferred = new CompletableFuture<T>();
        original.whenComplete((value, failure) -> {
            synchronized (gate) {
                use.value = value;
                if (value != null) { synchronized (HANDLES) { HANDLES.put(value, this); } }
                use.creationObserved = true;
            }
            try {
                if (frozen && value != null) { closeNative(value); }
            } catch (RuntimeException | Error cleanup) { recordFailure(cleanup); }
            finally {
                // SDK callbacks need not run in registration order. Expose only after original ownership is recorded.
                try {
                    if (failure == null) { transferred.complete(value); }
                    else { transferred.completeExceptionally(failure); }
                } finally {
                    synchronized (gate) { use.creationCallbackExited = true; }
                    cleanupProgress();
                    use.creationExit.complete(null);
                }
            }
        });
        return transferred.copy();
    }

    NativeUse use(Object value) {
        synchronized (gate) {
            return users.values().stream().filter(use -> use.value == value).findFirst().orElse(null);
        }
    }

    CompletableFuture<Void> nativeCreationExit(Object value) {
        synchronized (gate) {
            final var original = use(value);
            if (original == null) throw new IllegalStateException("P1 creation has no original native registration");
            return original.creationExit.copy();
        }
    }

    CompletableFuture<Void> closeNative(Object value) {
        final NativeUse use;
        synchronized (gate) {
            use = use(value);
            if (use == null) { throw new IllegalStateException("P1 native handle was not registered in its original group"); }
            if (use.firstClose != null) { return use.closeExit.copy(); }
            use.firstClose = new CompletableFuture<>();
        }
        try {
            if (value instanceof Producer<?> producer) { use.sdkClose = producer.closeAsync(); }
            else if (value instanceof org.apache.pulsar.client.api.Consumer<?> consumer) { use.sdkClose = consumer.closeAsync(); }
            else { throw new IllegalArgumentException("unsupported P1 native handle"); }
        } catch (RuntimeException | Error failure) { use.sdkClose = CompletableFuture.failedFuture(failure); }
        use.sdkClose.whenComplete((__, failure) -> {
            try {
                if (failure == null) {
                    try {
                        if (use.allocation != null) { use.allocation.closed(); }
                        synchronized (gate) {
                            final var source = sourceOwners.remove(value);
                            if (source != null) {
                                admittedSources.remove(source);
                                admittedWorkers.removeIf(worker -> worker.ownsSourceConsumer(source));
                            }
                        }
                        use.firstClose.complete(null);
                        synchronized (HANDLES) { HANDLES.remove(value); }
                    } catch (RuntimeException | Error failed) { use.firstClose.completeExceptionally(failed); }
                } else { use.firstClose.completeExceptionally(failure); recordFailure(failure); }
            } finally {
                final var failed = com.nereusstream.delay.ownership.CheckpointInputFuture.ownedFailure(use.firstClose);
                cleanupProgress();
                if (failed == null) use.closeExit.complete(null); else use.closeExit.completeExceptionally(failed);
            }
        });
        return use.closeExit.copy();
    }

    /** Existing producer helpers observe this registration; they never register a second native owner. */
    boolean producerBackendStopped(Object value) {
        synchronized (gate) {
            final var use = use(value);
            return frozen && sdkTerminated && use != null && use.firstClose != null && use.firstClose.isDone();
        }
    }

    /** A failed/frozen shared SDK can retire resources but cannot authorize another normal writer opening. */
    void requireNormalJournalHandoff(PulsarClient original) {
        synchronized (gate) {
            if (client != original || !ready || frozen || sdkTerminated || firstFailure != null) {
                throw new IllegalStateException("normal Journal handoff lost its original live shared SDK owner", firstFailure);
            }
        }
    }

    /** Startup seek/proof waits use the existing native registration, including before a replay cursor takes it. */
    static NativeRead beginNativeRead(Object value) {
        final var group = findNative(value);
        if (group == null) { return null; }
        synchronized (group.gate) {
            final var use = group.use(value);
            if (group.frozen || use == null || use.firstClose != null) {
                throw new IllegalStateException("P1 native read registration is closing");
            }
            use.activeReads = Math.incrementExact(use.activeReads);
            return new NativeRead(use);
        }
    }

    static final class NativeRead implements AutoCloseable {
        private final NativeUse use;
        private boolean closed;
        private NativeRead(NativeUse use) { this.use = use; }
        @Override public void close() {
            synchronized (use.group.gate) {
                if (closed) { return; }
                closed = true;
                use.activeReads--;
            }
            use.group.cleanupProgress();
        }
    }

    void registerCleanup(Object identity, BooleanSupplier idle, BooleanSupplier released, Consumer<TerminationProof> finish) {
        synchronized (gate) {
            participants.removeIf(participant -> participant.released().getAsBoolean());
            if (frozen || participants.size() >= settings.maximumUsers()) {
                throw new IllegalStateException("P1 cleanup registration is frozen or full");
            }
            participants.add(new Participant(identity, idle, released, finish));
        }
    }

    <T> T nativeSend(Supplier<T> send) {
        synchronized (gate) {
            if (!ready || host == null || frozen) { throw new IllegalStateException("P1 Host has frozen native SEND/ACK"); }
            return send.get();
        }
    }

    <T> T recoveryJournalSend(Producer<?> producer, Supplier<T> send) {
        synchronized (gate) {
            final var use = use(producer);
            if (!ready || host != null || frozen || use == null || use.firstClose != null) {
                throw new IllegalStateException("original pre-Host recovery Journal SEND is unavailable");
            }
            return send.get();
        }
    }

    /** The exact registered Source producer, reader and Root permit are required before Claim/Host activation. */
    <T> T recoveryFenceSend(PulsarClientArtifactRecoveryFenceWriter writer, Producer<?> producer,
            com.nereusstream.delay.ownership.TargetTimeFenceMutationFactory.Prepared exact,
            com.nereusstream.delay.ownership.TargetWorkerSourceStartup.RecoveryFenceAppend permit, Supplier<T> send) {
        final var source = writer.requireNativeSendBinding(this, producer);
        permit.beforeSdk(writer, exact);
        synchronized (gate) {
            final var nativeProducer = use(producer);
            final var nativeSource = use(source);
            if (!ready || host != null || frozen || sdkTerminated || nativeProducer == null
                    || nativeProducer.firstClose != null || nativeSource == null || nativeSource.firstClose != null) {
                throw new IllegalStateException("original pre-Host recovery Source Fence SEND is unavailable");
            }
            return send.get();
        }
    }

    static <T> T nativeSend(Object nativeHandle, Supplier<T> send) {
        final var group = findNative(nativeHandle);
        if (group != null) { return group.nativeSend(send); }
        return send.get();
    }

    static CompletableFuture<Void> closeConsumer(org.apache.pulsar.client.api.Consumer<?> consumer) {
        final var group = findNative(consumer);
        if (group != null) { return group.closeNative(consumer); }
        return consumer.closeAsync();
    }

    static <T> CompletableFuture<Producer<T>> createProducer(PulsarClient client, ProducerBuilder<T> builder,
            TargetDestinationSdkBudget.Client cost) {
        final var group = find(client);
        return group == null ? builder.createAsync() : group.createNative(builder::createAsync, cost);
    }

    static <T> CompletableFuture<org.apache.pulsar.client.api.Consumer<T>> subscribe(
            PulsarClient client, ConsumerBuilder<T> builder) {
        final var group = find(client);
        return group == null ? builder.subscribeAsync() : group.createNative(builder::subscribeAsync, null);
    }

    public CompletableFuture<Void> closeBeforeHostBinding() {
        synchronized (gate) {
            if (host != null) { return CompletableFuture.failedFuture(new IllegalStateException("bound P1 client needs its Host permit")); }
            freezeRegistrations();
        }
        advanceTermination();
        return terminated.copy();
    }

    @Override public List<Object> freezeRegistrations() {
        synchronized (gate) {
            frozen = true;
            if (frozenRegistrations == null) {
                final var snapshot = new ArrayList<Object>(); snapshot.add(construction);
                snapshot.addAll(users.keySet()); participants.forEach(p -> snapshot.add(p.identity()));
                frozenRegistrations = List.copyOf(snapshot);
            }
            return frozenRegistrations;
        }
    }

    @Override public CompletableFuture<Void> closeOwnedBackend(TargetWorkerHostRuntime.BackendClosePermit actualPermit) {
        actualPermit.requireOriginalGroup(this, frozenRegistrations);
        synchronized (gate) {
            if (permit != null && permit != actualPermit) { throw new IllegalStateException("P1 close generation changed"); }
            permit = actualPermit;
        }
        advanceTermination();
        return terminated.copy();
    }

    private void advanceTermination() {
        synchronized (gate) {
            if (!frozen || host != null && permit == null || terminated.isDone()
                    || teardownStarting || teardown != null && !teardown.ioExited()) { return; }
            teardownStarting = true;
            try {
                teardown = OwnedCheckpointIo.submit(ignored -> {
                    if (!opening.ioExited()) { return false; }
                    final PulsarClient actual;
                    synchronized (gate) { actual = client; }
                    if (!sdkTerminated) {
                        if (actual == null) {
                            final var failed = openingFailure();
                            if (failed instanceof PulsarClientImpl.ConstructionCleanupFailure cleanup) {
                                try { cleanup.retryTermination(); }
                                catch (PulsarClientException failure) { throw new java.util.concurrent.CompletionException(failure); }
                            }
                            else {
                                // This pinned builder either failed before client construction or completed its
                                // constructor cleanup; unfinished cleanup is exposed as ConstructionCleanupFailure.
                                allocation.closed();
                            }
                        } else {
                            final CompletableFuture<Void> closing;
                            final List<Object> values;
                            synchronized (gate) { values = users.values().stream().map(use -> use.value).filter(Objects::nonNull).toList(); }
                            for (var value : values) { closeNative(value); }
                            synchronized (gate) {
                                if (firstClientClose == null) {
                                    firstClientClose = actual.closeAsync();
                                    firstClientClose.whenComplete((__, failure) -> {
                                        if (failure != null) { recordFailure(failure); }
                                        scheduleProgress();
                                    });
                                }
                                closing = firstClientClose;
                            }
                            if (!closing.isDone()) { return false; }
                            if (closing.isCompletedExceptionally()) {
                                try { actual.shutdown(); }
                                catch (PulsarClientException failure) { throw new java.util.concurrent.CompletionException(failure); }
                            }
                            if (!(actual instanceof PulsarClientImpl nativeClient)) {
                                throw new IllegalStateException("P1 group does not contain the locked native client");
                            }
                            try { nativeClient.awaitCloseShutdownTermination(settings.operationTimeoutMs(), TimeUnit.MILLISECONDS); }
                            catch (InterruptedException failure) {
                                Thread.currentThread().interrupt(); throw new java.util.concurrent.CompletionException(failure);
                            } catch (java.util.concurrent.TimeoutException failure) { throw new java.util.concurrent.CompletionException(failure); }
                        }
                        synchronized (gate) { sdkTerminated = true; }
                    }
                    final var proof = new TerminationProof(this);
                    final List<Participant> finishing;
                    synchronized (gate) { finishing = List.copyOf(participants); }
                    for (var participant : finishing) {
                        if (!participant.idle().getAsBoolean()) { return false; }
                        participant.finish().accept(proof);
                        if (!participant.released().getAsBoolean()) { return false; }
                    }
                    synchronized (gate) {
                        for (var use : users.values()) {
                            if (!use.creationObserved || !use.creationCallbackExited || use.activeReads != 0) { return false; }
                            if (use.allocation != null) { use.allocation.closed(); }
                        }
                        allocation.closed();
                    }
                    synchronized (CLIENTS) { CLIENTS.remove(actual); }
                    synchronized (HANDLES) { HANDLES.entrySet().removeIf(entry -> entry.getValue() == this); }
                    return true;
                }, () -> {}, io);
                teardown.receipt().whenComplete((done, failure) -> {
                    if (failure != null) { recordFailure(failure); }
                    else if (Boolean.TRUE.equals(done)) { terminated.complete(null); }
                    else {
                        synchronized (gate) {
                            if (!sdkTerminated && firstClientClose != null && firstClientClose.isDone()) { scheduleProgress(); }
                        }
                    }
                });
            } catch (RejectedExecutionException full) {
                if (io.isShutdown()) { recordFailure(full); }
                // The original Root/Host close turn retries this same owner; no native work was accepted.
            } catch (RuntimeException | Error failure) { recordFailure(failure); }
            finally { teardownStarting = false; }
        }
    }

    void cleanupProgress() {
        synchronized (gate) { if (!frozen || host != null && permit == null) { return; } }
        scheduleProgress();
    }

    private void scheduleProgress() {
        try { io.execute(this::advanceTermination); }
        catch (RejectedExecutionException full) { if (io.isShutdown()) { recordFailure(full); } }
        catch (RuntimeException | Error failure) { recordFailure(failure); }
    }

    public CompletableFuture<Void> requestHostQuarantine() {
        final TargetWorkerHostRuntime owner;
        synchronized (gate) { owner = host; }
        if (owner == null) { return CompletableFuture.failedFuture(new IllegalStateException("P1 group has no bound Host owner")); }
        owner.quarantineOwnedBackends();
        return terminated.copy();
    }

    private Throwable openingFailure() {
        return com.nereusstream.delay.ownership.CheckpointInputFuture.ownedFailure(opening.receipt());
    }
    private void recordFailure(Throwable failure) { synchronized (gate) { if (firstFailure == null) { firstFailure = failure; } } }
    @Override public boolean resourcesTerminated() { return terminated.isDone() && !terminated.isCompletedExceptionally(); }
    @Override public boolean nativeResourcesTerminated() { synchronized (gate) { return sdkTerminated; } }
    @Override public Throwable cleanupFailure() { synchronized (gate) { return firstFailure; } }
}
