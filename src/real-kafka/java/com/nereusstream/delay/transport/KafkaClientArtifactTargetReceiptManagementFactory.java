package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.OwnedCheckpointIo;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetSdkArtifactIdentity;
import com.nereusstream.delay.ownership.TargetReceiptControlHistory;
import com.nereusstream.delay.ownership.TargetReceiptCurrentManagement;
import com.nereusstream.delay.ownership.TargetReceiptCurrentManagement.Action;
import com.nereusstream.delay.ownership.TargetWorkerProcessRuntime;
import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.KafkaReceiptSlotResource;
import com.nereusstream.delay.protocol.TargetChannelControlRequest;
import com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact;
import com.nereusstream.delay.protocol.TargetKafkaReceiptManagementImage;
import com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog;
import com.nereusstream.delay.runtime.TargetChannelControlVerifier;
import com.nereusstream.delay.runtime.TargetKafkaReceiptProtectionTrust;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.SharedRocksDbResources;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.KafkaAdminClient;

/** Configured CONTROL backing on the original process graph; the Worker receives only catalog/protection/publisher. */
public final class KafkaClientArtifactTargetReceiptManagementFactory {
    private KafkaClientArtifactTargetReceiptManagementFactory() {}

    /** Authentication still comes from original registered CONTROL history and the live operator resolver. */
    public record TrustInputs(TargetChannelControlVerifier.KeyAuthority keys,
            TargetKafkaReceiptProtectionTrust.HistoricalActor actors,
            TargetKafkaReceiptProtectionTrust.ReceiptScope scope, TargetReceiptCurrentManagement current) {
        public TrustInputs { Objects.requireNonNull(keys); Objects.requireNonNull(actors); Objects.requireNonNull(scope); }
        /** A bare projection is not a current whole-topic permission; retained only for source compatibility. */
        public TrustInputs(TargetChannelControlVerifier.KeyAuthority keys,
                TargetKafkaReceiptProtectionTrust.HistoricalActor actors, TargetKafkaReceiptProtectionTrust.ReceiptScope scope,
                Supplier<ControlAuthorizationContext> legacyOperator) {
            this(keys, actors, scope, (TargetReceiptCurrentManagement) null); Objects.requireNonNull(legacyOperator);
        }
    }

    public record Receipt(TargetKafkaReceiptControlArtifact authority, int laneSlot, long slotGeneration,
            int slotsPerShard, int physicalPartition) {
        public Receipt {
            if (Objects.requireNonNull(authority).kind() != TargetKafkaReceiptControlArtifact.MANAGEMENT)
                throw new IllegalArgumentException("receipt scope requires an original signed management authorization");
            scope(authority, laneSlot, slotGeneration, slotsPerShard, physicalPartition);
        }
        private byte[] channel() { return ((TargetChannelControlRequest) authority.control().request().branch()).channel().digest(); }
    }

    public record Configuration(TargetKafkaReceiptControlArtifact management, PrivateKey key,
            Map<String,String> admin, byte[] sdkJar, Duration timeout, int maximumAcls, int maximumBrokers,
            CapacityVector cost, String catalogPrefix, OxiaSyncTargetKafkaReceiptProtectionCatalog.Limits storage,
            List<Receipt> receipts) {
        public Configuration {
            if (Objects.requireNonNull(management).kind() != TargetKafkaReceiptControlArtifact.MANAGEMENT)
                throw new IllegalArgumentException("configured receipt management requires its original signed deployment artifact");
            Objects.requireNonNull(key); Objects.requireNonNull(cost); Objects.requireNonNull(storage); Objects.requireNonNull(timeout);
            Bytes.requireLength(sdkJar, 32, "actual management SDK artifact"); sdkJar = Bytes.copy(sdkJar);
            admin = Map.copyOf(admin);
            if (timeout.toMillis() < 1000 || timeout.toMillis() > 60_000 || maximumAcls < 1 || maximumAcls > 4096
                    || maximumBrokers < 1 || maximumBrokers > 256 || catalogPrefix == null || catalogPrefix.isBlank()
                    || catalogPrefix.endsWith("/") || catalogPrefix.indexOf('\0') >= 0
                    || Bytes.utf8(catalogPrefix).length > 1024
                    || !java.text.Normalizer.isNormalized(catalogPrefix, java.text.Normalizer.Form.NFC))
                throw new IllegalArgumentException("receipt management requires finite explicit deployment bounds");
            final int bootstrap = requireAdmin(admin, maximumBrokers);
            final long connections = Math.addExact(maximumBrokers, bootstrap);
            if (cost.amount(CapacityDimension.ADAPTER_CONNECTIONS) < connections
                    || cost.amount(CapacityDimension.OPEN_FILES) < connections
                    || cost.amount(CapacityDimension.ADAPTER_THREADS) < 2
                    || cost.amount(CapacityDimension.OTHER_NATIVE_BYTES) == 0
                    || cost.amount(CapacityDimension.DIRECT_BUFFER_BYTES) == 0
                    || cost.amount(CapacityDimension.BUFFERED_BYTES) == 0
                    || cost.amount(CapacityDimension.BUFFERED_MESSAGES) == 0
                    || cost.amount(CapacityDimension.ADAPTER_PRODUCERS) != 0)
                throw new IllegalArgumentException("management cost does not cover its explicit Admin resources");
            receipts = List.copyOf(receipts);
            if (receipts.isEmpty() || receipts.size() > storage.maximumPublications()
                    || !Arrays.equals(receipts.getFirst().authority().canonicalBytes(), management.canonicalBytes()))
                throw new IllegalArgumentException("receipt inventory lacks its original first management authorization or exceeds storage bounds");
            final var channels = new HashSet<String>(); final var resources = new HashSet<String>();
            for (var receipt : receipts) {
                if (!Arrays.equals(management.image(), receipt.authority().image()) || !channels.add(Bytes.hex(receipt.channel())))
                    throw new IllegalArgumentException("duplicate/foreign original receipt channel deployment");
                final var scoped = scope(receipt.authority(), receipt.laneSlot(), receipt.slotGeneration(),
                        receipt.slotsPerShard(), receipt.physicalPartition());
                if (!resources.add(Bytes.hex(scoped.resource().canonicalBytes()) + "/" + Long.toUnsignedString(scoped.evidenceGeneration())))
                    throw new IllegalArgumentException("duplicate original receipt slot/evidence generation");
            }
            if (cost.amount(CapacityDimension.BUFFERED_BYTES) < retainedConfigurationBytes(management, admin, receipts))
                throw new IllegalArgumentException("management cost omits its retained bounded deployment inputs");
            final var deployment = TargetKafkaReceiptManagementImage.decode(management.image());
            if (!Arrays.equals(KafkaClientArtifactTargetReceiptManagement.configurationFingerprint(new HashMap<>(admin)),
                    deployment.configuredCredentialFingerprint()))
                throw new IllegalArgumentException("management credentials differ from its original signed deployment");
        }
        @Override public byte[] sdkJar() { return Bytes.copy(sdkJar); }
        @Override public String toString() { return "ConfiguredReceiptManagement"; }
    }

    public static Configuration load(Path path) throws IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        final var artifact = TargetKafkaReceiptControlArtifact.decode(f.image("management.artifactFile", 65536));
        final byte[] keyImage = f.image("control.privateKeyFile", 1024);
        final PrivateKey key;
        try { key = KeyFactory.getInstance("Ed25519").generatePrivate(new PKCS8EncodedKeySpec(keyImage)); }
        catch (GeneralSecurityException failure) { throw new IllegalArgumentException("invalid configured CONTROL signing key", failure); }
        finally { Arrays.fill(keyImage, (byte) 0); }
        final var storage = new OxiaSyncTargetKafkaReceiptProtectionCatalog.Limits(f.number("catalog.physicalBytes"),
                f.integer("catalog.physicalRecords"), f.integer("catalog.maximumPublications"), f.integer("catalog.maximumTopics"),
                f.integer("catalog.maximumPreparedReferences"), f.hex("catalog.capacitySourceDigest", 32, 32));
        final int count = f.integer("receipts.count");
        if (count < 1 || count > storage.maximumPublications()) throw new IllegalArgumentException("receipt scope count exceeds its finite catalog bound");
        final var receipts = new ArrayList<Receipt>();
        for (int i = 0; i < count; i++) {
            final String prefix = "receipts." + i + ".";
            receipts.add(new Receipt(TargetKafkaReceiptControlArtifact.decode(f.image(prefix + "artifactFile", 65536)),
                    f.integer(prefix + "laneSlot"), f.unsigned64(prefix + "slotGeneration"),
                    f.integer(prefix + "slotsPerShard"), f.integer(prefix + "physicalPartition")));
        }
        final var result = new Configuration(artifact, key, f.prefixed("kafka.", 64), f.hex("sdk.sha256", 32, 32),
                Duration.ofMillis(f.number("management.timeoutMs")), f.integer("management.maximumAcls"),
                f.integer("management.maximumBrokers"), f.capacity("management.cost."), f.text("catalog.prefix"), storage, receipts);
        f.requireComplete(); return result;
    }

    private static long retainedConfigurationBytes(TargetKafkaReceiptControlArtifact management,
            Map<String,String> admin, List<Receipt> receipts) {
        long bytes = management.canonicalBytes().length;
        for (var receipt : receipts) bytes = Math.addExact(bytes, receipt.authority().canonicalBytes().length);
        for (var entry : admin.entrySet()) bytes = Math.addExact(bytes,
                Math.addExact(Bytes.utf8(entry.getKey()).length, Bytes.utf8(entry.getValue()).length));
        return bytes;
    }

    /** Old projections alone cannot establish current management authority. */
    public static Opening openConfigured(TargetWorkerProcessRuntime root, Path configuration, Path controlHistory,
            TargetDestinationSdkBudget budget, Supplier<ControlAuthorizationContext> authenticatedOperator) throws IOException {
        throw new IllegalArgumentException("current whole-topic policy and actual authenticator snapshot provider are mandatory");
    }

    /** Historical and current views have independent root inputs; current policy materialization stays on owned I/O. */
    public static Opening openConfigured(TargetWorkerProcessRuntime root, Path configuration, Path controlHistory,
            Path currentPolicy, TargetDestinationSdkBudget budget,
            TargetReceiptCurrentManagement.Authenticator authenticator) throws IOException {
        Objects.requireNonNull(authenticator, "original live CONTROL authenticator");
        final var config = load(configuration);
        final var captured = TargetReceiptControlHistory.prepare(controlHistory);
        final var policy = TargetReceiptCurrentManagement.prepare(currentPolicy);
        final long bytes = Math.addExact(Math.addExact(captured.maximumRetainedBytes(), policy.maximumRetainedBytes()),
                retainedConfigurationBytes(config.management(), config.admin(), config.receipts()));
        if (config.cost().amount(CapacityDimension.BUFFERED_BYTES) < bytes)
            throw new IllegalArgumentException("management reservation omits its history/current-policy input bounds");
        return openLoaded(root, config, budget, guard -> {
            try {
                final var history = TargetReceiptControlHistory.load(captured, guard);
                for (var receipt : config.receipts()) {
                    guard.run();
                    final var required = scope(receipt.authority(), receipt.laneSlot(), receipt.slotGeneration(),
                            receipt.slotsPerShard(), receipt.physicalPartition());
                    history.requireReceipt(receipt.authority(), required.resource(), required.slotsPerShard(), required.physicalPartition());
                }
                final var current = TargetReceiptCurrentManagement.load(policy, authenticator, guard);
                return new TrustInputs(history, history, history, current);
            } catch (IOException failure) { throw new IllegalStateException("original CONTROL input materialization failed", failure); }
        });
    }

    /** No native SDK or request is created until this exact owner and cost have been retained by the original root. */
    public static Opening openConfigured(TargetWorkerProcessRuntime root, Path path,
            TargetDestinationSdkBudget budget, TrustInputs inputs) throws IOException {
        Objects.requireNonNull(inputs, "original trusted CONTROL providers");
        Objects.requireNonNull(inputs.current(), "actual current whole-topic scope authority");
        return openLoaded(root, load(path), budget, guard -> inputs);
    }

    private static Opening openLoaded(TargetWorkerProcessRuntime root, Configuration config,
            TargetDestinationSdkBudget budget, java.util.function.Function<Runnable,TrustInputs> trustLoader) {
        final var original = new Opening(root, config, budget, trustLoader);
        try (var construction = root.beginReceiptManagementConstruction(original, budget)) {
            original.registered = true;
            construction.requireCurrent(); original.start(construction.graph()); construction.requireCurrent(); return original;
        } catch (RuntimeException | Error failure) {
            if (original.registered) original.failed(failure);
            throw failure;
        }
    }

    private static KafkaClientArtifactTargetReceiptManagement.Scope scope(TargetKafkaReceiptControlArtifact artifact,
            int slot, long generation, int slots, int partition) {
        if (!(artifact.control().request().branch() instanceof TargetChannelControlRequest request)
                || request.channel().context().kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT)
            throw new IllegalArgumentException("receipt deployment requires its registered K2 channel CONTROL request");
        final var context = request.channel().context();
        final var image = TargetKafkaReceiptManagementImage.decode(artifact.image());
        final var kafka = image.topic().kafka();
        return new KafkaClientArtifactTargetReceiptManagement.Scope(new KafkaReceiptSlotResource(kafka.authenticatedClusterId(),
                kafka.nativeTopicUuid(), context.sourceShard().routeIncarnation(), context.sourceShard().partition(), slot, generation),
                image.physicalTopic(), slots, partition, Objects.requireNonNull(context.evidenceGeneration()),
                image.capability().ref(), context.controlScopeRef());
    }

    /** Literal PEM is covered by the signed configuration fingerprint; mutable file references or plugin factories cannot replace it. */
    private static int requireAdmin(Map<String,String> admin, int maximumBrokers) {
        if (admin.isEmpty() || admin.size() > 64) throw new IllegalArgumentException("management operational field bound exceeded");
        if (!"SSL".equals(admin.get("security.protocol")) || !"PEM".equals(admin.get("ssl.keystore.type"))
                || !"PEM".equals(admin.get("ssl.truststore.type")) || !"https".equals(admin.get("ssl.endpoint.identification.algorithm"))
                || !"false".equals(admin.get("enable.metrics.push")))
            throw new IllegalArgumentException("configured receipt management requires exact literal mTLS and finite local metrics");
        for (var key : new String[] {"ssl.keystore.key", "ssl.keystore.certificate.chain", "ssl.truststore.certificates"})
            if (!admin.containsKey(key) || admin.get(key).isBlank()) throw new IllegalArgumentException("missing configured management TLS material");
        for (var entry : admin.entrySet()) {
            final String key = entry.getKey(), value = entry.getValue();
            if (key.isBlank() || key.length() > 128 || value.isBlank() || value.length() > 4096
                    || key.startsWith("sasl.") || key.equals("ssl.keystore.location") || key.equals("ssl.truststore.location")
                    || key.equals("ssl.engine.factory.class") || key.equals("metric.reporters") || key.equals("security.providers"))
                throw new IllegalArgumentException("unsupported/nonliteral configured management field");
        }
        final String[] endpoints = Objects.requireNonNull(admin.get("bootstrap.servers"), "explicit management endpoint").split(",", -1);
        if (endpoints.length < 1 || endpoints.length > maximumBrokers || Arrays.stream(endpoints).anyMatch(String::isBlank))
            throw new IllegalArgumentException("management bootstrap inventory exceeds its finite bound");
        return endpoints.length;
    }

    /** Privileged material stays in this original owner; only actual verified results are exposed to Worker assembly. */
    public static final class Opening implements TargetWorkerProcessRuntime.ReceiptManagement {
        private final TargetWorkerProcessRuntime root;
        private final Configuration config;
        private final TargetDestinationSdkBudget budget;
        private final java.util.function.Function<Runnable,TrustInputs> trustLoader;
        private final byte[] identity;
        private volatile OwnedCheckpointIo<Void> opening;
        private OwnedCheckpointIo<Boolean> closing;
        private java.util.concurrent.ThreadPoolExecutor io;
        private volatile Throwable cleanupFailure;
        private volatile TargetDestinationSdkBudget.Client allocation;
        private volatile KafkaClientArtifactTargetReceiptManagement manager;
        private volatile OxiaSyncTargetKafkaReceiptProtectionCatalog catalog;
        private Map<String,TargetKafkaReceiptControlArtifact> protections = Map.of();
        private Map<String,KafkaClientArtifactTargetReceiptProvider.AnchorPublisher> publishers = Map.of();
        private volatile boolean registered;
        private volatile boolean nativeStarted;
        private volatile boolean prepared;
        private volatile boolean stopping;
        private volatile boolean closed;
        private volatile Throwable failure;
        private Opening(TargetWorkerProcessRuntime root, Configuration config, TargetDestinationSdkBudget budget,
                java.util.function.Function<Runnable,TrustInputs> trustLoader) {
            this.root = Objects.requireNonNull(root); this.config = config; this.budget = Objects.requireNonNull(budget);
            this.trustLoader = Objects.requireNonNull(trustLoader);
            identity = TargetKafkaReceiptManagementImage.decode(config.management().image()).topicIdentity();
        }
        private void start(TargetWorkerProcessRuntime.Assembly graph) {
            io = graph.io();
            allocation = budget.reserve("receipt-management/" + Bytes.hex(identity), config.cost());
            opening = OwnedCheckpointIo.submit(guard -> {
                try {
                    guard.run(); TargetSdkArtifactIdentity.requireExact(KafkaAdminClient.class, config.sdkJar());
                    final var inputs = Objects.requireNonNull(trustLoader.apply(guard), "actual original CONTROL inputs");
                    guard.run();
                    final var trust = new TargetKafkaReceiptProtectionTrust(graph.oxia().providers().registrations(),
                            inputs.keys(), inputs.actors(), inputs.scope(), (image, control, at) ->
                                    Objects.requireNonNull(manager, "actual receipt Admin backing").requireCurrent(image, control, at), inputs.current());
                    final var original = config.management();
                    final var image = TargetKafkaReceiptManagementImage.decode(original.image());
                    for (var receipt : config.receipts()) {
                        guard.run(); final var authority = receipt.authority();
                        try (var permission = trust.currentManagement(authority, Action.OBSERVE)) { permission.requireCurrent(); }
                        trust.requireIssuerSignature(TargetKafkaReceiptControlArtifact.signManagement(image,
                                authority.control(), authority.authorizedAt(), config.key()));
                    }
                    catalog = graph.oxia().providers().receiptProtectionCatalog(config.catalogPrefix(), trust, config.storage());
                    guard.run(); nativeStarted = true; allocation.beginConstruction();
                    manager = KafkaClientArtifactTargetReceiptManagement.open(new HashMap<>(config.admin()), image,
                            config.key(), config.timeout(), config.maximumAcls(), config.maximumBrokers());
                    manager.attach(catalog, trust); guard.run();
                    final var active = new LinkedHashMap<String,TargetKafkaReceiptControlArtifact>();
                    final var restricted = new LinkedHashMap<String,KafkaClientArtifactTargetReceiptProvider.AnchorPublisher>();
                    for (var receipt : config.receipts()) {
                        guard.run(); final var authority = receipt.authority();
                        final var protection = manager.establishForStartup(scope(authority, receipt.laneSlot(), receipt.slotGeneration(),
                                receipt.slotsPerShard(), receipt.physicalPartition()), authority.control(), authority.authorizedAt(),
                                config.key(), trust, catalog);
                        final String channel = Bytes.hex(receipt.channel());
                        active.put(channel, protection);
                        restricted.put(channel, manager.anchorPublisher(protection, config.key(), trust, catalog, graph.io()));
                    }
                    guard.run(); protections = Map.copyOf(active); publishers = Map.copyOf(restricted); prepared = true; return null;
                } catch (RuntimeException | Error rejected) { failed(rejected); throw rejected; }
            }, () -> {}, graph.io());
        }
        private void failed(Throwable rejected) {
            synchronized (this) { if (failure != null) return; failure = rejected; }
            root.failReceiptManagement(this, rejected);
        }
        @Override public byte[] identity() { return Bytes.copy(identity); }
        @Override public void requireGraph(WorkClassExecutionRegistry classes, SharedRocksDbResources resources) {
            budget.requireWorker(classes, resources);
        }
        @Override public boolean ready() {
            final var operation = opening;
            return prepared && !stopping && failure == null && operation != null && operation.ioExited();
        }
        private void requireReady() { if (!ready()) throw new IllegalStateException("original CONTROL receipt backing is not ready", failure); }
        public OxiaSyncTargetKafkaReceiptProtectionCatalog catalog() { requireReady(); return catalog; }
        public TargetKafkaReceiptControlArtifact protection(byte[] channelDigest) {
            requireReady(); return Objects.requireNonNull(protections.get(Bytes.hex(channelDigest)), "original configured receipt channel");
        }
        public KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher(byte[] channelDigest) {
            requireReady(); return Objects.requireNonNull(publishers.get(Bytes.hex(channelDigest)), "original configured receipt channel");
        }
        public Optional<Throwable> failure() { return Optional.ofNullable(failure); }
        @Override public void requestStop() {
            stopping = true;
            final var operation = opening; if (operation != null) operation.requestStop();
            final var actual = manager; if (actual != null) actual.requestStop();
        }
        @Override public synchronized boolean closeForProcess(TargetWorkerProcessRuntime.ReceiptManagementClosePermit permit) {
            permit.requireOriginal(this); requestStop();
            if (closed) return true;
            final var operation = opening;
            if (operation != null) {
                try { operation.retryCleanup(); }
                catch (java.util.concurrent.RejectedExecutionException busy) {
                    if (io.isShutdown()) rememberCleanup(busy);
                    return false;
                }
                if (!operation.ioExited()) return false;
            }
            final var actual = manager;
            if (actual != null && !closeNative(actual, permit)) return false;
            final var charge = allocation;
            if (charge != null) { if (nativeStarted) charge.closed(); else charge.releaseUnstarted(); }
            closed = true; return true;
        }
        private synchronized boolean closeNative(KafkaClientArtifactTargetReceiptManagement actual,
                TargetWorkerProcessRuntime.ReceiptManagementClosePermit permit) {
            if (closing != null) {
                if (!closing.ioExited()) return false;
                final var completed = closing; closing = null;
                try { if (Boolean.TRUE.equals(completed.receipt().join())) return true; }
                catch (RuntimeException failure) {
                    rememberCleanup(failure.getCause() == null ? failure : failure.getCause());
                    return false;
                }
            }
            try {
                closing = OwnedCheckpointIo.submit(guard -> {
                    try { guard.run(); final boolean exited = actual.settleClose(); guard.run(); return exited; }
                    finally { if (actual.firstCloseFailure() != null) rememberCleanup(actual.firstCloseFailure()); }
                }, () -> permit.requireOriginal(this), io);
            } catch (java.util.concurrent.RejectedExecutionException busy) {
                if (io.isShutdown()) rememberCleanup(busy);
            }
            return false;
        }
        private synchronized void rememberCleanup(Throwable rejected) { if (cleanupFailure == null) cleanupFailure = rejected; }
        @Override public Throwable cleanupFailure() { return cleanupFailure; }
    }
}
