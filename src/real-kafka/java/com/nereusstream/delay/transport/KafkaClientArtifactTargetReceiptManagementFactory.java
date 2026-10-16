package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.OwnedCheckpointIo;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetSdkArtifactIdentity;
import com.nereusstream.delay.ownership.TargetReceiptControlHistory;
import com.nereusstream.delay.ownership.TargetReceiptCurrentManagement;
import com.nereusstream.delay.ownership.TargetReceiptCurrentManagement.Action;
import com.nereusstream.delay.ownership.TargetRecoveryCredentialAuthorization;
import com.nereusstream.delay.ownership.TargetPublishRecoveryMaintenance;
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
        return openConfigured(root, configuration, controlHistory, currentPolicy, null, List.of(), budget, authenticator);
    }

    /** Recovery proof purpose/key inputs share this original management owner, its cost and its I/O lifecycle. */
    public static Opening openConfigured(TargetWorkerProcessRuntime root, Path configuration, Path controlHistory,
            Path currentPolicy, Path recoveryArchive,
            List<TargetRecoveryCredentialAuthorization.Trust> originalRecoveryMaterialHistory,
            TargetDestinationSdkBudget budget, TargetReceiptCurrentManagement.Authenticator authenticator) throws IOException {
        return openConfigured(root, configuration, controlHistory, currentPolicy, recoveryArchive, null,
                originalRecoveryMaterialHistory, budget, authenticator);
    }

    /** Retirement uses a separate explicit signing-purpose manifest; recovery signing never implies this purpose. */
    public static Opening openConfigured(TargetWorkerProcessRuntime root, Path configuration, Path controlHistory,
            Path currentPolicy, Path recoveryArchive, Path retirementSigning,
            List<TargetRecoveryCredentialAuthorization.Trust> originalRecoveryMaterialHistory,
            TargetDestinationSdkBudget budget, TargetReceiptCurrentManagement.Authenticator authenticator) throws IOException {
        return openConfigured(root,configuration,controlHistory,currentPolicy,recoveryArchive,retirementSigning,null,
                originalRecoveryMaterialHistory,budget,authenticator);
    }

    /** Historical Source authorization has an independent purpose/issuer pin and never inherits the signing manifest. */
    public static Opening openConfigured(TargetWorkerProcessRuntime root, Path configuration, Path controlHistory,
            Path currentPolicy, Path recoveryArchive, Path retirementSigning,Path retirementHistory,
            List<TargetRecoveryCredentialAuthorization.Trust> originalRecoveryMaterialHistory,
            TargetDestinationSdkBudget budget, TargetReceiptCurrentManagement.Authenticator authenticator) throws IOException {
        Objects.requireNonNull(authenticator, "original live CONTROL authenticator");
        final var config = load(configuration);
        final var captured = TargetReceiptControlHistory.prepare(controlHistory);
        final var policy = TargetReceiptCurrentManagement.prepare(currentPolicy);
        final var recovery = recoveryArchive == null ? null : KafkaClientArtifactTargetRecoveryArchiveConfiguration.prepare(
                recoveryArchive, config.catalogPrefix(), originalRecoveryMaterialHistory);
        if (recovery == null && !originalRecoveryMaterialHistory.isEmpty())
            throw new IllegalArgumentException("recovery material history requires its explicit archive configuration");
        if (retirementSigning != null && recovery == null)
            throw new IllegalArgumentException("recovery-object retirement requires original archive history");
        final var retirement = retirementSigning == null ? null : KafkaClientArtifactTargetEvidenceRetirementConfiguration.prepare(
                retirementSigning, TargetKafkaReceiptManagementImage.decode(config.management().image()).deploymentRootDigest());
        final var historyInputs=retirementHistory==null?null:KafkaClientArtifactTargetRetirementHistoryConfiguration.prepare(retirementHistory,
                TargetKafkaReceiptManagementImage.decode(config.management().image()).deploymentRootDigest());
        final long bytes = Math.addExact(Math.addExact(captured.maximumRetainedBytes(), policy.maximumRetainedBytes()),
                Math.addExact(retainedConfigurationBytes(config.management(), config.admin(), config.receipts()),
                        Math.addExact(recovery == null ? 0 : recovery.maximumRetainedBytes(),
                                Math.addExact(retirement == null ? 0 : retirement.maximumRetainedBytes(),historyInputs==null?0:historyInputs.maximumRetainedBytes()))));
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
        }, recovery, retirement, historyInputs,bytes);
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
        return openLoaded(root, config, budget, trustLoader, null);
    }

    private static Opening openLoaded(TargetWorkerProcessRuntime root, Configuration config,
            TargetDestinationSdkBudget budget, java.util.function.Function<Runnable,TrustInputs> trustLoader,
            KafkaClientArtifactTargetRecoveryArchiveConfiguration.Inputs recovery) {
        return openLoaded(root,config,budget,trustLoader,recovery,Math.addExact(
                retainedConfigurationBytes(config.management(),config.admin(),config.receipts()),recovery==null?0:recovery.maximumRetainedBytes()));
    }
    private static Opening openLoaded(TargetWorkerProcessRuntime root, Configuration config,
            TargetDestinationSdkBudget budget, java.util.function.Function<Runnable,TrustInputs> trustLoader,
            KafkaClientArtifactTargetRecoveryArchiveConfiguration.Inputs recovery,long retainedInputBytes) {
        return openLoaded(root, config, budget, trustLoader, recovery, null, retainedInputBytes);
    }
    private static Opening openLoaded(TargetWorkerProcessRuntime root, Configuration config,
            TargetDestinationSdkBudget budget, java.util.function.Function<Runnable,TrustInputs> trustLoader,
            KafkaClientArtifactTargetRecoveryArchiveConfiguration.Inputs recovery,
            KafkaClientArtifactTargetEvidenceRetirementConfiguration.Inputs retirement,long retainedInputBytes) {
        return openLoaded(root,config,budget,trustLoader,recovery,retirement,null,retainedInputBytes);
    }
    private static Opening openLoaded(TargetWorkerProcessRuntime root, Configuration config,
            TargetDestinationSdkBudget budget, java.util.function.Function<Runnable,TrustInputs> trustLoader,
            KafkaClientArtifactTargetRecoveryArchiveConfiguration.Inputs recovery,
            KafkaClientArtifactTargetEvidenceRetirementConfiguration.Inputs retirement,
            KafkaClientArtifactTargetRetirementHistoryConfiguration.Inputs history,long retainedInputBytes) {
        final var original = new Opening(root, config, budget, trustLoader, recovery,retirement,history,retainedInputBytes);
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
        private KafkaClientArtifactTargetRecoveryArchiveConfiguration.Inputs recoveryInputs;
        private volatile KafkaClientArtifactTargetRecoveryArchiveConfiguration.Loaded recoveryArchive;
        private KafkaClientArtifactTargetEvidenceRetirementConfiguration.Inputs retirementSigningInputs;
        private volatile KafkaClientArtifactTargetEvidenceRetirementConfiguration.Loaded retirementSigning;
        private KafkaClientArtifactTargetRetirementHistoryConfiguration.Inputs retirementHistoryInputs;
        private volatile KafkaClientArtifactTargetRetirementHistoryConfiguration.Loaded retirementHistory;
        private final long retainedInputBytes;
        private volatile OwnedCheckpointIo<OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject> referenceClosure;
        private String referenceClosureKey;
        private byte[] referenceClosureOwner;
        private volatile OwnedCheckpointIo<com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.ClosedUses> closedUsesRead;
        private KafkaClientArtifactTargetWorkerSourceFactory.Opening closedUsesSource;
        private byte[] closedUsesSelection;
        private volatile OwnedCheckpointIo<RetirementPreparation> retirementPreparation;
        private KafkaClientArtifactTargetWorkerSourceFactory.Opening retirementSource;
        private byte[] retirementSelection;
        private volatile OwnedCheckpointIo<com.nereusstream.delay.protocol.SystemMutation> retirementSignature;
        private byte[] retirementSignatureSelection;
        private volatile OwnedCheckpointIo<Boolean> retirementRelease;
        private volatile com.nereusstream.delay.ownership.TargetEvidenceRetirementWorkClassExecutor.Submission retirementSubmission;
        private boolean retirementSubmitting;
        private RetirementSourceCopy retirementSourceCopy;
        private RetirementHistoryInput retirementHistoryInput;
        private Map<String,TargetKafkaReceiptControlArtifact> protections = Map.of();
        private Map<String,KafkaClientArtifactTargetReceiptProvider.AnchorPublisher> publishers = Map.of();
        private volatile boolean registered;
        private volatile boolean nativeStarted;
        private volatile boolean prepared;
        private volatile boolean stopping;
        private volatile boolean closed;
        private volatile Throwable failure;
        private Opening(TargetWorkerProcessRuntime root, Configuration config, TargetDestinationSdkBudget budget,
                java.util.function.Function<Runnable,TrustInputs> trustLoader,
                KafkaClientArtifactTargetRecoveryArchiveConfiguration.Inputs recoveryInputs,
                KafkaClientArtifactTargetEvidenceRetirementConfiguration.Inputs retirementSigningInputs,
                KafkaClientArtifactTargetRetirementHistoryConfiguration.Inputs retirementHistoryInputs,long retainedInputBytes) {
            this.root = Objects.requireNonNull(root); this.config = config; this.budget = Objects.requireNonNull(budget);
            this.trustLoader = Objects.requireNonNull(trustLoader);
            this.recoveryInputs = recoveryInputs;
            this.retirementSigningInputs = retirementSigningInputs;
            this.retirementHistoryInputs=retirementHistoryInputs;
            this.retainedInputBytes=retainedInputBytes;
            identity = TargetKafkaReceiptManagementImage.decode(config.management().image()).topicIdentity();
        }
        private void start(TargetWorkerProcessRuntime.Assembly graph) {
            io = graph.io();
            allocation = budget.reserve("receipt-management/" + Bytes.hex(identity), config.cost());
            opening = OwnedCheckpointIo.submit(guard -> {
                try {
                    guard.run(); TargetSdkArtifactIdentity.requireExact(KafkaAdminClient.class, config.sdkJar());
                    final var inputs = Objects.requireNonNull(trustLoader.apply(guard), "actual original CONTROL inputs");
                    if (recoveryInputs != null) {
                        try { recoveryArchive = KafkaClientArtifactTargetRecoveryArchiveConfiguration.load(recoveryInputs, guard); }
                        catch (IOException rejected) { throw new IllegalStateException("original recovery signing input materialization failed", rejected); }
                        if (stopping) recoveryArchive.requestStop();
                    }
                    if (retirementSigningInputs != null) {
                        try { retirementSigning = KafkaClientArtifactTargetEvidenceRetirementConfiguration.load(retirementSigningInputs, guard); }
                        catch (IOException rejected) { throw new IllegalStateException("original retirement signing input materialization failed", rejected); }
                        if (stopping) retirementSigning.requestStop();
                    }
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
                    if(retirementHistoryInputs!=null){
                        try{retirementHistory=KafkaClientArtifactTargetRetirementHistoryConfiguration.load(retirementHistoryInputs,catalog.namespaceIdentity(image),guard);}
                        catch(IOException rejected){throw new IllegalStateException("original retirement authorization history materialization failed",rejected);}
                        if(stopping)retirementHistory.requestStop();
                    }
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
            }, () -> { if (stopping) throw new IllegalStateException("original receipt management owner stopped"); }, graph.io());
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

        /** Only an actual Native APPLIED recovery owner can enter this first closure path; other object lifetimes stay separate. */
        public synchronized java.util.concurrent.CompletableFuture<OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject> closeAdoptedRecoveryReferences(
                com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption applied,
                com.nereusstream.delay.store.BoundedReadBudget reads) {
            final var original=requireRecoveryArchive();Objects.requireNonNull(reads);
            if(closedUsesRead!=null&&!closedUsesRead.ioExited())throw new IllegalStateException("original closed-use read I/O remains owned");
            requireRetirementIoExited();
            final var owner=Objects.requireNonNull(applied,"actual Native recovery adoption").owner();
            final String key=owner.reference().exactKey();final byte[] image=Bytes.sha256(owner.canonicalBytes());
            final var previous=referenceClosure;
            if(previous!=null){
                if(!previous.ioExited()&&!key.equals(referenceClosureKey))throw new IllegalStateException("original object closure I/O remains owned");
                if(key.equals(referenceClosureKey)){
                    if(!Arrays.equals(image,referenceClosureOwner))throw new IllegalStateException("closure changed its original Native adoption input");
                    previous.retryCleanup();if(!previous.ioExited()||previous.completedValue().isPresent())return previous.receipt();
                }
            }
            if(reads.maxBytes()==Long.MAX_VALUE)throw new IllegalArgumentException("closure reads require an explicit finite byte limit");
            final long[] minimum=new long[CapacityDimension.COUNT];
            minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,
                    OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes());
            allocation.requireCovers(new CapacityVector(minimum));
            final var next=OwnedCheckpointIo.submit(guard->{final Runnable current=()->{guard.run();requireReady();};current.run();
                final var result=catalog.closeAdoptedRecoveryReferences(applied,original.trust,reads,current);current.run();return result;
            },this::requireReady,io);
            referenceClosureKey=key;referenceClosureOwner=image;referenceClosure=next;return next.receipt();
        }

        /** Complete Native preparation facts, never a durable per-use or external-delete authorization. */
        public synchronized java.util.concurrent.CompletableFuture<com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.ClosedUses> readClosedRecoveryUses(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,
                OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption applied,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            requireReady();requireRetirementIoExited();Objects.requireNonNull(source);Objects.requireNonNull(closure);Objects.requireNonNull(applied);
            Objects.requireNonNull(catalogReads);Objects.requireNonNull(nativeReads);
            final byte[] selection=Bytes.sha256(closure.canonicalBytes(),applied.owner().canonicalBytes());
            final var previous=closedUsesRead;
            if(previous!=null){
                previous.retryCleanup();
                if(!previous.ioExited()){
                    if(source!=closedUsesSource||!Arrays.equals(selection,closedUsesSelection))
                        throw new IllegalStateException("original closed-use read selection remains owned");
                    return previous.receipt();
                }
            }
            final var closing=referenceClosure;
            if(closing!=null&&!closing.ioExited())throw new IllegalStateException("original reference closure I/O remains owned");
            if(catalogReads.maxBytes()==Long.MAX_VALUE||nativeReads.maxBytes()==Long.MAX_VALUE)
                throw new IllegalArgumentException("closed-use reads require explicit finite byte limits");
            final long[] minimum=new long[CapacityDimension.COUNT];
            minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,
                    Math.addExact(OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),nativeReads.maxBytes()));
            allocation.requireCovers(new CapacityVector(minimum));
            final var next=OwnedCheckpointIo.submit(guard->{final Runnable current=()->{guard.run();requireReady();};
                return source.readClosedRecoveryUses(this,closure,applied,catalogReads,nativeReads,current);
            },this::requireReady,io);
            closedUsesSource=source;closedUsesSelection=selection;closedUsesRead=next;return next.receipt();
        }
        private void requireRetirementIoExited(){
            if(retirementHistoryInput!=null)throw new IllegalStateException("original Source retirement history remains owned");
            if(retirementSourceCopy!=null)throw new IllegalStateException("original Source retirement input remains owned");
            if(retirementPreparation!=null&&!retirementPreparation.ioExited())
                throw new IllegalStateException("original retirement preparation I/O remains owned");
            if(retirementPreparation!=null&&retirementPreparation.completedValue().filter(value->!value.released).isPresent())
                throw new IllegalStateException("original prepared retirement input remains owned");
            requireRetirementAuxiliaryIoExited();
        }
        private void requireRetirementAuxiliaryIoExited(){
            if(retirementSignature!=null&&!retirementSignature.ioExited()||retirementRelease!=null&&!retirementRelease.ioExited())
                throw new IllegalStateException("original retirement signing/release I/O remains owned");
        }
        /** Actual prepared public bytes and Native cut. Source signing/apply must still revalidate its own authority/state. */
        public static final class RetirementPreparation {
            private final Opening root;
            private com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.ClosedUses uses;
            private OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy archived;
            private com.nereusstream.delay.protocol.TargetEvidenceRetirementContext context;
            private final long reservedWindowBytes;
            private boolean released,sourceBorrowed;
            private RetirementPreparation(Opening root,com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.ClosedUses uses,
                    OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy archived,long reservedWindowBytes){
                this.root=root;this.uses=uses;this.archived=archived;this.reservedWindowBytes=reservedWindowBytes;archived.requireCatalog(root.catalog);
                final var copy=archived.image();
                if(!Arrays.equals(copy.resource().canonicalBytes(),uses.resource().canonicalBytes()))
                    throw new IllegalStateException("prepared copy changed actual Native owner/resource");
                context=uses.retirementContext(copy.digest(),copy.canonicalBytes().length);copy.requireContext(context);
            }
            public com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.ClosedUses uses(){synchronized(root){requireRoot(root);return uses;}}
            public com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy copy(){synchronized(root){requireRoot(root);return archived.image();}}
            public com.nereusstream.delay.protocol.TargetEvidenceRetirementContext context(){synchronized(root){requireRoot(root);return context;}}
            /** Actual receipt only; Source purpose/history/capacity approval remains independent and mandatory. */
            public OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy authenticatedCopy(){synchronized(root){requireRoot(root);return archived;}}
            public void requireRoot(Opening expected){
                synchronized(root){
                    if(root!=expected)throw new IllegalArgumentException("retirement preparation belongs to another original Root");
                    root.requireReady();
                    if(released||root.retirementPreparation==null||!root.retirementPreparation.ioExited()
                            ||root.retirementPreparation.completedValue().orElse(null)!=this)
                        throw new IllegalStateException("original prepared retirement input is released or unavailable");
                }
            }
            private void releaseLocal(){
                if(sourceBorrowed)throw new IllegalStateException("original Source still holds retirement input");
                released=true;uses=null;archived=null;context=null;
            }
        }
        public synchronized java.util.concurrent.CompletableFuture<RetirementPreparation> prepareRecoveryRetirement(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,
                OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption applied,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            final var original=requireRecoveryArchive();Objects.requireNonNull(source);Objects.requireNonNull(closure);Objects.requireNonNull(applied);
            if(retirementHistoryInput!=null)throw new IllegalStateException("original Source retirement history remains owned");
            if(retirementSourceCopy!=null)throw new IllegalStateException("original Source retirement input remains owned");
            Objects.requireNonNull(catalogReads);Objects.requireNonNull(nativeReads);
            if(catalogReads.maxBytes()==Long.MAX_VALUE||nativeReads.maxBytes()==Long.MAX_VALUE)
                throw new IllegalArgumentException("retirement preparation requires finite read bytes");
            final long[] baseInput=new long[CapacityDimension.COUNT];
            baseInput[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,
                    Math.addExact(OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),nativeReads.maxBytes()));
            allocation.requireCovers(new CapacityVector(baseInput));
            final byte[] selection=Bytes.sha256(closure.canonicalBytes(),applied.owner().canonicalBytes());
            final var previous=retirementPreparation;
            requireRetirementAuxiliaryIoExited();
            if(previous!=null){previous.retryCleanup();if(!previous.ioExited()||previous.completedValue().filter(value->!value.released).isPresent()){
                if(source!=retirementSource||!Arrays.equals(selection,retirementSelection))
                    throw new IllegalStateException("original retirement preparation selection remains owned");
                return previous.receipt();
            }}
            if(referenceClosure!=null&&!referenceClosure.ioExited()||closedUsesRead!=null&&!closedUsesRead.ioExited())
                throw new IllegalStateException("original closure/use I/O remains owned");
            retirementSignature=null;retirementSignatureSelection=null;retirementRelease=null;retirementSubmission=null;
            final var resource=closure.recoveryResource(applied.owner());
            final long copyWindow=com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.estimatedBytes(resource,
                    resource.canonicalValueBytes(),com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.MAX_PUBLIC_INPUT_BYTES,
                    closure.canonicalBytes().length);
            final long[] minimum=new long[CapacityDimension.COUNT];
            minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,Math.addExact(
                    OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),Math.addExact(nativeReads.maxBytes(),Math.multiplyExact(4L,copyWindow))));
            allocation.requireCovers(new CapacityVector(minimum));
            final var next=OwnedCheckpointIo.submit(guard->{final Runnable current=()->{guard.run();requireReady();};
                final var uses=source.readClosedRecoveryUses(this,closure,applied,catalogReads,nativeReads,current);
                final var copy=catalog.prepareRecoveryRetirementCopy(uses.closure(),uses.owner(),original.trust,catalogReads,current);
                source.requireRetirementReadCurrent(this,current);return new RetirementPreparation(this,uses,copy,
                        minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]);
            },this::requireReady,io);
            retirementSource=source;retirementSelection=selection;retirementPreparation=next;return next.receipt();
        }

        /**
         * Synchronous history-only input acquisition on the caller's original Source owned-I/O turn.
         * Does not require an admitted Worker or old signing/transport credentials. Caller closes the loan
         * or transfers it to a Source hold; native owner/ref/first/cut validation remains in Source apply.
         */
        public RetirementSourceCopy readRetirementSourceCopy(com.nereusstream.delay.ownership.SourceReplayMutation entry,
                com.nereusstream.delay.store.BoundedReadBudget reads,Runnable current){
            Objects.requireNonNull(entry);Objects.requireNonNull(reads);Objects.requireNonNull(current);
            if(entry.mutation().type()!=com.nereusstream.delay.protocol.SystemMutationType.RESOURCE_RETIRE_INTENT
                    ||reads.maxBytes()==Long.MAX_VALUE)throw new IllegalArgumentException("bounded original retirement Source input required");
            final RetirementSourceCopy loan;final KafkaClientArtifactTargetRecoveryArchiveConfiguration.Loaded history;
            synchronized(this){
                history=requireRecoveryArchive();
                if(retirementHistoryInput!=null&&!sameRetirementEntry(retirementHistoryInput.entry,entry))
                    throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("Root history belongs to another original Source input");
                if(retirementSourceCopy!=null)throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original Root Source copy remains owned");
                if(opening!=null&&!opening.ioExited()||referenceClosure!=null&&!referenceClosure.ioExited()
                        ||closedUsesRead!=null&&!closedUsesRead.ioExited()||retirementPreparation!=null&&!retirementPreparation.ioExited()
                        ||retirementSignature!=null&&!retirementSignature.ioExited()||retirementRelease!=null&&!retirementRelease.ioExited())
                    throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original Root input I/O remains owned");
                final long[] minimum=new long[CapacityDimension.COUNT];
                final long frameWindow=Math.multiplyExact(4L,Math.addExact(entry.mutation().canonicalBodyLength(),(long)entry.mutation().authorIdentityLength()+4096));
                minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,
                        Math.addExact(OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),frameWindow));
                allocation.requireCovers(new CapacityVector(minimum));
                final var prepared=retirementPreparation==null?null:retirementPreparation.completedValue().orElse(null);
                if(prepared!=null&&!prepared.released){
                    final var signed=retirementSignature==null?null:retirementSignature.completedValue().orElse(null);
                    if(prepared.sourceBorrowed||signed==null||!signed.equals(entry.mutation()))
                        throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original prepared Root input belongs to another Source frame");
                    prepared.sourceBorrowed=true;loan=new RetirementSourceCopy(this,entry,prepared,frameWindow);
                    loan.copy=prepared.archived;loan.reading=false;
                }else loan=new RetirementSourceCopy(this,entry,null,frameWindow);
                retirementSourceCopy=loan;
            }
            try{
                final Runnable guard=()->{current.run();loan.requireCurrent();};guard.run();
                if(!loan.reading)return loan;
                final var body=com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.decode(entry.mutation().canonicalBody());
                final long copyWindow=com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.estimatedBytes(body.resource(),
                        body.resource().canonicalValueBytes(),com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.MAX_PUBLIC_INPUT_BYTES,
                        body.context().closureBytes());
                final long[] minimum=new long[CapacityDimension.COUNT];
                minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,Math.addExact(
                        OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),Math.addExact(loan.frameWindow,Math.multiplyExact(4L,copyWindow))));
                allocation.requireCovers(new CapacityVector(minimum));guard.run();
                final var actual=catalog.readRetirementSourceCopy(body,history.trust,reads,guard);
                guard.run();synchronized(this){loan.copy=actual;loan.reading=false;}return loan;
            }catch(RuntimeException|Error failure){synchronized(this){loan.reading=false;loan.releaseLocal();}throw failure;}
        }

        /** Authenticates complete history on the existing Source I/O turn; activation/provider/capacity checks remain independent. */
        public RetirementHistoryInput readRetirementHistory(com.nereusstream.delay.ownership.SourceReplayMutation entry,Runnable current){
            Objects.requireNonNull(entry);Objects.requireNonNull(current);
            com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.Action.of(entry.mutation().type());
            final RetirementHistoryInput loan;final KafkaClientArtifactTargetRetirementHistoryConfiguration.Loaded history;
            synchronized(this){
                requireReady();history=Objects.requireNonNull(retirementHistory,"explicit independent retirement Source history configuration");
                if(retirementHistoryInput!=null||retirementSourceCopy!=null&&!sameRetirementEntry(retirementSourceCopy.entry,entry))
                    throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original Root Source history/copy remains owned");
                final long[] minimum=new long[CapacityDimension.COUNT];
                minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,
                        Math.multiplyExact(4L,Math.addExact(entry.mutation().canonicalBodyLength(),(long)entry.mutation().authorIdentityLength()+4096)));
                allocation.requireCovers(new CapacityVector(minimum));
                loan=new RetirementHistoryInput(this,entry);retirementHistoryInput=loan;
            }
            try{
                final Runnable guard=()->{current.run();loan.requireCurrent();};guard.run();
                final var resource=entry.mutation().type()==com.nereusstream.delay.protocol.SystemMutationType.RESOURCE_RETIRE_INTENT
                        ?com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.decode(entry.mutation().canonicalBody()).resource()
                        :com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.decode(entry.mutation().canonicalBody()).resource();
                final var captured=history.capture(resource,entry,guard);final long bytes=captured.retainedBytes();guard.run();
                synchronized(this){loan.requireCurrent();loan.captured=captured;loan.bytes=bytes;loan.reading=false;}return loan;
            }catch(IOException failure){
                synchronized(this){loan.reading=false;loan.releaseLocal();}
                throw new com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.Unavailable("original retirement history read incomplete",failure);
            }catch(RuntimeException|Error failure){synchronized(this){loan.reading=false;loan.releaseLocal();}throw failure;}
        }

        private static boolean sameRetirementEntry(com.nereusstream.delay.ownership.SourceReplayMutation first,
                com.nereusstream.delay.ownership.SourceReplayEntry second){
            return second instanceof com.nereusstream.delay.ownership.SourceReplayMutation actual&&first.mutation().equals(actual.mutation())
                    &&Arrays.equals(first.position().canonicalBytes(),actual.position().canonicalBytes())
                    &&Objects.equals(first.sourceConnectionGeneration(),actual.sourceConnectionGeneration())
                    &&Arrays.equals(first.guardAttestationDigest(),actual.guardAttestationDigest());
        }

        /** Retains the immutable authenticated public snapshot until the exact Source input has actually closed. */
        public static final class RetirementHistoryInput implements AutoCloseable {
            private final Opening root;
            private final com.nereusstream.delay.ownership.SourceReplayMutation entry;
            private KafkaClientArtifactTargetRetirementHistoryConfiguration.Captured captured;
            private long bytes;
            private boolean reading=true,borrowed,closed;
            private RetirementHistoryInput(Opening root,com.nereusstream.delay.ownership.SourceReplayMutation entry){this.root=root;this.entry=entry;}
            public void requireCurrent(){synchronized(root){
                root.requireReady();if(closed||root.retirementHistoryInput!=this)throw new IllegalStateException("original Source history input closed");
            }}
            private void requireLoaded(){requireCurrent();if(reading||captured==null)throw new IllegalStateException("original history I/O has not exited");}
            /** Grant data still requires actual prior protocol/Artifact activation and action-specific provider checks. */
            public java.util.Optional<com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.Grant> grant(){synchronized(root){requireLoaded();return captured.grant();}}
            public long retainedBytes(){synchronized(root){requireLoaded();return bytes;}}
            public byte[] publicInputs(){synchronized(root){requireLoaded();return captured.publicInputs();}}
            /** Pin comes from the independently configured/verified Root, not from decoded public proof bytes. */
            public com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.SourceHistory sourceHistory(){synchronized(root){
                requireLoaded();return new com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.SourceHistory(captured.proof(),captured.pin(),this::requireCurrent);
            }}
            public com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs hold(
                    com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs original){
                final long retained;
                synchronized(root){
                    requireLoaded();Objects.requireNonNull(original).requireCurrent();
                    if(borrowed||!sameRetirementEntry(entry,original.entry()))throw new IllegalArgumentException("history hold changed original Source frame/proof");
                    final long originalBytes=original.retainedBytes();
                    if(originalBytes<0)throw new IllegalArgumentException("history hold lost original input byte accounting");
                    retained=Math.addExact(bytes,originalBytes);borrowed=true;
                }
                return new com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs(){
                    private boolean released,closing;
                    public com.nereusstream.delay.ownership.SourceReplayEntry entry(){return original.entry();}
                    public com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Inputs inputs(){return original.inputs();}
                    public long retainedBytes(){return retained;}
                    public void requireCurrent(){synchronized(root){
                        if(released||closing)throw new IllegalStateException("original Source history hold closing/closed");
                        RetirementHistoryInput.this.requireCurrent();original.requireCurrent();
                    }}
                    public void close(){
                        synchronized(root){if(released)return;if(closing)throw new IllegalStateException("history close remains owned");closing=true;}
                        try{original.close();synchronized(root){released=true;borrowed=false;releaseLocal();}}
                        finally{synchronized(root){closing=false;}}
                    }
                };
            }
            private void releaseLocal(){
                if(reading||borrowed)throw new IllegalStateException("original history I/O/Source hold remains owned");
                if(closed)return;closed=true;captured=null;if(root.retirementHistoryInput==this)root.retirementHistoryInput=null;
            }
            @Override public void close(){synchronized(root){if(!borrowed)releaseLocal();}}
        }

        /** One Root-owned public-copy loan, also usable before Worker admission during cold Source replay. */
        public static final class RetirementSourceCopy implements AutoCloseable {
            private final Opening root;
            private final com.nereusstream.delay.ownership.SourceReplayMutation entry;
            private final RetirementPreparation prepared;
            private final long frameWindow;
            private OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy copy;
            private boolean reading=true,borrowed,closed;
            private RetirementSourceCopy(Opening root,com.nereusstream.delay.ownership.SourceReplayMutation entry,
                    RetirementPreparation prepared,long frameWindow){this.root=root;this.entry=entry;this.prepared=prepared;this.frameWindow=frameWindow;}
            public void requireCurrent(){synchronized(root){
                root.requireReady();if(closed||root.retirementSourceCopy!=this)throw new IllegalStateException("original Source copy loan closed");
            }}
            public OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy authenticatedCopy(){synchronized(root){
                requireCurrent();if(reading||copy==null)throw new IllegalStateException("original Source copy I/O has not exited");return copy;
            }}
            /** Caller keeps original rights on failure. On success Source close owns this loan; caller close is harmless. */
            public com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs hold(
                    com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs original){
                synchronized(root){
                    requireCurrent();Objects.requireNonNull(original).requireCurrent();
                    if(reading||borrowed||copy==null||!sameRetirementEntry(entry,original.entry())
                            ||!(original.inputs() instanceof com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Retirement input)
                            ||input.approval()==null||input.approval().copy()!=copy)
                        throw new IllegalArgumentException("historical Source hold changed exact entry/actual Catalog copy");
                    borrowed=true;
                }
                return new com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs(){
                    private boolean released,closing;
                    public com.nereusstream.delay.ownership.SourceReplayEntry entry(){return original.entry();}
                    public com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Inputs inputs(){return original.inputs();}
                    public long retainedBytes(){return original.retainedBytes();}
                    public void requireCurrent(){synchronized(root){
                        if(released||closing)throw new IllegalStateException("historical Source hold closing/closed");
                        RetirementSourceCopy.this.requireCurrent();original.requireCurrent();
                    }}
                    public void close(){
                        synchronized(root){if(released)return;if(closing)throw new IllegalStateException("historical Source close remains owned");closing=true;}
                        try{original.close();synchronized(root){released=true;borrowed=false;releaseLocal();}}
                        finally{synchronized(root){closing=false;}}
                    }
                };
            }
            private void releaseLocal(){
                if(reading||borrowed)throw new IllegalStateException("actual Source copy/read remains owned");
                if(closed)return;closed=true;copy=null;
                if(prepared!=null)prepared.sourceBorrowed=false;
                if(root.retirementSourceCopy==this)root.retirementSourceCopy=null;
            }
            @Override public void close(){synchronized(root){if(!borrowed)releaseLocal();}}
        }

        /** Signs one exact candidate on the original owned I/O pool; this neither appends Source nor permits delete. */
        public synchronized java.util.concurrent.CompletableFuture<com.nereusstream.delay.protocol.SystemMutation> signRetirementIntent(
                RetirementPreparation prepared,com.nereusstream.delay.protocol.ResourceRetireIntentBody.ProtectionSet protections,
                long retryUntil){
            prepared.requireRoot(this);Objects.requireNonNull(protections);
            final var signer=Objects.requireNonNull(retirementSigning,"explicit original retirement signing configuration");
            if(protections.references().size()>com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_PROTECTIONS
                    ||protections.canonicalByteLength()>com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.MAX_PROTECTION_BYTES)
                throw new IllegalArgumentException("retirement protections exceed finite signing bound");
            final long[] minimum=new long[CapacityDimension.COUNT];
            minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(prepared.reservedWindowBytes,signer.signingWindowBytes());
            allocation.requireCovers(new CapacityVector(minimum));
            final byte[] selection=Bytes.sha256(prepared.context.canonicalBytes(),protections.digest(),
                    Bytes.u64beBits(retryUntil));
            final var previous=retirementSignature;
            if(previous!=null){
                if(!Arrays.equals(selection,retirementSignatureSelection))throw new IllegalStateException("original signed retirement candidate remains owned");
                previous.retryCleanup();
                if(!previous.ioExited()||previous.completedValue().isPresent())return previous.receipt();
                retirementSignature=null;retirementSignatureSelection=null;
            }
            discardFailedRetirementRelease();
            if(retirementRelease!=null)throw new IllegalStateException("original retirement input release is pending");
            final var source=Objects.requireNonNull(retirementSource);
            final var next=OwnedCheckpointIo.submit(guard->{
                final Runnable current=()->{guard.run();prepared.requireRoot(this);};
                final var time=source.retirementSigningTime(this,current);
                final var signed=signer.sign(prepared.uses,prepared.archived,protections,retryUntil,time,current);
                source.requireRetirementReadCurrent(this,current);return signed;
            },this::requireReady,io);
            retirementSignatureSelection=selection;retirementSignature=next;return next.receipt();
        }

        /** Uses the same Worker's actual registered Source writer and shared WorkClass queue; no local apply shortcut. */
        public com.nereusstream.delay.ownership.TargetEvidenceRetirementWorkClassExecutor.Submission submitRetirementIntent(
                RetirementPreparation prepared,com.nereusstream.delay.ownership.OwnedSourceSystemWriter writer){
            final com.nereusstream.delay.protocol.SystemMutation signed;
            final KafkaClientArtifactTargetWorkerSourceFactory.Opening source;
            synchronized(this){
                prepared.requireRoot(this);discardFailedRetirementRelease();
                if(retirementSubmitting||retirementRelease!=null||retirementSignature==null||!retirementSignature.ioExited())
                    throw new IllegalStateException("original retirement signing/submission/release remains owned");
                signed=retirementSignature.completedValue().orElseThrow();source=Objects.requireNonNull(retirementSource);
                Objects.requireNonNull(retirementSigning).requireSigned(signed);retirementSubmitting=true;
            }
            try{
                final var submitted=source.submitRetirementIntent(this,signed,writer,()->{
                    prepared.requireRoot(this);Objects.requireNonNull(retirementSigning).requireSigned(signed);
                },retained->{synchronized(this){retirementSubmission=retained;if(stopping)retained.requestStop();}});
                return submitted;
            }finally{synchronized(this){retirementSubmitting=false;}}
        }

        /** One same-process Source look-ahead may borrow this exact candidate. Caller keeps its original hold on failure. */
        public synchronized com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs holdRetirementSourceInputs(
                RetirementPreparation prepared,com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs original){
            prepared.requireRoot(this);Objects.requireNonNull(original).requireCurrent();
            discardFailedRetirementRelease();
            if(prepared.sourceBorrowed||retirementRelease!=null||retirementSignature==null||!retirementSignature.ioExited())
                throw new IllegalStateException("original retirement Source input is already held or not signed");
            final var signed=retirementSignature.completedValue().orElseThrow();
            if(!(original.entry() instanceof com.nereusstream.delay.ownership.SourceReplayMutation entry)
                    ||!Arrays.equals(entry.mutation().canonicalEnvelope(),signed.canonicalEnvelope())
                    ||!(original.inputs() instanceof com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Retirement input)
                    ||input.approval()==null||input.approval().copy()!=prepared.archived)
                throw new IllegalArgumentException("Source hold changed original signed frame or actual Catalog receipt");
            Objects.requireNonNull(retirementSigning).requireSigned(signed);
            prepared.sourceBorrowed=true;
            return new com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs(){
                private boolean closed,closing;
                public com.nereusstream.delay.ownership.SourceReplayEntry entry(){return original.entry();}
                public com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Inputs inputs(){return original.inputs();}
                public long retainedBytes(){return original.retainedBytes();}
                public void requireCurrent(){synchronized(Opening.this){
                    if(closed||closing)throw new IllegalStateException("original retirement Source hold closing/closed");
                    prepared.requireRoot(Opening.this);original.requireCurrent();
                }}
                public void close(){
                    synchronized(Opening.this){if(closed)return;if(closing)throw new IllegalStateException("original Source input close remains owned");closing=true;}
                    try{original.close();synchronized(Opening.this){closed=true;prepared.sourceBorrowed=false;}}
                    finally{synchronized(Opening.this){closing=false;}}
                }
            };
        }

        /** Signed/UNKNOWN candidates stay retained until an actual Native first is known; caller DTOs cannot release them. */
        public synchronized java.util.concurrent.CompletableFuture<Boolean> releaseRetirementPreparation(RetirementPreparation prepared){
            prepared.requireRoot(this);
            if(retirementSubmitting)throw new IllegalStateException("original retirement Source submission remains owned");
            if(retirementSignature!=null&&!retirementSignature.ioExited())throw new IllegalStateException("original retirement signing I/O remains owned");
            if(prepared.sourceBorrowed)throw new IllegalStateException("actual Source still holds retirement input");
            if(retirementRelease!=null){
                retirementRelease.retryCleanup();
                if(!retirementRelease.ioExited()||retirementRelease.completedValue().isPresent())return retirementRelease.receipt();
                retirementRelease=null;
            }
            final var signed=retirementSignature==null?null:retirementSignature.completedValue().orElse(null);
            final var source=Objects.requireNonNull(retirementSource);
            final var submission=retirementSubmission;
            final var next=OwnedCheckpointIo.submit(guard->{
                final Runnable current=()->{guard.run();prepared.requireRoot(this);};
                if(signed!=null&&source.retirementMutationResult(this,signed,current).isEmpty())
                    throw new IllegalStateException("original signed retirement Source outcome is still unknown");
                if(submission!=null){
                    source.settleRetirementIntent(this,signed,current);
                    if(!submission.closeForRelease())throw new IllegalStateException("original retirement append action has not exited");
                }
                current.run();synchronized(this){prepared.releaseLocal();}return true;
            },this::requireReady,io);
            retirementRelease=next;return next.receipt();
        }
        private void discardFailedRetirementRelease(){
            final var release=retirementRelease;
            if(release!=null){release.retryCleanup();if(release.ioExited()&&release.completedValue().isEmpty())retirementRelease=null;}
        }

        private KafkaClientArtifactTargetRecoveryArchiveConfiguration.Loaded requireRecoveryArchive() {
            requireReady();
            return Objects.requireNonNull(recoveryArchive, "explicit original Root recovery signing configuration");
        }

        /** Only the history callback is exposed; purpose and Source private keys remain inside this Root owner. */
        public TargetPublishRecoveryMaintenance.History receiptRecoveryHistory(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source, KafkaClientArtifactTargetRecoveryRuntime recovery,
                int maximumTurns, KafkaClientArtifactTargetRecoveryRuntime.ResolutionContext contexts,
                TargetPublishRecoveryMaintenance.History originalHistory) {
            final var original = requireRecoveryArchive();
            return Objects.requireNonNull(source).receiptRecoveryHistory(recovery, this, maximumTurns,
                    original.issuer, original.trust, original.sourceSigner, original.contexts(contexts), originalHistory);
        }

        /** Bootstrap authenticates retained original proofs/seals; it neither claims adoption nor requires a live old SDK. */
        public com.nereusstream.delay.store.TargetCheckpointDependencies.RecoveryEvidenceAuthority receiptRecoveryEvidenceAuthority(
                Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads) {
            final var original = requireRecoveryArchive(); Objects.requireNonNull(reads);
            return (owner, input) -> {
                requireReady(); final var budget = Objects.requireNonNull(reads.get(), "bounded original recovery proof reads");
                if (!budget.beforeRead() || !budget.tryCharge(Bytes.utf8(owner.reference().exactKey()).length,
                        Math.toIntExact(owner.reference().canonicalValueBytes()))) throw budget.incomplete();
                final var retained = catalog.retainedRecovery(owner, input.image(), original.trust);
                requireReady(); return retained;
            };
        }

        /** After actual Worker admission, the existing Source provider may reconcile its proven APPLIED owners. */
        public com.nereusstream.delay.store.TargetCheckpointDependencies.RecoveryEvidenceAuthority reconcilingReceiptRecoveryEvidenceAuthority(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,
                Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads) {
            final var original = requireRecoveryArchive();
            return Objects.requireNonNull(source).reconcilingReceiptRecoveryEvidenceAuthority(this, original.trust, reads);
        }

        @Override public void requestStop() {
            stopping = true;
            final var signing = recoveryArchive; if (signing != null) signing.requestStop();
            final var retireSigning=retirementSigning;if(retireSigning!=null)retireSigning.requestStop();
            final var history=retirementHistory;if(history!=null)history.requestStop();
            final var operation = opening; if (operation != null) operation.requestStop();
            final var closure = referenceClosure; if (closure != null) closure.requestStop();
            final var uses = closedUsesRead; if (uses != null) uses.requestStop();
            final var retirement = retirementPreparation; if (retirement != null) retirement.requestStop();
            final var signature=retirementSignature;if(signature!=null)signature.requestStop();
            final var release=retirementRelease;if(release!=null)release.requestStop();
            final var submission=retirementSubmission;if(submission!=null)submission.requestStop();
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
            final var closure=referenceClosure;
            if(closure!=null){closure.requestStop();closure.retryCleanup();if(!closure.ioExited())return false;}
            final var uses=closedUsesRead;
            if(uses!=null){uses.requestStop();uses.retryCleanup();if(!uses.ioExited())return false;}
            final var retirement=retirementPreparation;
            if(retirement!=null){retirement.requestStop();retirement.retryCleanup();if(!retirement.ioExited())return false;}
            final var signature=retirementSignature;
            if(signature!=null){signature.requestStop();signature.retryCleanup();if(!signature.ioExited())return false;}
            final var release=retirementRelease;
            if(release!=null){release.requestStop();release.retryCleanup();if(!release.ioExited())return false;}
            if(retirementSubmitting)return false;
            if(retirementHistoryInput!=null)return false;
            if(retirementSourceCopy!=null)return false;
            final var submission=retirementSubmission;
            if(submission!=null){submission.requestStop();if(!submission.ioExited())return false;}
            final var preparedRetirement=retirement==null?null:retirement.completedValue().orElse(null);
            if(preparedRetirement!=null){if(preparedRetirement.sourceBorrowed)return false;preparedRetirement.releaseLocal();}
            final var actual = manager;
            if (actual != null && !closeNative(actual, permit)) return false;
            final var signing = recoveryArchive;
            if (signing != null) signing.close();
            final var retireSigning=retirementSigning;if(retireSigning!=null)retireSigning.close();
            recoveryArchive = null; recoveryInputs = null;
            retirementSigning=null;retirementSigningInputs=null;
            retirementHistory=null;retirementHistoryInputs=null;
            referenceClosure=null;referenceClosureKey=null;referenceClosureOwner=null;
            closedUsesRead=null;closedUsesSource=null;closedUsesSelection=null;
            retirementPreparation=null;retirementSource=null;retirementSelection=null;
            retirementSignature=null;retirementSignatureSelection=null;retirementRelease=null;
            retirementSubmission=null;
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
