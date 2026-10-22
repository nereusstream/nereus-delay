package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.OwnedCheckpointIo;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetCatalogRootAuthority;
import com.nereusstream.delay.ownership.TargetEvidenceObjectLifecycleAuthority;
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
    /** Already-held complete images only; acquisition/transport remains with their original bounded owner. */
    public record ColdConfirmationInputs(long maximumRecoveryInputBytes,
            Supplier<List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage>> images,
            Supplier<com.nereusstream.delay.store.BoundedReadBudget> nativeReads,Supplier<com.nereusstream.delay.store.BoundedReadBudget> recoveryReads){
        public ColdConfirmationInputs{
            if(maximumRecoveryInputBytes<=0||maximumRecoveryInputBytes==Long.MAX_VALUE)throw new IllegalArgumentException("finite cold recovery input window required");
            Objects.requireNonNull(images);Objects.requireNonNull(nativeReads);Objects.requireNonNull(recoveryReads);
        }
    }

    /** Independent trusted inputs; no initializer is inferred from ordinary management or absent storage. */
    public record CatalogRootInputs(TargetCatalogRootAuthority history, CatalogInitialization initialization,
            TargetEvidenceObjectLifecycleAuthority.Inputs evidenceLifecycle) {
        public CatalogRootInputs { Objects.requireNonNull(history); }
        public CatalogRootInputs(TargetCatalogRootAuthority history,CatalogInitialization initialization){this(history,initialization,null);}
        public long maximumRetainedBytes(){return Math.addExact(TargetCatalogRootAuthority.maximumInputBytes(),
                evidenceLifecycle==null?0:evidenceLifecycle.maximumRetainedBytes());}
    }
    public record CatalogInitialization(TargetCatalogRootAuthority.Policy policy,
            TargetReceiptCurrentManagement.Authenticator authenticator, TargetCatalogRootAuthority.FreshAllocation externalAllocation,
            Supplier<com.nereusstream.delay.store.BoundedReadBudget> reads) {
        public CatalogInitialization {
            Objects.requireNonNull(policy).canonicalBytes();Objects.requireNonNull(authenticator);Objects.requireNonNull(reads);
            if((policy.purpose()==TargetCatalogRootAuthority.Purpose.INITIALIZE_FRESH_CATALOG_ROOT)!=(externalAllocation!=null))
                throw new IllegalArgumentException("fresh initialization requires its actual external allocation authority; adoption does not substitute one");
        }
    }

    /** Authentication still comes from original registered CONTROL history and the live operator resolver. */
    public record TrustInputs(TargetChannelControlVerifier.KeyAuthority keys,
            TargetKafkaReceiptProtectionTrust.HistoricalActor actors,
            TargetKafkaReceiptProtectionTrust.ReceiptScope scope, TargetReceiptCurrentManagement current,
            TargetKafkaReceiptProtectionTrust.PublicHistory publicHistory, CatalogRootInputs catalogRoot) {
        public TrustInputs { Objects.requireNonNull(keys); Objects.requireNonNull(actors); Objects.requireNonNull(scope); }
        public TrustInputs(TargetChannelControlVerifier.KeyAuthority keys,
                TargetKafkaReceiptProtectionTrust.HistoricalActor actors,TargetKafkaReceiptProtectionTrust.ReceiptScope scope,
                TargetReceiptCurrentManagement current,TargetKafkaReceiptProtectionTrust.PublicHistory publicHistory){
            this(keys,actors,scope,current,publicHistory,null);
        }
        public TrustInputs(TargetChannelControlVerifier.KeyAuthority keys,
                TargetKafkaReceiptProtectionTrust.HistoricalActor actors, TargetKafkaReceiptProtectionTrust.ReceiptScope scope,
                TargetReceiptCurrentManagement current) {
            this(keys, actors, scope, current, null);
        }
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
        return openConfigured(root,configuration,controlHistory,currentPolicy,recoveryArchive,retirementSigning,retirementHistory,
                originalRecoveryMaterialHistory,budget,authenticator,null);
    }
    public static Opening openConfigured(TargetWorkerProcessRuntime root, Path configuration, Path controlHistory,
            Path currentPolicy, Path recoveryArchive, Path retirementSigning,Path retirementHistory,
            List<TargetRecoveryCredentialAuthorization.Trust> originalRecoveryMaterialHistory,
            TargetDestinationSdkBudget budget, TargetReceiptCurrentManagement.Authenticator authenticator,CatalogRootInputs catalogRoot) throws IOException {
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
        final long bytes = Math.addExact(catalogRoot==null?0:catalogRoot.maximumRetainedBytes(),Math.addExact(Math.addExact(captured.maximumRetainedBytes(), policy.maximumRetainedBytes()),
                Math.addExact(retainedConfigurationBytes(config.management(), config.admin(), config.receipts()),
                        Math.addExact(recovery == null ? 0 : recovery.maximumRetainedBytes(),
                                Math.addExact(retirement == null ? 0 : retirement.maximumRetainedBytes(),historyInputs==null?0:historyInputs.maximumRetainedBytes())))));
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
                return new TrustInputs(history, history, history, current, history::historicalPublicInputs,catalogRoot);
            } catch (IOException failure) { throw new IllegalStateException("original CONTROL input materialization failed", failure); }
        }, recovery, retirement, historyInputs,bytes);
    }

    /** No native SDK or request is created until this exact owner and cost have been retained by the original root. */
    public static Opening openConfigured(TargetWorkerProcessRuntime root, Path path,
            TargetDestinationSdkBudget budget, TrustInputs inputs) throws IOException {
        Objects.requireNonNull(inputs, "original trusted CONTROL providers");
        Objects.requireNonNull(inputs.current(), "actual current whole-topic scope authority");
        final var config=load(path);
        return openLoaded(root,config,budget,guard->inputs,null,Math.addExact(retainedConfigurationBytes(config.management(),config.admin(),config.receipts()),
                inputs.catalogRoot()==null?0:inputs.catalogRoot().maximumRetainedBytes()));
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
        private SharedRocksDbResources originalResources;
        private KafkaClientArtifactTargetRecoveryArchiveConfiguration.Inputs recoveryInputs;
        private volatile KafkaClientArtifactTargetRecoveryArchiveConfiguration.Loaded recoveryArchive;
        private KafkaClientArtifactTargetEvidenceRetirementConfiguration.Inputs retirementSigningInputs;
        private volatile KafkaClientArtifactTargetEvidenceRetirementConfiguration.Loaded retirementSigning;
        private KafkaClientArtifactTargetRetirementHistoryConfiguration.Inputs retirementHistoryInputs;
        private volatile KafkaClientArtifactTargetRetirementHistoryConfiguration.Loaded retirementHistory;
        private TargetEvidenceObjectLifecycleAuthority.Verified evidenceLifecycle;
        private TargetReceiptCurrentManagement.Authenticator evidenceLifecycleAuthenticator;
        private TargetEvidenceObjectLifecycleAuthority.AdmissionProtection evidenceAdmission;
        private boolean evidenceAdmissionAcquiring;
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
        private volatile OwnedCheckpointIo<MarkerPreparation> markerReservation;
        private MarkerReservationRequest markerRequest;
        private volatile OwnedCheckpointIo<PayloadPreparation> payloadRetirement;
        private OxiaSyncTargetKafkaReceiptProtectionCatalog.PayloadRetirementProtection payloadProtection;
        private volatile OwnedCheckpointIo<com.nereusstream.delay.protocol.SystemMutation> confirmationSignature;
        private byte[] confirmationSignatureSelection;
        private ConfirmationSourceInputs confirmationSourceInputs;
        private volatile OwnedCheckpointIo<Boolean> physicalCompletion;
        private KafkaClientArtifactTargetWorkerSourceFactory.Opening physicalCompletionSource;
        private byte[] physicalCompletionSelection;
        private com.nereusstream.delay.ownership.TargetWorkerOxiaProviders registryInputOwner;
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
            originalResources=graph.resources();
            allocation = budget.reserve("receipt-management/" + Bytes.hex(identity), config.cost());
            final var originalRegistry=graph.oxia().providers();
            if(originalRegistry.hasManagedRegistryInputOwner())registryInputOwner=originalRegistry;
            opening = OwnedCheckpointIo.submit(guard -> {
                try {
                    guard.run(); TargetSdkArtifactIdentity.requireExact(KafkaAdminClient.class, config.sdkJar());
                    final var inputs = Objects.requireNonNull(trustLoader.apply(guard), "actual original CONTROL inputs");
                    final var rootInputs=Objects.requireNonNull(inputs.catalogRoot(),"explicit Catalog root history/purpose inputs are unavailable");
                    final long[] rootWindow=new long[CapacityDimension.COUNT];rootWindow[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,
                            OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes());
                    allocation.requireCovers(new CapacityVector(rootWindow));
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
                                    Objects.requireNonNull(manager, "actual receipt Admin backing").requireCurrent(image, control, at), inputs.current(), inputs.publicHistory());
                    final var original = config.management();
                    final var image = TargetKafkaReceiptManagementImage.decode(original.image());
                    guard.run();nativeStarted=true;allocation.beginConstruction();
                    for (var receipt : config.receipts()) {
                        guard.run(); final var authority = receipt.authority();
                        try (var permission = trust.currentManagement(authority, Action.OBSERVE)) { permission.requireCurrent(); }
                        trust.requireIssuerSignature(TargetKafkaReceiptControlArtifact.signManagement(image,
                                authority.control(), authority.authorizedAt(), config.key()));
                    }
                    catalog = graph.oxia().providers().receiptProtectionCatalog(config.catalogPrefix(), trust, config.storage(),rootInputs.history());
                    final var initialize=rootInputs.initialization();
                    if(initialize!=null)catalog.initializeRoot(initialize.policy(),image.deploymentRootDigest(),initialize.authenticator(),
                            initialize.externalAllocation(),Objects.requireNonNull(initialize.reads().get(),"original finite Catalog initialization read budget"),guard);
                    guard.run();catalog.requireReadyRoot(image.deploymentRootDigest());guard.run();
                    final var lifecycle=rootInputs.evidenceLifecycle();
                    if(lifecycle!=null){
                        final var actualScope=catalog.evidenceLifecycleScope(image.deploymentRootDigest(),
                                Objects.requireNonNull(lifecycle.reads().get(),"finite actual lifecycle scope reads"),guard);
                        try{evidenceLifecycle=lifecycle.load(actualScope,guard);}
                        catch(IOException rejected){throw new IllegalStateException("original complete lifecycle history materialization failed",rejected);}
                        evidenceLifecycleAuthenticator=lifecycle.authenticator();
                    }
                    if(retirementHistoryInputs!=null){
                        try{retirementHistory=KafkaClientArtifactTargetRetirementHistoryConfiguration.load(retirementHistoryInputs,catalog.namespaceIdentity(image),guard);}
                        catch(IOException rejected){throw new IllegalStateException("original retirement authorization history materialization failed",rejected);}
                        if(stopping)retirementHistory.requestStop();
                    }
                    guard.run();
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
        @Override public synchronized void requireOriginalEvidenceCatalog(OxiaSyncTargetKafkaReceiptProtectionCatalog expected){
            requireReady();if(catalog!=expected)throw new IllegalArgumentException("physical Catalog differs from its original registered Root");
        }
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

        /** Only an actual Native APPLIED recovery owner can enter this closure path. */
        public synchronized java.util.concurrent.CompletableFuture<OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject> closeAdoptedRecoveryReferences(
                com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption applied,
                com.nereusstream.delay.store.BoundedReadBudget reads) {
            final var original=requireRecoveryArchive();
            final var owner=Objects.requireNonNull(applied,"actual Native recovery adoption").owner();
            return closeAdoptedReferences(owner.reference().exactKey(),Bytes.sha256(owner.canonicalBytes()),reads,
                    current->catalog.closeAdoptedRecoveryReferences(applied,original.trust,reads,current));
        }

        /** Freezes new references only; it does not decide shared dependency release or authorize deletion. */
        public synchronized java.util.concurrent.CompletableFuture<OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject> closeAdoptedControlReferences(
                com.nereusstream.delay.runtime.TargetExternalEvidenceStore.Adoption applied,int kind,
                com.nereusstream.delay.store.BoundedReadBudget reads){
            throw new IllegalStateException("CONTROL closure requires the original admitted Source and complete bounded Native preflight");
        }

        public synchronized java.util.concurrent.CompletableFuture<OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject> closeAdoptedControlReferences(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,
                com.nereusstream.delay.runtime.TargetExternalEvidenceStore.Adoption applied,int kind,
                com.nereusstream.delay.store.BoundedReadBudget reads,com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            requireReady();Objects.requireNonNull(applied,"actual Native CONTROL object adoption");
            Objects.requireNonNull(source);Objects.requireNonNull(reads);Objects.requireNonNull(nativeReads);
            if(reads.maxBytes()==Long.MAX_VALUE||nativeReads.maxBytes()==Long.MAX_VALUE)
                throw new IllegalArgumentException("CONTROL closure requires explicit finite Catalog and Native read bounds");
            final var owner=kind==TargetKafkaReceiptControlArtifact.PROTECTION?applied.protection()
                    :kind==TargetKafkaReceiptControlArtifact.ANCHOR?applied.anchor():null;
            if(owner==null)throw new IllegalArgumentException("CONTROL closure requires protection or anchor");
            return closeAdoptedControlReferences(source,owner,reads,nativeReads);
        }
        /** A selected immutable owner is reauthenticated from actual Native facts; its sibling may already be retired. */
        public synchronized java.util.concurrent.CompletableFuture<OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject> closeAdoptedControlReferences(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,
                com.nereusstream.delay.protocol.TargetExternalEvidenceOwner owner,
                com.nereusstream.delay.store.BoundedReadBudget reads,com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            requireReady();Objects.requireNonNull(source);Objects.requireNonNull(owner);Objects.requireNonNull(reads);Objects.requireNonNull(nativeReads);
            if(reads.maxBytes()==Long.MAX_VALUE||nativeReads.maxBytes()==Long.MAX_VALUE)
                throw new IllegalArgumentException("CONTROL closure requires explicit finite Catalog and Native read bounds");
            final long[] minimum=new long[CapacityDimension.COUNT];
            minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,
                    Math.addExact(OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),nativeReads.maxBytes()));
            allocation.requireCovers(new CapacityVector(minimum));
            return closeAdoptedReferences(owner.exactKey(),Bytes.sha256(owner.canonicalBytes()),reads,
                    current->{
                        final Runnable held=()->{current.run();source.requireRetirementReadCurrent(this,current);};
                        final var snapshot=catalog.captureControlReferences(owner,reads,held);
                        final var preflight=source.readControlReferenceClosure(this,snapshot,nativeReads,held);
                        held.run();return catalog.closeAdoptedControlReferences(preflight,reads,held);
                    });
        }

        private java.util.concurrent.CompletableFuture<OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject> closeAdoptedReferences(
                String key,byte[] image,com.nereusstream.delay.store.BoundedReadBudget reads,
                java.util.function.Function<Runnable,OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject> close){
            Objects.requireNonNull(reads);
            if(closedUsesRead!=null&&!closedUsesRead.ioExited())throw new IllegalStateException("original closed-use read I/O remains owned");
            requireRetirementIoExited();
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
                final var result=close.apply(current);current.run();return result;
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
            requireMarkerWindowFree();
            if(evidenceAdmissionAcquiring||evidenceAdmission!=null)throw new IllegalStateException("original actual deployment/admission protection remains owned");
            if(retirementHistoryInput!=null)throw new IllegalStateException("original Source retirement history remains owned");
            if(retirementSourceCopy!=null)throw new IllegalStateException("original Source retirement input remains owned");
            if(retirementPreparation!=null&&!retirementPreparation.ioExited())
                throw new IllegalStateException("original retirement preparation I/O remains owned");
            if(retirementPreparation!=null&&retirementPreparation.completedValue().filter(value->!value.released).isPresent())
                throw new IllegalStateException("original prepared retirement input remains owned");
            requireRetirementAuxiliaryIoExited();
        }
        private void requireMarkerWindowFree(){
            clearCompletedPhysicalCompletion();
            if(physicalCompletion!=null)throw new IllegalStateException("original physical settlement I/O/window remains owned");
            if(payloadRetirement!=null)throw new IllegalStateException("original physical retirement I/O/window remains owned");
            if(markerReservation!=null)throw new IllegalStateException("original marker reservation I/O/window remains owned");
        }
        private void requireRetirementAuxiliaryIoExited(){
            if(retirementSignature!=null&&!retirementSignature.ioExited()||retirementRelease!=null&&!retirementRelease.ioExited())
                throw new IllegalStateException("original retirement signing/release I/O remains owned");
        }
        /** Called only by the original finite physical task; retain before validation, including failure/stop. */
        private TargetEvidenceObjectLifecycleAuthority.AdmissionProtection acquireEvidenceAdmission(){
            requireReady();final TargetEvidenceObjectLifecycleAuthority.Verified history;
            final TargetReceiptCurrentManagement.Authenticator authenticator;
            synchronized(this){
                if(evidenceAdmissionAcquiring||evidenceAdmission!=null)throw new IllegalStateException("original lifecycle admission hold is already owned");
                history=Objects.requireNonNull(evidenceLifecycle,"independent complete lifecycle history is unavailable");
                authenticator=Objects.requireNonNull(evidenceLifecycleAuthenticator,"actual original admission authenticator is unavailable");
                evidenceAdmissionAcquiring=true;
            }
            final TargetEvidenceObjectLifecycleAuthority.AdmissionProtection acquired;
            try{
                acquired=history.acquire(authenticator);
                synchronized(this){evidenceAdmission=acquired;evidenceAdmissionAcquiring=false;}
            }catch(RuntimeException|Error rejected){
                synchronized(this){evidenceAdmissionAcquiring=false;}
                failed(rejected);throw rejected;
            }
            try{acquired.requireCurrent();requireReady();return acquired;}
            catch(RuntimeException|Error rejected){failed(rejected);throw rejected;}
        }
        private static final class MarkerReservationRequest {
            private final KafkaClientArtifactTargetWorkerSourceFactory.Opening source;
            private final byte[] selection;
            private com.nereusstream.delay.protocol.SystemMutation selected;
            private com.nereusstream.delay.store.BoundedReadBudget catalogReads,nativeReads;
            // Set by the actual owned task before entering any Catalog SDK operation; never caller-supplied.
            private boolean catalogEntered;
            private MarkerReservationRequest(KafkaClientArtifactTargetWorkerSourceFactory.Opening source,byte[] selection,
                    com.nereusstream.delay.protocol.SystemMutation selected,com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                    com.nereusstream.delay.store.BoundedReadBudget nativeReads){
                this.source=source;this.selection=selection;this.selected=selected;this.catalogReads=catalogReads;this.nativeReads=nativeReads;
            }
            private void clearInputs(){selected=null;catalogReads=null;nativeReads=null;}
        }
        /** Owned pending metadata only; this never authenticates a physical payload operation or provider observation. */
        public static final class MarkerPreparation {
            private final Opening root;
            private OxiaSyncTargetKafkaReceiptProtectionCatalog.ReservedMarker reserved;
            private boolean released;
            private MarkerPreparation(Opening root,OxiaSyncTargetKafkaReceiptProtectionCatalog.ReservedMarker reserved){
                this.root=root;this.reserved=reserved;reserved.requireCatalog(root.catalog);
            }
            public com.nereusstream.delay.protocol.TargetEvidenceRetiredMarker marker(){synchronized(root){requireRoot(root);return reserved.marker();}}
            public void requireRoot(Opening expected){synchronized(root){
                if(root!=expected)throw new IllegalArgumentException("marker preparation belongs to another original Root");
                root.requireReady();
                if(released||root.markerReservation==null||!root.markerReservation.ioExited()
                        ||root.markerReservation.completedValue().orElse(null)!=this)
                    throw new IllegalStateException("original marker preparation is unavailable or released");
            }}
            private void releaseLocal(){released=true;reserved=null;}
        }
        public synchronized java.util.concurrent.CompletableFuture<MarkerPreparation> reserveRetiredMarker(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,com.nereusstream.delay.protocol.SystemMutation selected,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            requireReady();Objects.requireNonNull(source);Objects.requireNonNull(selected);Objects.requireNonNull(catalogReads);Objects.requireNonNull(nativeReads);
            catalog.requireManagedMarkerInputOwner();
            if(selected.type()!=com.nereusstream.delay.protocol.SystemMutationType.RESOURCE_RETIRE_INTENT
                    ||catalogReads.maxBytes()==Long.MAX_VALUE||nativeReads.maxBytes()==Long.MAX_VALUE)
                throw new IllegalArgumentException("marker reservation requires original intent and finite Catalog/Native bounds");
            final long frameWindow=Math.multiplyExact(4L,Math.addExact(selected.canonicalBodyLength(),(long)selected.authorIdentityLength()+4096));
            final long window=Math.addExact(retainedInputBytes,Math.addExact(OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),
                    Math.addExact(frameWindow,Math.multiplyExact(4L,nativeReads.maxBytes()))));
            final long[] minimum=new long[CapacityDimension.COUNT];minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=window;
            allocation.requireCovers(new CapacityVector(minimum));
            final byte[] selection=Bytes.sha256(selected.canonicalEnvelope());
            if(markerReservation!=null){
                if(markerRequest.source!=source||!Arrays.equals(markerRequest.selection,selection))
                    throw new IllegalStateException("original marker reservation selection remains owned");
                markerReservation.retryCleanup();return markerReservation.receipt();
            }
            requireRetirementIoExited();
            if(referenceClosure!=null&&!referenceClosure.ioExited()||closedUsesRead!=null&&!closedUsesRead.ioExited())
                throw new IllegalStateException("original closure/closed-use I/O remains owned");
            final var request=new MarkerReservationRequest(source,selection,selected,catalogReads,nativeReads);
            final var next=OwnedCheckpointIo.submit(guard->{
                final Runnable rootCurrent=()->{guard.run();requireReady();};
                final Runnable current=()->{rootCurrent.run();request.source.requireRetirementReadCurrent(this,rootCurrent);};
                try{
                    current.run();final var actual=request.source.readAppliedRetirementIntent(this,request.selected,request.nativeReads,current);
                    current.run();synchronized(this){request.catalogEntered=true;}
                    final var reserved=catalog.reserveRetiredMarker(actual,request.catalogReads,current);
                    current.run();return new MarkerPreparation(this,reserved);
                }catch(RuntimeException|Error rejected){
                    if(request.catalogEntered)failed(rejected);
                    throw rejected;
                }finally{synchronized(this){request.clearInputs();}}
            },this::requireReady,io);
            markerRequest=request;markerReservation=next;return next.receipt();
        }
        /** Successful metadata RPCs have exited; pending server metadata and its physical reservation remain. */
        public synchronized void releaseMarkerPreparation(MarkerPreparation prepared){
            clearCompletedPhysicalCompletion();
            if(physicalCompletion!=null)throw new IllegalStateException("original physical settlement I/O/window remains owned");
            if(payloadRetirement!=null)throw new IllegalStateException("original physical retirement still owns its marker association");
            Objects.requireNonNull(prepared).requireRoot(this);prepared.releaseLocal();markerReservation=null;markerRequest=null;
        }
        /** Actual original SDK operation, owned until Source confirmation/cleanup; never constructed from an evidence DTO. */
        public static final class PayloadPreparation {
            private final Opening root;
            private final KafkaClientArtifactTargetWorkerSourceFactory.Opening source;
            private final byte[] selection;
            private final long reservedWindowBytes;
            private OxiaSyncTargetKafkaReceiptProtectionCatalog.ObservedRetiredMarker observation;
            private boolean released,sourceBorrowed;
            private PayloadPreparation(Opening root,KafkaClientArtifactTargetWorkerSourceFactory.Opening source,byte[] selection,
                    OxiaSyncTargetKafkaReceiptProtectionCatalog.ObservedRetiredMarker observation,long reservedWindowBytes){
                this.root=root;this.source=source;this.selection=selection;this.observation=observation;this.reservedWindowBytes=reservedWindowBytes;
                observation.requireCatalog(root.catalog);
            }
            public void requireRoot(Opening expected){synchronized(root){
                if(root!=expected)throw new IllegalArgumentException("payload observation belongs to another original Root");root.requireReady();
                if(released||root.payloadRetirement==null||!root.payloadRetirement.ioExited()
                        ||root.payloadRetirement.completedValue().orElse(null)!=this)
                    throw new IllegalStateException("original actual payload observation is unavailable");
            }}
            public OxiaSyncTargetKafkaReceiptProtectionCatalog.ObservedRetiredMarker observation(){synchronized(root){requireRoot(root);return observation;}}
            private void releaseLocal(){released=true;observation=null;}
        }
        public synchronized java.util.concurrent.CompletableFuture<PayloadPreparation> retireEvidencePayload(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,MarkerPreparation prepared,
                com.nereusstream.delay.protocol.SystemMutation selected,
                List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,com.nereusstream.delay.store.BoundedReadBudget nativeReads,
                com.nereusstream.delay.store.BoundedReadBudget recoveryReads){
            requireReady();Objects.requireNonNull(source);Objects.requireNonNull(prepared).requireRoot(this);
            OxiaSyncTargetKafkaReceiptProtectionCatalog.requireSupportedProtectionDeclarations(selected);
            final byte[] selection=Bytes.sha256(selected.canonicalEnvelope());
            if(markerRequest.source!=source||!Arrays.equals(selection,markerRequest.selection))
                throw new IllegalArgumentException("physical request differs from original Source marker reservation");
            if(payloadRetirement!=null){payloadRetirement.retryCleanup();return payloadRetirement.receipt();}
            for(var reads:List.of(catalogReads,nativeReads,recoveryReads))if(reads.maxBytes()==Long.MAX_VALUE)
                throw new IllegalArgumentException("finite full physical read bounds required");
            final var retainedImages=List.copyOf(images);
            final long[] minimum=new long[CapacityDimension.COUNT];
            minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,
                    Math.addExact(OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),
                    Math.addExact(com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.recoveryProtectionInputWindowBytes(retainedImages),
                    Math.addExact(Math.multiplyExact(8L,nativeReads.maxBytes()),Math.multiplyExact(8L,
                            Math.addExact(selected.canonicalBodyLength(),(long)selected.authorIdentityLength()+com.nereusstream.delay.protocol.TargetEvidenceRetiredMarker.MAX_BYTES+4096))))));
            allocation.requireCovers(new CapacityVector(minimum));
            final var next=OwnedCheckpointIo.submit(guard->{
                final Runnable current=()->{guard.run();requireReady();source.requireRetirementReadCurrent(this,guard);};
                try{
                    current.run();final var admission=acquireEvidenceAdmission();
                    final var protection=source.preparePayloadRetirementProtection(this,selected,prepared.reserved,admission,retainedImages,
                            catalogReads,nativeReads,recoveryReads,current);
                    synchronized(this){payloadProtection=protection;}
                    final var observation=catalog.retireEvidencePayload(protection,catalogReads,nativeReads,recoveryReads,
                            ()->source.retirementSigningTime(this,protection.applied().record().copy().resource(),current),current);
                    current.run();return new PayloadPreparation(this,source,selection,observation,minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]);
                }catch(RuntimeException|Error rejected){failed(rejected);throw rejected;}
            },this::requireReady,io);
            payloadRetirement=next;return next.receipt();
        }
        /** Only an actual no-Catalog-start failure can discard locally; an entered failure stays until SDK termination. */
        public synchronized boolean discardUnstartedMarkerReservation(){
            requireReady();final var original=markerReservation;if(original==null)return true;
            if(!original.ioExited())return false;
            if(original.completedValue().isPresent())throw new IllegalStateException("successful marker preparation requires its original release handle");
            if(markerRequest.catalogEntered)return false;
            markerRequest.clearInputs();markerReservation=null;markerRequest=null;return true;
        }
        /** Actual prepared public bytes and Native cut. Source signing/apply must still revalidate its own authority/state. */
        public static final class RetirementPreparation {
            private final Opening root;
            private com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.ClosedUses uses;
            private com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ClosedUses controlUses;
            private com.nereusstream.delay.runtime.TargetStagingEvidenceStore.ClosedUses stagingUses;
            private OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy archived;
            private com.nereusstream.delay.protocol.TargetEvidenceRetirementContext context;
            private final long reservedWindowBytes;
            private boolean released,sourceBorrowed;
            private RetirementPreparation(Opening root,com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.ClosedUses uses,
                    OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy archived,long reservedWindowBytes){
                this(root,archived,uses.resource(),uses.retirementContext(archived.image().digest(),archived.image().canonicalBytes().length),reservedWindowBytes);
                this.uses=uses;
            }
            private RetirementPreparation(Opening root,com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ClosedUses uses,
                    OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy archived,long reservedWindowBytes){
                this(root,archived,uses.resource(),uses.retirementContext(archived.image().digest(),archived.image().canonicalBytes().length),reservedWindowBytes);
                this.controlUses=uses;
            }
            private RetirementPreparation(Opening root,com.nereusstream.delay.runtime.TargetStagingEvidenceStore.ClosedUses uses,
                    OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy archived,long reservedWindowBytes){
                this(root,archived,archived.image().resource(),uses.retirementContext(archived.image()),reservedWindowBytes);
                this.stagingUses=uses;
            }
            private RetirementPreparation(Opening root,OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy archived,
                    com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource resource,
                    com.nereusstream.delay.protocol.TargetEvidenceRetirementContext context,long reservedWindowBytes){
                this.root=root;this.archived=archived;this.context=context;this.reservedWindowBytes=reservedWindowBytes;archived.requireCatalog(root.catalog);
                final var copy=archived.image();
                if(!Arrays.equals(copy.resource().canonicalBytes(),resource.canonicalBytes()))
                    throw new IllegalStateException("prepared copy changed actual Native owner/resource");
                copy.requireContext(context);
            }
            public com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.ClosedUses uses(){synchronized(root){requireRoot(root);return Objects.requireNonNull(uses,"prepared object is not Recovery");}}
            public com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ClosedUses controlUses(){synchronized(root){requireRoot(root);return Objects.requireNonNull(controlUses,"prepared object is not CONTROL");}}
            public com.nereusstream.delay.runtime.TargetStagingEvidenceStore.ClosedUses stagingUses(){synchronized(root){requireRoot(root);return Objects.requireNonNull(stagingUses,"prepared object is not staging");}}
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
                released=true;uses=null;controlUses=null;stagingUses=null;archived=null;context=null;
            }
        }
        public synchronized java.util.concurrent.CompletableFuture<RetirementPreparation> prepareRecoveryRetirement(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,
                OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption applied,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            requireRecoveryArchive();
            return prepareRetirement(source,closure,Objects.requireNonNull(applied),null,catalogReads,nativeReads);
        }
        public synchronized java.util.concurrent.CompletableFuture<RetirementPreparation> prepareControlRetirement(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,
                OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.runtime.TargetExternalEvidenceStore.Adoption applied,int kind,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            requireReady();
            if(kind!=TargetKafkaReceiptControlArtifact.PROTECTION&&kind!=TargetKafkaReceiptControlArtifact.ANCHOR)
                throw new IllegalArgumentException("CONTROL retirement preparation requires protection or anchor");
            final var owner=kind==TargetKafkaReceiptControlArtifact.PROTECTION?Objects.requireNonNull(applied).protection():Objects.requireNonNull(applied).anchor();
            return prepareControlRetirement(source,closure,owner,catalogReads,nativeReads);
        }
        public synchronized java.util.concurrent.CompletableFuture<RetirementPreparation> prepareControlRetirement(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,
                OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.protocol.TargetExternalEvidenceOwner selected,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            requireReady();Objects.requireNonNull(selected);
            return prepareRetirement(source,closure,null,selected,catalogReads,nativeReads);
        }
        /** Already-closed reservation only; closing original publication I/O and physical retirement are separate obligations. */
        public synchronized java.util.concurrent.CompletableFuture<RetirementPreparation> prepareStagingRetirement(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,
                OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            requireReady();
            return prepareRetirement(source,Objects.requireNonNull(closure),null,null,catalogReads,nativeReads);
        }
        private java.util.concurrent.CompletableFuture<RetirementPreparation> prepareRetirement(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,
                OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
                com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption recovery,
                com.nereusstream.delay.protocol.TargetExternalEvidenceOwner controlOwner,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,
                com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            requireMarkerWindowFree();
            Objects.requireNonNull(source);Objects.requireNonNull(closure);
            final boolean staging=recovery==null&&controlOwner==null;
            final var original=recovery!=null||staging&&closure.subtype()==3?requireRecoveryArchive():null;
            if(retirementHistoryInput!=null)throw new IllegalStateException("original Source retirement history remains owned");
            if(retirementSourceCopy!=null)throw new IllegalStateException("original Source retirement input remains owned");
            Objects.requireNonNull(catalogReads);Objects.requireNonNull(nativeReads);
            if(catalogReads.maxBytes()==Long.MAX_VALUE||nativeReads.maxBytes()==Long.MAX_VALUE)
                throw new IllegalArgumentException("retirement preparation requires finite read bytes");
            final long[] baseInput=new long[CapacityDimension.COUNT];
            baseInput[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,
                    Math.addExact(OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),nativeReads.maxBytes()));
            allocation.requireCovers(new CapacityVector(baseInput));
            final byte[] selection=Bytes.sha256(closure.canonicalBytes(),staging?Bytes.utf8("reserved-staging2")
                    :recovery==null?controlOwner.canonicalBytes():recovery.owner().canonicalBytes());
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
            final long copyWindow;
            if(recovery!=null){
                final var resource=closure.recoveryResource(recovery.owner());
                copyWindow=com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.estimatedBytes(resource,
                        resource.canonicalValueBytes(),com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.MAX_PUBLIC_INPUT_BYTES,
                        closure.canonicalBytes().length);
            }else{
                // Native creator facts or original staging provenance are recovered below. Reserve their
                // complete bounded identity before loading; no caller-supplied identity is signed.
                copyWindow=Math.addExact(768L+com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES,
                        Math.addExact(staging?closure.objectBytes():controlOwner.canonicalValueBytes(),Math.addExact(
                            com.nereusstream.delay.protocol.TargetEvidenceRetirementCopy.MAX_PUBLIC_INPUT_BYTES,closure.canonicalBytes().length)));
            }
            final long[] minimum=new long[CapacityDimension.COUNT];
            minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,Math.addExact(
                    OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),Math.addExact(nativeReads.maxBytes(),Math.multiplyExact(4L,copyWindow))));
            allocation.requireCovers(new CapacityVector(minimum));
            final var next=OwnedCheckpointIo.submit(guard->{
                final Runnable rootCurrent=()->{guard.run();requireReady();};
                final Runnable current=()->{rootCurrent.run();source.requireRetirementReadCurrent(this,rootCurrent);};current.run();
                if(staging){
                    final var uses=source.readClosedStagingUses(this,closure,catalogReads,nativeReads,current);
                    final var copy=catalog.prepareStagingRetirementCopy(uses.closure(),uses.feeOwner(),
                            original==null?null:original.trust,catalogReads,current);
                    current.run();return new RetirementPreparation(this,uses,copy,minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]);
                }
                if(recovery!=null){
                    final var uses=source.readClosedRecoveryUses(this,closure,recovery,catalogReads,nativeReads,current);
                    final var copy=catalog.prepareRecoveryRetirementCopy(uses.closure(),uses.owner(),original.trust,catalogReads,current);
                    current.run();return new RetirementPreparation(this,uses,copy,minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]);
                }
                final var uses=source.readClosedControlUses(this,closure,controlOwner,catalogReads,nativeReads,current);
                final var copy=catalog.prepareControlRetirementCopy(uses.closure(),uses.resource().provenance(),catalogReads,current);
                current.run();return new RetirementPreparation(this,uses,copy,minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]);
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
                requireReady();
                if(markerReservation!=null)throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original marker reservation I/O/window remains owned");
                final var body=com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.decode(entry.mutation().canonicalBody());
                final var provenance=body.resource().provenance();
                if(provenance instanceof com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Staging)
                    throw new IllegalStateException("legacy staging creation-time provenance is not reconstructed from current inputs");
                history=body.resource().subtype()
                        ==com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Subtype.RECOVERY_PROOF?requireRecoveryArchive():null;
                if(retirementHistoryInput!=null&&!sameRetirementEntry(retirementHistoryInput.entry,entry))
                    throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("Root history belongs to another original Source input");
                if(retirementSourceCopy!=null)throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original Root Source copy remains owned");
                if(opening!=null&&!opening.ioExited()||referenceClosure!=null&&!referenceClosure.ioExited()
                        ||closedUsesRead!=null&&!closedUsesRead.ioExited()||retirementPreparation!=null&&!retirementPreparation.ioExited()
                        ||retirementSignature!=null&&!retirementSignature.ioExited()||retirementRelease!=null&&!retirementRelease.ioExited())
                    throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original Root input I/O remains owned");
                final long[] minimum=new long[CapacityDimension.COUNT];
                final long frameWindow=Math.multiplyExact(4L,Math.addExact(entry.mutation().canonicalBodyLength(),(long)entry.mutation().authorIdentityLength()+4096));
                final var prepared=retirementPreparation==null?null:retirementPreparation.completedValue().orElse(null);
                final long keptWindow=prepared!=null&&!prepared.released?prepared.reservedWindowBytes:retainedInputBytes;
                minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(keptWindow,
                        Math.addExact(OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),frameWindow));
                allocation.requireCovers(new CapacityVector(minimum));
                if(prepared!=null&&!prepared.released){
                    final var signed=retirementSignature==null?null:retirementSignature.completedValue().orElse(null);
                    if(prepared.sourceBorrowed)
                        throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original prepared Root input remains borrowed");
                    if(signed!=null&&signed.equals(entry.mutation())){
                        prepared.sourceBorrowed=true;loan=new RetirementSourceCopy(this,entry,prepared,frameWindow,keptWindow);
                        loan.copy=prepared.archived;loan.reading=false;
                    }else loan=new RetirementSourceCopy(this,entry,null,frameWindow,keptWindow);
                }else loan=new RetirementSourceCopy(this,entry,null,frameWindow,keptWindow);
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
                minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(loan.keptWindow,Math.addExact(
                        OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),Math.addExact(loan.frameWindow,Math.multiplyExact(4L,copyWindow))));
                allocation.requireCovers(new CapacityVector(minimum));guard.run();
                final var actual=catalog.readRetirementSourceCopy(body,history==null?null:history.trust,reads,guard);
                guard.run();synchronized(this){loan.copy=actual;loan.reading=false;}return loan;
            }catch(RuntimeException|Error failure){synchronized(this){loan.reading=false;loan.releaseLocal();}throw failure;}
        }

        /** Composes original Root Recovery inputs with the existing complete Source graph and physical guards. */
        public com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content withRecoveryRetirementSourceInputs(
                com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content original,
                com.nereusstream.delay.protocol.TargetQuotaScope scope,
                Supplier<com.nereusstream.delay.store.BoundedReadBudget> catalogReads){
            return withRetirementSourceInputs(original,scope,catalogReads,true);
        }
        public com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content withRecoveryRetirementSourceInputs(
                com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content original,
                com.nereusstream.delay.protocol.TargetQuotaScope scope,Supplier<com.nereusstream.delay.store.BoundedReadBudget> catalogReads,
                ColdConfirmationInputs cold){return withRetirementSourceInputs(original,scope,catalogReads,true,Objects.requireNonNull(cold));}

        /** Original Root inputs for adopted objects and explicit reserved staging; Native projection is checked independently. */
        public com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content withRetirementSourceInputs(
                com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content original,
                com.nereusstream.delay.protocol.TargetQuotaScope scope,
                Supplier<com.nereusstream.delay.store.BoundedReadBudget> catalogReads){
            return withRetirementSourceInputs(original,scope,catalogReads,false);
        }
        public com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content withRetirementSourceInputs(
                com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content original,
                com.nereusstream.delay.protocol.TargetQuotaScope scope,Supplier<com.nereusstream.delay.store.BoundedReadBudget> catalogReads,
                ColdConfirmationInputs cold){return withRetirementSourceInputs(original,scope,catalogReads,false,Objects.requireNonNull(cold));}

        private com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content withRetirementSourceInputs(
                com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content original,
                com.nereusstream.delay.protocol.TargetQuotaScope scope,
                Supplier<com.nereusstream.delay.store.BoundedReadBudget> catalogReads,boolean recoveryOnly){
            return withRetirementSourceInputs(original,scope,catalogReads,recoveryOnly,null);
        }
        private com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content withRetirementSourceInputs(
                com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content original,
                com.nereusstream.delay.protocol.TargetQuotaScope scope,
                Supplier<com.nereusstream.delay.store.BoundedReadBudget> catalogReads,boolean recoveryOnly,ColdConfirmationInputs cold){
            Objects.requireNonNull(original);Objects.requireNonNull(scope);Objects.requireNonNull(catalogReads);
            if(scope.target()!=null)throw new IllegalArgumentException("retirement Source composition requires Shard scope");
            if(retirementHistoryInputs==null)
                throw new IllegalStateException("retirement Source requires independent history configuration");
            final var provider=new com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Provider(){
            public com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs load(com.nereusstream.delay.ownership.SourceReplayEntry entry,
                    com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.Family family,Runnable current){return load(entry,family,current,null);}
            public com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs load(com.nereusstream.delay.ownership.SourceReplayEntry entry,
                    com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.Family family,Runnable current,
                    com.nereusstream.delay.ownership.TargetSourceApplyRuntime.ConfirmationRead source){
                if(family!=com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.Family.RETIREMENT)
                    return original.provider().load(entry,family,current,source);
                if(!(entry instanceof com.nereusstream.delay.ownership.SourceReplayMutation mutation))
                    throw new IllegalArgumentException("retirement Source content changed original frame");
                if(mutation.mutation().type()==com.nereusstream.delay.protocol.SystemMutationType.RESOURCE_DELETE_CONFIRMED){
                    final var confirmation=com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.decode(mutation.mutation().canonicalBody());
                    if(confirmation.markerRetained()&&(!recoveryOnly||confirmation.resource().provenance() instanceof com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Recovery))
                        return loadConfirmationSourceInputs(mutation,scope,original.maximumBytes(),current,source,cold,catalogReads);
                    return original.provider().load(entry,family,current,source);
                }
                if(mutation.mutation().type()!=com.nereusstream.delay.protocol.SystemMutationType.RESOURCE_RETIRE_INTENT)
                    return original.provider().load(entry,family,current,source);
                final var provenance=com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.decode(mutation.mutation().canonicalBody()).resource().provenance();
                if(provenance instanceof com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Staging
                        ||recoveryOnly&&!(provenance instanceof com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Recovery))
                    return original.provider().load(entry,family,current,source);
                return loadRetirementSourceInputs(mutation,scope,catalogReads,original.maximumBytes(),current);
            }};
            return new com.nereusstream.delay.ownership.TargetWorkerSourceRuntimeFactory.Content(provider,original.physical(),original.reads(),
                    original.lookupTimeout(),original.maximumBytes(),original.evidencePublication());
        }

        /** Uses the original Source preparation task; it never queues a nested Root task or requires Worker admission. */
        private com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs loadRetirementSourceInputs(
                com.nereusstream.delay.ownership.SourceReplayMutation entry,com.nereusstream.delay.protocol.TargetQuotaScope scope,
                Supplier<com.nereusstream.delay.store.BoundedReadBudget> catalogReads,long maximumBytes,Runnable current){
            current.run();
            if(entry.mutation().type()!=com.nereusstream.delay.protocol.SystemMutationType.RESOURCE_RETIRE_INTENT)
                throw new IllegalArgumentException("retirement Source provider received another operation");
            final var body=com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.decode(entry.mutation().canonicalBody());
            if(!scope.shard().equals(body.shard())||!Arrays.equals(scope.tenantScope(),body.resource().tenantScope())
                    ||body.resource().provenance() instanceof com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.Staging)
                throw new IllegalStateException("retirement provider requires its exact original Source scope/provenance");
            final var history=readRetirementHistory(entry,current);
            RetirementSourceCopy copy=null;
            try{
                current.run();final var selected=history.grant();
                final long frameBytes=Math.addExact(entry.mutation().canonicalBodyLength(),(long)entry.mutation().authorIdentityLength()+4096);
                long bytes=Math.addExact(frameBytes,history.retainedBytes());
                if(bytes>maximumBytes)throw new IllegalStateException("original retirement history/frame exceeds Source input reservation");
                if(selected.isPresent()){
                    final var reads=Objects.requireNonNull(catalogReads.get(),"original Source catalog read budget");
                    if(reads.maxBytes()==Long.MAX_VALUE)throw new IllegalArgumentException("retirement Source needs a finite catalog byte bound");
                    // The signed exact manifest bounds retained bytes. Root copy loading separately reserves its
                    // full bounded parse/reread window, and requireContext verifies this declared length/digest.
                    final long copyUpper=Math.addExact(body.context().copyManifestBytes(),512L);
                    if(Math.addExact(bytes,Math.addExact(copyUpper,44L))>maximumBytes)
                        throw new IllegalStateException("complete retirement Source copy exceeds input reservation before loading");
                    copy=readRetirementSourceCopy(entry,reads,current);copy.requireCurrent();
                    final var actual=copy.authenticatedCopy();actual.requireCatalog(catalog);
                    bytes=Math.addExact(bytes,Math.addExact(actual.image().estimatedBytes(),44L));
                }
                if(bytes>maximumBytes)throw new IllegalStateException("complete original retirement inputs exceed Source bound");
                final var held=new RetirementSourceInputs(this,entry,scope,history,copy,bytes);
                held.requireCurrent();current.run();return held;
            }catch(RuntimeException|Error failure){
                // Both returned loans have completed synchronous I/O and release only local ownership here.
                if(copy!=null)try{copy.close();}catch(RuntimeException|Error cleanup){failure.addSuppressed(cleanup);}
                try{history.close();}catch(RuntimeException|Error cleanup){failure.addSuppressed(cleanup);}throw failure;
            }
        }

        private static final class RetirementSourceInputs implements com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs {
            private final Opening root;
            private final com.nereusstream.delay.ownership.SourceReplayMutation entry;
            private final RetirementHistoryInput history;
            private final RetirementSourceCopy copy;
            private final com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Retirement input;
            private final long bytes;
            private boolean copyClosed,historyClosed,closing;
            private RetirementSourceInputs(Opening root,com.nereusstream.delay.ownership.SourceReplayMutation entry,
                    com.nereusstream.delay.protocol.TargetQuotaScope scope,RetirementHistoryInput history,RetirementSourceCopy copy,long bytes){
                this.root=root;this.entry=entry;this.history=history;this.copy=copy;this.bytes=bytes;
                final var grant=history.grant();final com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.Approval approval;
                if(grant.isEmpty()){
                    if(copy!=null)throw new IllegalStateException("denied Source input acquired an unrelated copy");approval=null;
                }else{
                    final var actual=Objects.requireNonNull(copy,"authenticated original Catalog copy").authenticatedCopy();actual.requireCatalog(root.catalog);
                    final var original=grant.orElseThrow();
                    approval=new com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.Approval(original.writerKey(),actual,this::requireCurrent,
                            original.activation().tuple(),original);
                }
                input=new com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Retirement(scope,approval,history.sourceHistory());
            }
            public com.nereusstream.delay.ownership.SourceReplayEntry entry(){return entry;}
            public com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Inputs inputs(){return input;}
            public long retainedBytes(){return bytes;}
            public void requireCurrent(){synchronized(root){
                if(copyClosed||historyClosed||closing)throw new IllegalStateException("original retirement Source input closing/closed");
                root.requireReady();history.requireCurrent();if(copy!=null)copy.requireCurrent();
            }}
            public void close(){
                synchronized(root){if(historyClosed)return;if(closing)throw new IllegalStateException("original Source input close remains owned");closing=true;}
                try{
                    if(!copyClosed){if(copy!=null)copy.close();synchronized(root){copyClosed=true;}}
                    if(!historyClosed){history.close();synchronized(root){historyClosed=true;}}
                }finally{synchronized(root){closing=false;}}
            }
        }

        /** Same Source owned-I/O turn; a hot observation is borrowed, never reconstructed from the body DTO. */
        private com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs loadConfirmationSourceInputs(
                com.nereusstream.delay.ownership.SourceReplayMutation entry,com.nereusstream.delay.protocol.TargetQuotaScope scope,
                long maximumBytes,Runnable current,com.nereusstream.delay.ownership.TargetSourceApplyRuntime.ConfirmationRead source,
                ColdConfirmationInputs cold,Supplier<com.nereusstream.delay.store.BoundedReadBudget> catalogReads){
            current.run();final var body=com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.decode(entry.mutation().canonicalBody());
            scope.requireRoute(body.shard(),body.resource().tenantScope());
            final var history=readRetirementHistory(entry,current);
            try{
                current.run();final var grant=history.grant().filter(value->entry.mutation().verifySignature(value.writerKey()));
                final long frame=Math.addExact(entry.mutation().canonicalBodyLength(),(long)entry.mutation().authorIdentityLength()+4096);
                long bytes=Math.addExact(frame,history.retainedBytes());
                if(bytes>maximumBytes)throw new IllegalStateException("complete confirmation frame/history exceeds Source input reservation");
                synchronized(this){
                    requireReady();if(confirmationSourceInputs!=null)throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original confirmation input remains owned");
                    PayloadPreparation payload=null;
                    if(grant.isPresent()){
                        payload=payloadRetirement==null?null:payloadRetirement.completedValue().orElse(null);
                        if(payload!=null){
                            payload.requireRoot(this);if(payload.sourceBorrowed)throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("actual provider observation is already borrowed");
                            payload.observation.requireConfirmation(body);bytes=Math.addExact(bytes,Math.addExact(payload.observation.retainedBytes(),44L));
                        }else{
                            if(source==null||cold==null||evidenceLifecycle==null
                                    ||!evidenceLifecycle.history().protocols().contains(TargetEvidenceObjectLifecycleAuthority.Rule.SOURCE_CONFIRMATION_FROM_ACTUAL_MARKER_OBSERVATION_V1))
                                throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("complete historical confirmation inputs/writer protocol are unavailable");
                        }
                    }
                    if(bytes>maximumBytes)throw new IllegalStateException("complete actual confirmation observation exceeds Source input reservation");
                    final var held=new ConfirmationSourceInputs(this,entry,scope,history,payload,bytes);
                    if(payload!=null)payload.sourceBorrowed=true;confirmationSourceInputs=held;
                    try{if(!held.reading){held.requireCurrent();return held;}}
                    catch(RuntimeException|Error failure){confirmationSourceInputs=null;if(payload!=null)payload.sourceBorrowed=false;throw failure;}
                }
                final ConfirmationSourceInputs held;synchronized(this){held=confirmationSourceInputs;}
                try{
                    source.requireResources(originalResources);source.requireCurrent();
                    final var nativeBudget=Objects.requireNonNull(cold.nativeReads().get());final var recoveryBudget=Objects.requireNonNull(cold.recoveryReads().get());
                    final var registryBudget=Objects.requireNonNull(catalogReads.get());
                    if(nativeBudget.maxBytes()==Long.MAX_VALUE||recoveryBudget.maxBytes()==Long.MAX_VALUE||registryBudget.maxBytes()==Long.MAX_VALUE)
                        throw new IllegalArgumentException("finite historical Source read budgets required");
                    final long upper=Math.addExact(body.context().copyManifestBytes(),Math.addExact(Math.multiplyExact(4L,frame),
                            com.nereusstream.delay.protocol.TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES+16384L));
                    if(Math.addExact(bytes,Math.addExact(upper,44L))>maximumBytes)throw new IllegalStateException("complete historical Source inputs exceed reservation before I/O");
                    final long[] minimum=new long[CapacityDimension.COUNT];minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(retainedInputBytes,
                            Math.addExact(OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),Math.addExact(cold.maximumRecoveryInputBytes(),
                            Math.addExact(Math.multiplyExact(8L,nativeBudget.maxBytes()),Math.multiplyExact(4L,frame)))));
                    allocation.requireCovers(new CapacityVector(minimum));
                    final var images=List.copyOf(cold.images().get());
                    if(com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.recoveryProtectionInputWindowBytes(images)>cold.maximumRecoveryInputBytes())
                        throw new IllegalArgumentException("held historical recovery images exceed original input window");
                    final var admission=acquireEvidenceAdmission();
                    synchronized(this){held.coldAdmission=admission;}
                    final var historical=catalog.authenticateHistoricalConfirmation(source,history.sourceHistory(),admission,images,
                            registryBudget,nativeBudget,recoveryBudget,()->{current.run();held.requireLoadingCurrent();});
                    synchronized(this){held.install(historical);held.bytes=Math.addExact(bytes,Math.addExact(historical.retainedBytes(),44L));}
                    if(held.bytes>maximumBytes)throw new IllegalStateException("actual historical confirmation inputs exceed Source bound");
                    held.requireCurrent();current.run();return held;
                }catch(RuntimeException|Error failure){
                    failed(failure);synchronized(this){held.reading=false;}
                    try{held.close();}catch(RuntimeException|Error cleanup){failure.addSuppressed(cleanup);}throw failure;
                }
            }catch(RuntimeException|Error failure){try{history.close();}catch(RuntimeException|Error cleanup){failure.addSuppressed(cleanup);}throw failure;}
        }
        private static final class ConfirmationSourceInputs implements com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.HeldInputs {
            private final Opening root;
            private final com.nereusstream.delay.ownership.SourceReplayMutation entry;
            private final RetirementHistoryInput history;
            private final PayloadPreparation payload;
            private long bytes;
            private com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Confirmation input;
            private final com.nereusstream.delay.protocol.TargetQuotaScope scope;
            private OxiaSyncTargetKafkaReceiptProtectionCatalog.HistoricalConfirmation historical;
            private TargetEvidenceObjectLifecycleAuthority.AdmissionProtection coldAdmission;
            private boolean reading=true,closing,closed;
            private ConfirmationSourceInputs(Opening root,com.nereusstream.delay.ownership.SourceReplayMutation entry,
                    com.nereusstream.delay.protocol.TargetQuotaScope scope,RetirementHistoryInput history,PayloadPreparation payload,long bytes){
                this.root=root;this.entry=entry;this.history=history;this.payload=payload;this.bytes=bytes;this.scope=scope;
                if(payload!=null||history.grant().filter(value->entry.mutation().verifySignature(value.writerKey())).isEmpty())install(null);
            }
            private void install(OxiaSyncTargetKafkaReceiptProtectionCatalog.HistoricalConfirmation historical){
                this.historical=historical;
                final var grant=history.grant().filter(value->entry.mutation().verifySignature(value.writerKey()));
                final com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.ConfirmationApproval approval;
                if(grant.isEmpty())approval=null;
                else{final var original=grant.orElseThrow();approval=new com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.ConfirmationApproval(
                        original.writerKey(),payload==null?null:payload.observation,historical,this::requireCurrent,original.activation().tuple(),original);}
                input=new com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Confirmation(scope,approval,history.sourceHistory());
                reading=false;
            }
            public com.nereusstream.delay.ownership.SourceReplayEntry entry(){return entry;}
            public com.nereusstream.delay.ownership.TargetSourceAuthorityLoader.Inputs inputs(){return input;}
            public long retainedBytes(){return bytes;}
            /** Immutable observation/history and actual local loan state only; no Worker/Registry/provider RPC at commit. */
            private void requireLoadingCurrent(){synchronized(root){
                if(closed||closing||root.confirmationSourceInputs!=this)throw new IllegalStateException("original confirmation Source input closed");
                root.requireReady();history.requireCurrent();if(payload!=null)payload.requireRoot(root);
            }}
            public void requireCurrent(){requireLoadingCurrent();if(reading||input==null)throw new IllegalStateException("historical Source I/O has not exited");
                if(historical!=null)historical.requireCurrent();}
            public void close(){
                synchronized(root){if(closed)return;if(closing)throw new IllegalStateException("original confirmation input close remains owned");closing=true;}
                try{
                    if(reading)throw new IllegalStateException("original historical Source I/O remains owned");
                    if(historical!=null)historical.close();
                    final TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission;
                    synchronized(root){admission=!root.stopping&&root.failure==null?coldAdmission:null;
                        if(admission!=null&&root.evidenceAdmission!=admission)throw new IllegalStateException("original cold admission ownership changed");}
                    if(admission!=null){admission.close();synchronized(root){root.evidenceAdmission=null;coldAdmission=null;}}
                    history.close();synchronized(root){if(payload!=null)payload.sourceBorrowed=false;root.confirmationSourceInputs=null;closed=true;}
                }
                finally{synchronized(root){closing=false;}}
            }
        }

        /** Authenticates complete history on the existing Source I/O turn; activation/provider/capacity checks remain independent. */
        public RetirementHistoryInput readRetirementHistory(com.nereusstream.delay.ownership.SourceReplayMutation entry,Runnable current){
            Objects.requireNonNull(entry);Objects.requireNonNull(current);
            com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.Action.of(entry.mutation().type());
            final RetirementHistoryInput loan;final KafkaClientArtifactTargetRetirementHistoryConfiguration.Loaded history;
            synchronized(this){
                requireReady();history=Objects.requireNonNull(retirementHistory,"explicit independent retirement Source history configuration");
                if(markerReservation!=null&&(entry.mutation().type()!=com.nereusstream.delay.protocol.SystemMutationType.RESOURCE_DELETE_CONFIRMED
                        ||!markerReservation.ioExited()||payloadRetirement==null||!payloadRetirement.ioExited()))
                    throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original marker/physical I/O/window remains owned");
                if(confirmationSignature!=null&&!confirmationSignature.ioExited())
                    throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original confirmation signing input remains owned");
                if(retirementHistoryInput!=null||retirementSourceCopy!=null&&!sameRetirementEntry(retirementSourceCopy.entry,entry))
                    throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable("original Root Source history/copy remains owned");
                final long[] minimum=new long[CapacityDimension.COUNT];
                final var prepared=retirementPreparation==null?null:retirementPreparation.completedValue().orElse(null);
                final var physical=payloadRetirement==null?null:payloadRetirement.completedValue().orElse(null);
                final long keptWindow=physical!=null&&!physical.released?physical.reservedWindowBytes:
                        prepared!=null&&!prepared.released?prepared.reservedWindowBytes:retainedInputBytes;
                minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(keptWindow,
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
            private final long frameWindow,keptWindow;
            private OxiaSyncTargetKafkaReceiptProtectionCatalog.RetirementCopy copy;
            private boolean reading=true,borrowed,closed;
            private RetirementSourceCopy(Opening root,com.nereusstream.delay.ownership.SourceReplayMutation entry,
                    RetirementPreparation prepared,long frameWindow,long keptWindow){
                this.root=root;this.entry=entry;this.prepared=prepared;this.frameWindow=frameWindow;this.keptWindow=keptWindow;
            }
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
                final var time=source.retirementSigningTime(this,prepared.archived.image().resource(),current);
                final var signed=prepared.stagingUses!=null
                        ?signer.sign(prepared.stagingUses,prepared.archived,protections,retryUntil,time,current)
                        :prepared.controlUses==null
                        ?signer.sign(prepared.uses,prepared.archived,protections,retryUntil,time,current)
                        :signer.sign(prepared.controlUses,prepared.archived,protections,retryUntil,time,current);
                source.requireRetirementReadCurrent(this,current);return signed;
            },this::requireReady,io);
            retirementSignatureSelection=selection;retirementSignature=next;return next.receipt();
        }
        public synchronized java.util.concurrent.CompletableFuture<com.nereusstream.delay.protocol.SystemMutation> signRetirementConfirmation(
                PayloadPreparation payload,long retryUntil){
            payload.requireRoot(this);final var signer=Objects.requireNonNull(retirementSigning,"explicit original confirmation signing configuration");
            if(physicalCompletion!=null)throw new IllegalStateException("original physical completion owns this candidate");
            final long[] minimum=new long[CapacityDimension.COUNT];
            minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(payload.reservedWindowBytes,signer.signingWindowBytes());
            allocation.requireCovers(new CapacityVector(minimum));
            final byte[] selection=Bytes.sha256(payload.observation.evidence().canonicalBytes(),Bytes.u64beBits(retryUntil));
            if(confirmationSignature!=null){
                if(!Arrays.equals(selection,confirmationSignatureSelection))throw new IllegalStateException("original signed confirmation remains owned");
                confirmationSignature.retryCleanup();
                if(!confirmationSignature.ioExited()||confirmationSignature.completedValue().isPresent())return confirmationSignature.receipt();
                confirmationSignature=null;
            }
            if(payload.sourceBorrowed||confirmationSourceInputs!=null||retirementHistoryInput!=null)
                throw new IllegalStateException("original Source still holds actual confirmation inputs");
            final var next=OwnedCheckpointIo.submit(guard->{
                final Runnable current=()->{guard.run();payload.requireRoot(this);};
                final var actual=Objects.requireNonNull(payloadProtection).applied();
                final var time=payload.source.retirementSigningTime(this,actual.record().copy().resource(),current);
                return signer.signConfirmation(actual,payload.observation,retryUntil,time,current);
            },this::requireReady,io);
            confirmationSignatureSelection=selection;confirmationSignature=next;return next.receipt();
        }
        public com.nereusstream.delay.ownership.TargetEvidenceRetirementWorkClassExecutor.Submission submitRetirementConfirmation(
                PayloadPreparation payload,com.nereusstream.delay.ownership.OwnedSourceSystemWriter writer){
            final com.nereusstream.delay.protocol.SystemMutation signed;
            synchronized(this){
                payload.requireRoot(this);
                if(physicalCompletion!=null)throw new IllegalStateException("original physical completion owns this candidate");
                if(retirementSubmitting||confirmationSignature==null||!confirmationSignature.ioExited())
                    throw new IllegalStateException("original confirmation signing/submission remains owned");
                signed=confirmationSignature.completedValue().orElseThrow();Objects.requireNonNull(retirementSigning).requireConfirmationSigned(signed);
                payload.observation.requireConfirmation(com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.decode(signed.canonicalBody()));
                retirementSubmitting=true;
            }
            try{return payload.source.submitRetirementConfirmation(this,signed,writer,()->{
                payload.requireRoot(this);Objects.requireNonNull(retirementSigning).requireConfirmationSigned(signed);
            },retained->{synchronized(this){retirementSubmission=retained;if(stopping)retained.requestStop();}});}
            finally{synchronized(this){retirementSubmitting=false;}}
        }
        /** Actual Source first before release. REJECTED retains durable physical charges and the Native51 copy. */
        public synchronized java.util.concurrent.CompletableFuture<Boolean> releasePayloadPreparation(PayloadPreparation payload,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            payload.requireRoot(this);
            if(retirementSubmitting||payload.sourceBorrowed||confirmationSourceInputs!=null||retirementHistoryInput!=null
                    ||confirmationSignature==null||!confirmationSignature.ioExited())
                throw new IllegalStateException("original confirmation signature/Source input/submission remains owned");
            final var selected=confirmationSignature.completedValue().orElseThrow();
            payload.observation.requireConfirmation(com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.decode(selected.canonicalBody()));
            return completePhysical(payload.source,selected,payload,catalogReads,nativeReads);
        }
        /** Recovery bookkeeping after actual Source/Worker admission; does not reconstruct provider observations or sign. */
        public synchronized java.util.concurrent.CompletableFuture<Boolean> reconcileRetiredMarkerCapacity(
                KafkaClientArtifactTargetWorkerSourceFactory.Opening source,com.nereusstream.delay.protocol.SystemMutation selected,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            requireReady();Objects.requireNonNull(source);clearCompletedPhysicalCompletion();
            if(payloadRetirement!=null||markerReservation!=null||confirmationSourceInputs!=null||retirementHistoryInput!=null||retirementSourceCopy!=null
                    ||retirementSubmitting||evidenceAdmissionAcquiring)
                throw new IllegalStateException("original Root retirement inputs remain owned");
            return completePhysical(source,selected,null,catalogReads,nativeReads);
        }
        private java.util.concurrent.CompletableFuture<Boolean> completePhysical(KafkaClientArtifactTargetWorkerSourceFactory.Opening source,
                com.nereusstream.delay.protocol.SystemMutation selected,PayloadPreparation payload,
                com.nereusstream.delay.store.BoundedReadBudget catalogReads,com.nereusstream.delay.store.BoundedReadBudget nativeReads){
            requireReady();final var body=com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.decode(selected.canonicalBody());
            if(selected.type()!=com.nereusstream.delay.protocol.SystemMutationType.RESOURCE_DELETE_CONFIRMED||!body.markerRetained()
                    ||catalogReads.maxBytes()==Long.MAX_VALUE||nativeReads.maxBytes()==Long.MAX_VALUE)
                throw new IllegalArgumentException("bounded original retained-marker confirmation completion required");
            final byte[] selection=Bytes.sha256(selected.canonicalEnvelope(),new byte[]{(byte)(payload==null?0:1)});
            if(physicalCompletion!=null){
                if(physicalCompletionSource!=source||!Arrays.equals(selection,physicalCompletionSelection))
                    throw new IllegalStateException("another original physical completion remains owned");
                physicalCompletion.retryCleanup();
                if(!physicalCompletion.ioExited()||physicalCompletion.completedValue().isPresent())return physicalCompletion.receipt();
                physicalCompletion=null;
            }
            final long kept=payload==null?retainedInputBytes:payload.reservedWindowBytes;
            final long[] minimum=new long[CapacityDimension.COUNT];minimum[CapacityDimension.BUFFERED_BYTES.wireValue()-1]=Math.addExact(kept,
                    Math.addExact(OxiaSyncTargetKafkaReceiptProtectionCatalog.closureInputWindowBytes(),
                    Math.addExact(Math.multiplyExact(8L,nativeReads.maxBytes()),Math.multiplyExact(4L,
                            Math.addExact(selected.canonicalBodyLength(),(long)selected.authorIdentityLength()+4096)))));
            allocation.requireCovers(new CapacityVector(minimum));
            final var submission=payload==null?null:retirementSubmission;
            final var next=OwnedCheckpointIo.submit(guard->{
                final Runnable current=()->{guard.run();requireReady();source.requireRetirementReadCurrent(this,guard);};
                boolean registryEntered=false,registryFinished=false;
                try{
                    current.run();boolean applied=true;
                    if(payload!=null){
                        final var first=source.retirementMutationResult(this,selected,current).orElseThrow(()->new IllegalStateException("original confirmation Source outcome is still unknown"));
                        applied=first.applyStatus()==com.nereusstream.delay.runtime.ApplyStatus.APPLIED;
                        if(submission!=null&&submission.mutation().equals(selected)){
                            source.settleRetirementConfirmation(this,selected,current);
                            if(!submission.closeForRelease())throw new IllegalStateException("original confirmation append action has not exited");
                        }
                    }
                    if(applied){
                        registryEntered=true;
                        if(!catalog.markerCapacityAlreadySettled(selected,catalogReads,current)){
                            final TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission;
                            synchronized(this){admission=evidenceAdmission;}
                            final var actual=admission==null?acquireEvidenceAdmission():admission;
                            source.settleRetiredMarkerCapacity(this,selected,actual,catalogReads,nativeReads,current);
                        }
                    }
                    registryFinished=true;current.run();
                    if(payload!=null){
                        final TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission;
                        synchronized(this){if(payload.sourceBorrowed)throw new IllegalStateException("Source reacquired the original observation");admission=evidenceAdmission;}
                        if(admission!=null)admission.close();
                        synchronized(this){
                            evidenceAdmission=null;if(payloadProtection!=null){payloadProtection.close();payloadProtection=null;}
                            payload.releaseLocal();payloadRetirement=null;confirmationSignature=null;confirmationSignatureSelection=null;
                            if(retirementSubmission==submission)retirementSubmission=null;
                        }
                    }else{
                        final TargetEvidenceObjectLifecycleAuthority.AdmissionProtection admission;
                        synchronized(this){admission=evidenceAdmission;}
                        if(admission!=null)admission.close();synchronized(this){evidenceAdmission=null;}
                    }
                    return true;
                }catch(RuntimeException|Error failure){if(registryEntered&&!registryFinished)failed(failure);throw failure;}
            },this::requireReady,io);
            physicalCompletionSource=source;physicalCompletionSelection=selection;physicalCompletion=next;return next.receipt();
        }
        private void clearCompletedPhysicalCompletion(){
            final var completed=physicalCompletion;
            if(completed!=null&&completed.ioExited()&&completed.completedValue().filter(Boolean.TRUE::equals).isPresent()){
                physicalCompletion=null;physicalCompletionSource=null;physicalCompletionSelection=null;
            }
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
            final var confirmation=confirmationSignature;if(confirmation!=null)confirmation.requestStop();
            final var completion=physicalCompletion;if(completion!=null)completion.requestStop();
            final var release=retirementRelease;if(release!=null)release.requestStop();
            final var marker=markerReservation;if(marker!=null)marker.requestStop();
            final var payload=payloadRetirement;if(payload!=null)payload.requestStop();
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
            final var confirmation=confirmationSignature;
            if(confirmation!=null){confirmation.requestStop();confirmation.retryCleanup();if(!confirmation.ioExited())return false;}
            final var completion=physicalCompletion;
            if(completion!=null){completion.requestStop();completion.retryCleanup();if(!completion.ioExited())return false;}
            final var release=retirementRelease;
            if(release!=null){release.requestStop();release.retryCleanup();if(!release.ioExited())return false;}
            final var marker=markerReservation;
            if(marker!=null){marker.requestStop();marker.retryCleanup();if(!marker.ioExited())return false;}
            final var payload=payloadRetirement;
            if(payload!=null){payload.requestStop();payload.retryCleanup();if(!payload.ioExited())return false;}
            final var actualPayload=payload==null?null:payload.completedValue().orElse(null);
            if(actualPayload!=null&&actualPayload.sourceBorrowed)return false;
            if(retirementSubmitting)return false;
            if(retirementHistoryInput!=null)return false;
            if(confirmationSourceInputs!=null)return false;
            if(retirementSourceCopy!=null)return false;
            final var submission=retirementSubmission;
            if(submission!=null){submission.requestStop();if(!submission.ioExited())return false;}
            if(evidenceAdmissionAcquiring)return false;
            final var preparedRetirement=retirement==null?null:retirement.completedValue().orElse(null);
            if(preparedRetirement!=null){if(preparedRetirement.sourceBorrowed)return false;preparedRetirement.releaseLocal();}
            final var actual = manager;
            if (actual != null && !closeNative(actual, permit)) return false;
            final boolean registryInputRetained=registryInputOwner!=null;
            // Admin termination is separate from the shared Registry SDK. Hand its exact charge to that original owner.
            if(registryInputRetained){
                if(evidenceAdmission!=null)registryInputOwner.retainClosedRegistryClientInputs(allocation,evidenceAdmission);
                else registryInputOwner.retainClosedRegistryClientInputs(allocation);
            }else if(evidenceAdmission!=null){
                try{evidenceAdmission.close();}catch(RuntimeException|Error rejected){rememberCleanup(rejected);return false;}
            }
            evidenceAdmission=null;
            if(payloadProtection!=null){payloadProtection.close();payloadProtection=null;}
            if(actualPayload!=null)actualPayload.releaseLocal();payloadRetirement=null;
            if(marker!=null)marker.completedValue().ifPresent(MarkerPreparation::releaseLocal);
            if(markerRequest!=null)markerRequest.clearInputs();
            markerReservation=null;markerRequest=null;
            final var signing = recoveryArchive;
            if (signing != null) signing.close();
            final var retireSigning=retirementSigning;if(retireSigning!=null)retireSigning.close();
            recoveryArchive = null; recoveryInputs = null;
            retirementSigning=null;retirementSigningInputs=null;
            retirementHistory=null;retirementHistoryInputs=null;
            evidenceLifecycle=null;evidenceLifecycleAuthenticator=null;
            referenceClosure=null;referenceClosureKey=null;referenceClosureOwner=null;
            closedUsesRead=null;closedUsesSource=null;closedUsesSelection=null;
            retirementPreparation=null;retirementSource=null;retirementSelection=null;
            retirementSignature=null;retirementSignatureSelection=null;retirementRelease=null;
            confirmationSignature=null;confirmationSignatureSelection=null;
            physicalCompletion=null;physicalCompletionSource=null;physicalCompletionSelection=null;
            retirementSubmission=null;
            final var charge = allocation;
            if (charge != null&&!registryInputRetained) { if (nativeStarted) charge.closed(); else charge.releaseUnstarted(); }
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
