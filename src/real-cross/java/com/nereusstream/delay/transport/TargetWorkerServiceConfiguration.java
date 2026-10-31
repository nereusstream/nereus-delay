package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.TargetWorkerChannelInventory;
import com.nereusstream.delay.ownership.TargetWorkerCheckpointConfiguration;
import com.nereusstream.delay.ownership.TargetWorkerProcessRuntime;
import com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch;
import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader;
import com.nereusstream.delay.ownership.TargetWorkerSloServiceConfiguration;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ShardCapacityEnvelope;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.TargetChannelControlRequest;
import com.nereusstream.delay.protocol.TargetKafkaReceiptManagementImage;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.scheduler.WorkClass;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** One literal, bounded real-cross service deployment, loaded before opening the process or an SDK. */
public final class TargetWorkerServiceConfiguration {
    public static final int MAXIMUM_SOURCES = 32;
    public static final int MAXIMUM_CHANNELS = 256;
    public static final int MAXIMUM_CONFIGURATION_FILES = 2048;
    public static final long MAXIMUM_CONFIGURATION_BYTES = 64L << 20;
    private static final int MAXIMUM_FILE_BYTES = 256 << 10;
    private static final Set<CapacityDimension> SDK_DIMENSIONS = Set.of(CapacityDimension.OPEN_FILES,
            CapacityDimension.ADAPTER_CONNECTIONS, CapacityDimension.ADAPTER_PRODUCERS,
            CapacityDimension.ADAPTER_THREADS, CapacityDimension.BUFFERED_MESSAGES,
            CapacityDimension.BUFFERED_BYTES, CapacityDimension.DIRECT_BUFFER_BYTES,
            CapacityDimension.OTHER_NATIVE_BYTES);

    public enum Mode { FRESH, REOPEN, PUBLISHED_RECOVERY, MIGRATION }
    public enum Transport { KAFKA, PULSAR }
    public enum Destination { KAFKA_BASELINE, KAFKA_RECEIPT, PULSAR_BASELINE, PULSAR_MANAGED }

    public sealed interface SourceConfiguration permits KafkaSource, PulsarSource {
        CapacityVector cost();
        long maximumRecoveryInputBytes();
    }
    public record KafkaSource(KafkaClientArtifactTargetWorkerSourceFactory.SourceDeployment deployment)
            implements SourceConfiguration {
        public KafkaSource { Objects.requireNonNull(deployment); }
        @Override public CapacityVector cost() { return deployment.cost(); }
        @Override public long maximumRecoveryInputBytes() { return deployment.source().maximumRecoveryInputBytes(); }
    }
    public record PulsarSource(PulsarClientArtifactTargetWorkerSourceFactory.SourceDeployment deployment)
            implements SourceConfiguration {
        public PulsarSource { Objects.requireNonNull(deployment); }
        @Override public CapacityVector cost() { return deployment.cost(); }
        @Override public long maximumRecoveryInputBytes() { return deployment.source().maximumRecoveryInputBytes(); }
    }
    public sealed interface WriterConfiguration permits KafkaWriter, PulsarWriter {
        CapacityVector cost();
    }
    public record KafkaWriter(KafkaClientArtifactTargetWorkerSourceFactory.SystemWriterConfiguration configuration)
            implements WriterConfiguration {
        public KafkaWriter { Objects.requireNonNull(configuration); }
        @Override public CapacityVector cost() { return configuration.cost(); }
    }
    public record PulsarWriter(PulsarClientArtifactTargetWorkerSourceFactory.SystemWriterConfiguration configuration)
            implements WriterConfiguration {
        public PulsarWriter { Objects.requireNonNull(configuration); }
        @Override public CapacityVector cost() { return configuration.cost(); }
    }
    public sealed interface DestinationConfiguration permits KafkaBaseline, KafkaReceipt, PulsarBaseline, PulsarManaged {
        CapacityVector cost();
    }
    public record KafkaBaseline(KafkaClientArtifactTargetBaselineFactory.Configuration configuration)
            implements DestinationConfiguration {
        public KafkaBaseline { Objects.requireNonNull(configuration); }
        @Override public CapacityVector cost() { return configuration.cost(); }
    }
    public record KafkaReceipt(KafkaClientArtifactTargetReceiptFactory.Configuration configuration)
            implements DestinationConfiguration {
        public KafkaReceipt { Objects.requireNonNull(configuration); }
        @Override public CapacityVector cost() {
            return configuration.settings().producerCost().add(configuration.settings().readerCost());
        }
    }
    public record PulsarBaseline(PulsarClientArtifactTargetProducerFactory.Configuration configuration)
            implements DestinationConfiguration {
        public PulsarBaseline { Objects.requireNonNull(configuration); }
        @Override public CapacityVector cost() { return configuration.cost(); }
    }
    public record PulsarManaged(PulsarClientArtifactTargetProducerFactory.Configuration configuration)
            implements DestinationConfiguration {
        public PulsarManaged { Objects.requireNonNull(configuration); }
        @Override public CapacityVector cost() { return configuration.cost(); }
    }

    /** Independent actual destination client; a Source client supplies no destination credential authority. */
    public record PulsarClientConfiguration(PulsarClientArtifactOwnedBackendGroup.Settings settings,
            byte[] sdkJar, CapacityVector cost) {
        public PulsarClientConfiguration {
            Objects.requireNonNull(settings); Objects.requireNonNull(cost);
            Bytes.requireLength(sdkJar, 32, "destination P1 SDK artifact hash"); sdkJar = Bytes.copy(sdkJar);
            requirePulsarClientCost(settings, cost);
        }
        @Override public byte[] sdkJar() { return Bytes.copy(sdkJar); }
    }

    public record Channel(Destination kind, byte[] digest, Path configurationFile,
            DestinationConfiguration typedConfiguration, Path managementFile,
            KafkaClientArtifactTargetReceiptManagementFactory.Configuration typedManagement,
            Path pulsarClientConfigurationFile, PulsarClientConfiguration typedPulsarClient) {
        public Channel {
            Objects.requireNonNull(kind); Objects.requireNonNull(configurationFile); Objects.requireNonNull(typedConfiguration);
            Bytes.requireLength(digest, 32, "service channel digest"); digest = Bytes.copy(digest);
            if ((managementFile == null) != (typedManagement == null)
                    || managementFile != null && kind != Destination.KAFKA_RECEIPT) {
                throw new IllegalArgumentException("only a K2 channel can reference original receipt management");
            }
            final boolean pulsar = kind == Destination.PULSAR_BASELINE || kind == Destination.PULSAR_MANAGED;
            if (pulsar != (pulsarClientConfigurationFile != null && typedPulsarClient != null)
                    || (pulsarClientConfigurationFile == null) != (typedPulsarClient == null)) {
                throw new IllegalArgumentException("each P1 destination requires its own explicit actual client configuration");
            }
        }
        @Override public byte[] digest() { return Bytes.copy(digest); }
    }

    public record Source(String name, TargetQuotaScope scope, byte[] lineage, Mode mode, Transport transport,
            Path sourceFile, Path writerFile, Path checkpointFile, Path inventoryFile, Path storePath,
            SourceConfiguration typedSource, WriterConfiguration typedWriter,
            TargetWorkerCheckpointConfiguration typedCheckpoint, TargetWorkerChannelInventory.Limits inventoryLimits,
            List<Channel> channels, Path readResourcesFile, TargetWorkerSourceReadResources.Configuration typedReadResources,
            Path sloServiceFile, TargetWorkerSloServiceConfiguration typedSloService,
            Path migrationFile, com.nereusstream.delay.ownership.TargetMigrationConfiguration typedMigration) {
        public Source(String name, TargetQuotaScope scope, byte[] lineage, Mode mode, Transport transport,
                Path sourceFile, Path writerFile, Path checkpointFile, Path inventoryFile, Path storePath,
                SourceConfiguration typedSource, WriterConfiguration typedWriter,
                TargetWorkerCheckpointConfiguration typedCheckpoint, TargetWorkerChannelInventory.Limits inventoryLimits,
                List<Channel> channels, Path readResourcesFile, TargetWorkerSourceReadResources.Configuration typedReadResources,
                Path sloServiceFile, TargetWorkerSloServiceConfiguration typedSloService) {
            this(name, scope, lineage, mode, transport, sourceFile, writerFile, checkpointFile, inventoryFile,
                    storePath, typedSource, typedWriter, typedCheckpoint, inventoryLimits, channels,
                    readResourcesFile, typedReadResources, sloServiceFile, typedSloService, null, null);
        }
        public Source {
            Objects.requireNonNull(name); Objects.requireNonNull(scope); Objects.requireNonNull(mode); Objects.requireNonNull(transport);
            Objects.requireNonNull(sourceFile); Objects.requireNonNull(writerFile); Objects.requireNonNull(checkpointFile);
            Objects.requireNonNull(inventoryFile); Objects.requireNonNull(storePath); Objects.requireNonNull(typedSource);
            Objects.requireNonNull(typedWriter); Objects.requireNonNull(typedCheckpoint); Objects.requireNonNull(inventoryLimits);
            Objects.requireNonNull(readResourcesFile); Objects.requireNonNull(typedReadResources);
            Objects.requireNonNull(sloServiceFile); Objects.requireNonNull(typedSloService);
            Bytes.requireLength(lineage, 16, "service Native lineage"); lineage = Bytes.copy(lineage);
            channels = List.copyOf(channels);
            if ((mode == Mode.MIGRATION) != (migrationFile != null && typedMigration != null)
                    || typedMigration != null && !typedMigration.scope().equals(scope))
                throw new IllegalArgumentException("migration mode requires its exact finite maintenance configuration");
        }
        @Override public byte[] lineage() { return Bytes.copy(lineage); }
    }

    private final TargetWorkerProcessRuntime.Configuration processConfiguration;
    private final TargetWorkerPublicationDispatch.Configuration publicationDispatch;
    private final CapacityVector sdkLimit;
    private final CapacityVector otherFixedCost;
    private final CapacityVector transitionDemand;
    private final CapacityVector processFixedCost;
    private final CapacityVector configuredSdkDemand;
    private final int configuredSdkClients;
    private final Set<String> allocationIds;
    private final int maximumSdkClients;
    private final int maximumSources;
    private final int maximumChannels;
    private final long configurationHeapBytes;
    private final List<Source> sources;

    private TargetWorkerServiceConfiguration(TargetWorkerProcessRuntime.Configuration processConfiguration,
            TargetWorkerPublicationDispatch.Configuration publicationDispatch, CapacityVector sdkLimit,
            CapacityVector otherFixedCost, CapacityVector transitionDemand, CapacityVector processFixedCost,
            int maximumSdkClients, int maximumSources, int maximumChannels, long configurationHeapBytes,
            List<Source> sources, CapacityVector configuredSdkDemand, int configuredSdkClients,
            Set<String> allocationIds) {
        this.processConfiguration = processConfiguration; this.publicationDispatch = publicationDispatch;
        this.sdkLimit = sdkLimit; this.otherFixedCost = otherFixedCost; this.transitionDemand = transitionDemand;
        this.processFixedCost = processFixedCost; this.maximumSdkClients = maximumSdkClients;
        this.maximumSources = maximumSources; this.maximumChannels = maximumChannels;
        this.configurationHeapBytes = configurationHeapBytes; this.sources = List.copyOf(sources);
        this.configuredSdkDemand = configuredSdkDemand; this.configuredSdkClients = configuredSdkClients;
        this.allocationIds = Set.copyOf(allocationIds);
    }

    public TargetWorkerProcessRuntime.Configuration processConfiguration() { return processConfiguration; }
    public TargetWorkerPublicationDispatch.Configuration publicationDispatch() { return publicationDispatch; }
    public CapacityVector sdkLimit() { return sdkLimit; }
    public CapacityVector otherFixedCost() { return otherFixedCost; }
    public CapacityVector transitionDemand() { return transitionDemand; }
    public CapacityVector processFixedCost() { return processFixedCost; }
    public CapacityVector configuredSdkDemand() { return configuredSdkDemand; }
    public int configuredSdkClients() { return configuredSdkClients; }
    public Set<String> allocationIds() { return allocationIds; }
    public int maximumSdkClients() { return maximumSdkClients; }
    public int maximumSources() { return maximumSources; }
    public int maximumChannels() { return maximumChannels; }
    public long configurationHeapBytes() { return configurationHeapBytes; }
    public List<Source> sources() { return sources; }

    /** The actual independently trusted committed grants must be supplied before the first allocation. */
    public void requireCapacity(List<ShardCapacityEnvelope> committed) {
        processConfiguration.worker().requireCapacity(committed,
                processFixedCost.add(otherFixedCost).add(sdkLimit), transitionDemand);
    }

    /** References are resolved against the explicit absolute basePath; no class names or environment expansion. */
    public static TargetWorkerServiceConfiguration load(Path manifest) throws IOException {
        final var files = new InputFiles();
        final Path manifestFile = files.add(manifest, MAXIMUM_FILE_BYTES);
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(manifestFile);
        if (f.integer("version") != 1) throw new IllegalArgumentException("unsupported real-cross service manifest version");
        final Path base = f.absolutePath("basePath");
        requireUnambiguousPath(base);
        if (!Files.isDirectory(base, LinkOption.NOFOLLOW_LINKS)) throw new IOException("service basePath must be a real directory");
        final int maximumSources = boundedCount(f.integer("maximumSources"), MAXIMUM_SOURCES, "Sources");
        final int maximumChannels = boundedCount(f.integer("maximumChannels"), MAXIMUM_CHANNELS, "channels");
        files.maximumFiles = boundedCount(f.integer("maximumConfigurationFiles"), MAXIMUM_CONFIGURATION_FILES, "configuration files");
        files.maximumBytes = positive(f.number("maximumConfigurationBytes"), MAXIMUM_CONFIGURATION_BYTES, "configuration bytes");
        files.requireWithinBounds();
        final long heap = positive(f.number("configurationHeapBytes"), Long.MAX_VALUE - 1, "configuration heap");
        final Path processFile = files.add(resolve(base, f.text("processFile")), MAXIMUM_FILE_BYTES);
        final var process = TargetWorkerRuntimeConfigLoader.loadService(processFile);
        final Path storeRoot = process.worker().store().rootPath();
        if (!storeRoot.isAbsolute()) throw new IllegalArgumentException("process worker.rootPath must be absolute");
        requireDirectoryPath(storeRoot);
        final var dispatch = TargetWorkerRuntimeConfigLoader.loadPublicationDispatch(
                files.add(resolve(base, f.text("publicationDispatchFile")), MAXIMUM_FILE_BYTES));
        final int clients = boundedCount(f.integer("sdk.maximumClients"), Integer.MAX_VALUE - 1, "SDK clients");
        final var sdkLimit = f.capacity("sdk.limit.");
        final var fixed = f.capacity("otherFixedCost.");
        final var transition = f.capacity("transitionDemand.");
        requireSdkCost(sdkLimit);
        if (sdkLimit.amount(CapacityDimension.ADAPTER_CONNECTIONS) == 0
                || sdkLimit.amount(CapacityDimension.ADAPTER_THREADS) == 0
                || fixed.amount(CapacityDimension.ADAPTER_PRODUCERS) < process.worker().systemWriter().writers()) {
            throw new IllegalArgumentException("service SDK pool omits connections, threads or independent writer generations");
        }
        final int count = boundedCount(f.integer("sources.count"), maximumSources, "Sources");
        if (process.observation() == null) throw new IllegalArgumentException("configured service requires explicit ordinary observation limits");
        process.observation().requireSourceCount(count);
        if (count > process.worker().store().maxOwnedShards() || count > process.worker().store().maxOpenShardDbs()
                || count > process.worker().systemWriter().writers()) {
            throw new IllegalArgumentException("service Sources exceed original Store or system-writer limits");
        }
        final var sources = new ArrayList<Source>();
        final var names = new HashSet<String>(); final var shards = new HashSet<ShardId>();
        final var sourceResources = new HashSet<SourceResource>();
        final var tenants = new HashMap<String, byte[]>(); final var allocationIds = new HashSet<String>();
        final var channelIds = new HashSet<String>();
        final var management = new HashMap<Path, KafkaClientArtifactTargetReceiptManagementFactory.Configuration>();
        final var managementIds = new HashSet<String>();
        CapacityVector actualSdk = CapacityVector.empty();
        long retainedHeap = Math.addExact(heap, process.maximumMetricRetainedHeapBytes()), checkpointNative = 0;
        long sloQueuedBytes = 0;
        int sloTasks = 0;
        int actualClients = 0, channelCount = 0;
        for (int index = 0; index < count; index++) {
            final String key = "source." + index + '.';
            final String name = f.text(key + "name");
            if (!name.matches("[a-zA-Z0-9._-]{1,128}") || !names.add(name)) {
                throw new IllegalArgumentException("service Source name is malformed or duplicated");
            }
            final var scope = TargetQuotaScope.decode(files.image(resolve(base, f.text(key + "scopeFile")),
                    TargetQuotaScope.MAX_CANONICAL_BYTES));
            if (scope.target() != null || !shards.add(scope.shard())) {
                throw new IllegalArgumentException("service requires distinct complete Source scopes");
            }
            final String route = scope.shard().routeIncarnation().uuid().toString();
            final var previousTenant = tenants.putIfAbsent(route, scope.tenantScope());
            if (previousTenant != null && !Arrays.equals(previousTenant, scope.tenantScope())) {
                throw new IllegalArgumentException("one Route cannot have different service tenant bindings");
            }
            final byte[] lineage = f.hex(key + "lineage", 16, 16);
            final Mode mode = Mode.valueOf(f.text(key + "mode"));
            final Path migrationFile;
            final com.nereusstream.delay.ownership.TargetMigrationConfiguration migration;
            if (mode == Mode.MIGRATION) {
                migrationFile = files.add(resolve(base, f.text(key + "migrationFile")), MAXIMUM_FILE_BYTES);
                files.references(migrationFile, "migration.scopeFile");
                migration = com.nereusstream.delay.ownership.TargetMigrationConfiguration.load(migrationFile);
                if (!migration.scope().equals(scope)) throw new IllegalArgumentException("migration changes its configured Source scope");
                retainedHeap = Math.addExact(retainedHeap, migration.maximumRetainedBytes());
            } else { migrationFile = null; migration = null; }
            final Transport transport = Transport.valueOf(f.text(key + "transport"));
            final Path sourceFile = files.add(resolve(base, f.text(key + "sourceFile")), MAXIMUM_FILE_BYTES);
            files.references(sourceFile, "source.authorizationFile", "source.signedRouteFile");
            final Path writerFile = files.add(resolve(base, f.text(key + "writerFile")), MAXIMUM_FILE_BYTES);
            files.references(writerFile, "writer.authorizationFile", "writer.signedRouteFile");
            final SourceConfiguration source;
            final WriterConfiguration writer;
            if (transport == Transport.KAFKA) {
                final var kafka = KafkaClientArtifactTargetWorkerSourceFactory.loadSourceDeployment(sourceFile, scope.shard());
                final var kafkaWriter = KafkaClientArtifactTargetWorkerSourceFactory.loadSystemWriterConfiguration(writerFile);
                source = new KafkaSource(kafka); writer = new KafkaWriter(kafkaWriter);
                uniqueAllocation(allocationIds, kafka.source().settings().allocationId());
                uniqueAllocation(allocationIds, kafka.source().recoveryReader().allocationId());
                uniqueAllocation(allocationIds, kafkaWriter.settings().allocationId());
                requireSdkCost(kafka.source().recoveryReader().cost());
                final var settings = kafka.source().settings();
                if (!sourceResources.add(new SourceResource(transport, settings.authenticatedClusterId(),
                        settings.topicId().toString(), scope.shard().partition()))) {
                    throw new IllegalArgumentException("duplicate physical K1 service Source");
                }
                requireMinimumCost(kafka.cost(), settings.maximumConnections(), 1, 0, 1, settings.fetchBytes());
                requireMinimumCost(kafka.source().recoveryReader().cost(), kafka.source().recoveryReader().maximumConnections(),
                        1, 0, 1, process.worker().maximumSourceFrameBytes());
                if (kafka.source().recoveryReader().cost().amount(CapacityDimension.OTHER_NATIVE_BYTES) == 0) {
                    throw new IllegalArgumentException("K1 recovery reader omits its original native resource cost");
                }
                if (kafkaWriter.settings().bufferBytes() < process.worker().maximumSourceFrameBytes()) {
                    throw new IllegalArgumentException("K1 service writer cannot serve the configured Source frame");
                }
                requireMinimumCost(kafkaWriter.cost(), Math.multiplyExact(2L, kafkaWriter.settings().maximumBrokers()),
                        1, 1, 1, kafkaWriter.settings().bufferBytes());
                actualSdk = actualSdk.add(kafka.source().recoveryReader().cost());
                actualClients = Math.addExact(actualClients, 3);
            } else {
                final var pulsar = PulsarClientArtifactTargetWorkerSourceFactory.loadSourceDeployment(sourceFile);
                final var pulsarWriter = PulsarClientArtifactTargetWorkerSourceFactory.loadSystemWriterConfiguration(writerFile);
                source = new PulsarSource(pulsar); writer = new PulsarWriter(pulsarWriter);
                uniqueAllocation(allocationIds, pulsar.source().settings().allocationId());
                uniqueAllocation(allocationIds, pulsarWriter.allocationId());
                if (!sourceResources.add(new SourceResource(transport, pulsar.source().guard().authenticatedClusterId(),
                        Bytes.hex(pulsar.source().guard().resourceIncarnation()), scope.shard().partition()))) {
                    throw new IllegalArgumentException("duplicate physical P1 service Source");
                }
                requirePulsarClientCost(pulsar.source().settings(), pulsar.cost());
                requireMinimumCost(pulsarWriter.cost(), 0, 0, 1, 1, process.worker().maximumSourceFrameBytes());
                actualClients = Math.addExact(actualClients, 2);
            }
            requireSdkCost(source.cost()); requireSdkCost(writer.cost());
            if (mode == Mode.MIGRATION) {
                final CapacityVector maintenanceDemand;
                if (source instanceof KafkaSource kafka && writer instanceof KafkaWriter kafkaWriter)
                    maintenanceDemand = KafkaClientArtifactTargetMigrationFactory.migrationSdkCost(
                            kafka.deployment().source(), kafkaWriter.configuration());
                else maintenanceDemand = PulsarClientArtifactTargetMigrationFactory.migrationSdkCost(
                        ((PulsarSource) source).deployment(), ((PulsarWriter) writer).configuration(),
                        process.worker().maximumSourceFrameBytes());
                requireSdkCost(maintenanceDemand);
            }
            actualSdk = actualSdk.add(source.cost()).add(writer.cost());
            final Path readResourcesFile = files.add(resolve(base, f.text(key + "readResourcesFile")), MAXIMUM_FILE_BYTES);
            final var readResources = TargetWorkerSourceReadResources.Configuration.load(readResourcesFile, source, process.worker());
            requireSdkCost(readResources.sdkDemand());
            actualSdk = actualSdk.add(readResources.sdkDemand());
            actualClients = Math.addExact(actualClients, readResources.sdkClients());
            for (String identity : readResources.allocationIds()) uniqueAllocation(allocationIds, identity);
            final Path checkpointFile = files.add(resolve(base, f.text(key + "checkpointFile")), MAXIMUM_FILE_BYTES);
            final var checkpoint = TargetWorkerCheckpointConfiguration.load(checkpointFile);
            if (!scope.equals(checkpoint.scope()) || !Arrays.equals(lineage, checkpoint.recoveryLineageId())) {
                throw new IllegalArgumentException("checkpoint configuration changes its service Source scope or Native lineage");
            }
            files.add(checkpoint.historyAuthorizationFile(), MAXIMUM_FILE_BYTES);
            requireDirectoryPath(checkpoint.checkpointDirectory());
            requireDisjoint(storeRoot.toAbsolutePath().normalize(), checkpoint.checkpointDirectory().toAbsolutePath().normalize(),
                    "checkpoint directory overlaps the service Store root");
            retainedHeap = Math.addExact(retainedHeap, Math.addExact(source.maximumRecoveryInputBytes(), checkpoint.retainedHeapBytes()));
            checkpointNative = Math.addExact(checkpointNative, checkpoint.sdkNativeBytes());
            final Path inventoryFile = files.add(resolve(base, f.text(key + "inventoryFile")), MAXIMUM_FILE_BYTES);
            final var inventory = TargetWorkerRuntimeConfigLoader.loadChannelInventory(inventoryFile);
            retainedHeap = Math.addExact(retainedHeap, inventory.maximumRetainedBytes());
            final Path sloServiceFile = files.add(resolve(base, f.text(key + "sloServiceFile")), MAXIMUM_FILE_BYTES);
            final var sloService = TargetWorkerSloServiceConfiguration.load(sloServiceFile);
            sloService.requireWorker(process.worker());
            retainedHeap = Math.addExact(retainedHeap, sloService.retainedHeapBytes());
            sloQueuedBytes = Math.addExact(sloQueuedBytes, sloService.maximumQueuedBytes());
            sloTasks = Math.addExact(sloTasks, TargetWorkerSloServiceConfiguration.MAXIMUM_TASKS);
            final Path storePath = storeRoot.toAbsolutePath().normalize().resolve("shards").resolve(route)
                    .resolve(Integer.toUnsignedString(scope.shard().partition()));
            requireDirectoryPath(storePath);
            for (var previous : sources) requireDisjoint(storePath, previous.storePath(), "service Store paths overlap");
            for (var previous : sources) requireDisjoint(checkpoint.checkpointDirectory(),
                    previous.typedCheckpoint().checkpointDirectory(), "service checkpoint directories overlap");
            requireDisjoint(storePath, checkpoint.checkpointDirectory().toAbsolutePath().normalize(),
                    "checkpoint directory overlaps its Source Store");
            final int channels = f.integer(key + "channels.count");
            if (channels > maximumChannels - channelCount || channels > inventory.maximumIdentities()) {
                throw new IllegalArgumentException("service channels exceed their process or Source inventory bound");
            }
            channelCount = Math.addExact(channelCount, channels);
            final var channelList = new ArrayList<Channel>();
            for (int channelIndex = 0; channelIndex < channels; channelIndex++) {
                final String channelKey = key + "channel." + channelIndex + '.';
                final Destination kind = Destination.valueOf(f.text(channelKey + "kind"));
                final byte[] digest = f.hex(channelKey + "digest", 32, 32);
                if (!channelIds.add(Bytes.hex(digest))) throw new IllegalArgumentException("duplicate service channel digest");
                uniqueAllocation(allocationIds, Bytes.hex(digest) + "/producer");
                if (kind == Destination.KAFKA_RECEIPT) {
                    uniqueAllocation(allocationIds, Bytes.hex(digest) + "/recovery-producer");
                    uniqueAllocation(allocationIds, Bytes.hex(digest) + "/recovery-reader");
                }
                final Path configurationFile = files.add(resolve(base, f.text(channelKey + "configurationFile")), MAXIMUM_FILE_BYTES);
                final DestinationConfiguration configuration = switch (kind) {
                    case KAFKA_BASELINE -> new KafkaBaseline(KafkaClientArtifactTargetBaselineFactory.load(configurationFile));
                    case KAFKA_RECEIPT -> new KafkaReceipt(KafkaClientArtifactTargetReceiptFactory.load(configurationFile));
                    case PULSAR_BASELINE -> new PulsarBaseline(PulsarClientArtifactTargetProducerFactory.load(configurationFile));
                    case PULSAR_MANAGED -> new PulsarManaged(PulsarClientArtifactTargetProducerFactory.load(configurationFile));
                };
                if (configuration instanceof PulsarBaseline baseline && baseline.configuration().nativeEnabled()
                        || configuration instanceof PulsarManaged managed && !managed.configuration().nativeEnabled()) {
                    throw new IllegalArgumentException("P1 destination configuration changes its explicit channel kind");
                }
                requireSdkCost(configuration.cost()); actualSdk = actualSdk.add(configuration.cost());
                actualClients = Math.addExact(actualClients, kind == Destination.KAFKA_RECEIPT ? 2 : 1);
                Path pulsarClientFile = null;
                PulsarClientConfiguration pulsarClient = null;
                if (kind == Destination.PULSAR_BASELINE || kind == Destination.PULSAR_MANAGED) {
                    pulsarClientFile = files.add(resolve(base, f.text(channelKey + "pulsarClientConfigurationFile")), MAXIMUM_FILE_BYTES);
                    pulsarClient = loadPulsarClientConfiguration(pulsarClientFile);
                    uniqueAllocation(allocationIds, pulsarClient.settings().allocationId());
                    requireSdkCost(pulsarClient.cost());
                    actualSdk = actualSdk.add(pulsarClient.cost()); actualClients = Math.addExact(actualClients, 1);
                }
                Path managementFile = null;
                KafkaClientArtifactTargetReceiptManagementFactory.Configuration typedManagement = null;
                if (kind == Destination.KAFKA_RECEIPT) {
                    final String managementReference = f.text(channelKey + "managementFile");
                    managementFile = files.add(resolve(base, managementReference), MAXIMUM_FILE_BYTES);
                    typedManagement = management.get(managementFile);
                    if (typedManagement == null) {
                        files.managementReferences(managementFile);
                        typedManagement = KafkaClientArtifactTargetReceiptManagementFactory.load(managementFile);
                        final String identity = "receipt-management/" + Bytes.hex(
                                TargetKafkaReceiptManagementImage.decode(typedManagement.management().image()).topicIdentity());
                        if (!managementIds.add(identity)) throw new IllegalArgumentException("duplicate receipt management deployment identity");
                        uniqueAllocation(allocationIds, identity); requireSdkCost(typedManagement.cost());
                        management.put(managementFile, typedManagement);
                        actualSdk = actualSdk.add(typedManagement.cost()); actualClients = Math.addExact(actualClients, 1);
                    }
                    boolean exactChannel = false;
                    for (var receipt : typedManagement.receipts()) {
                        if (receipt.authority().control().request().branch() instanceof TargetChannelControlRequest request
                                && Arrays.equals(digest, request.channel().digest())) {
                            exactChannel = receipt.authority().authorizedAt().shardId().equals(scope.shard());
                            break;
                        }
                    }
                    if (!exactChannel) throw new IllegalArgumentException("receipt management omits its exact service Source/channel");
                }
                channelList.add(new Channel(kind, digest, configurationFile, configuration, managementFile, typedManagement,
                        pulsarClientFile, pulsarClient));
            }
            sources.add(new Source(name, scope, lineage, mode, transport, sourceFile, writerFile, checkpointFile,
                    inventoryFile, storePath, source, writer, checkpoint, inventory, channelList, readResourcesFile, readResources,
                    sloServiceFile, sloService, migrationFile, migration));
        }
        f.requireComplete();
        files.requireUnchanged();
        for (String identity : allocationIds) {
            for (String digest : channelIds) {
                if (identity.startsWith(digest + "/reader/")) {
                    throw new IllegalArgumentException("explicit SDK allocation collides with an original K2 reader generation");
                }
            }
        }
        if (channelCount > dispatch.maximumChannels() || actualClients > clients || !sdkLimit.covers(actualSdk)) {
            throw new IllegalArgumentException("actual service channels or SDK generations exceed the declared shared pool");
        }
        final var outcomeQueue = process.worker().workClasses().policies().get(WorkClass.OUTCOME_AND_CONTROL);
        if (sloTasks > outcomeQueue.maxQueueRecords() || sloQueuedBytes > outcomeQueue.maxQueueBytes()) {
            throw new IllegalArgumentException("SLO maintenance tasks exceed the configured shared Outcome/Control queue");
        }
        if (files.bytes > heap || retainedHeap > process.worker().resources().certifiedJvmHeapBytes()) {
            throw new IllegalArgumentException("service input, recovery, inventory and checkpoint heap exceed their declared envelope");
        }
        for (var source : sources) {
            for (Path input : files.files.keySet()) requireDisjoint(source.storePath(), input, "deployment input overlaps a service Store");
            for (var other : sources) requireDisjoint(source.storePath(), other.typedCheckpoint().checkpointDirectory().toAbsolutePath().normalize(),
                    "checkpoint directory overlaps a service Store");
        }
        final long[] processAmounts = new long[CapacityDimension.COUNT];
        processAmounts[CapacityDimension.OTHER_NATIVE_BYTES.wireValue() - 1] = Math.addExact(checkpointNative,
                Math.addExact(Math.addExact(process.process().ioNativeBytes(), process.process().metricNativeBytes()),
                        Math.addExact(process.monitoring().nativeBytes(), Math.multiplyExact(3L, process.oxia().nativeBytesPerClient()))));
        final var result = new TargetWorkerServiceConfiguration(process, dispatch, sdkLimit, fixed, transition,
                new CapacityVector(processAmounts), clients, maximumSources, maximumChannels, heap, sources,
                actualSdk, actualClients, allocationIds);
        result.requireCapacity(List.of());
        for (var dimension : List.of(CapacityDimension.PHYSICAL_REQUESTS, CapacityDimension.ZOMBIE_REQUESTS)) {
            if (Math.addExact(dispatch.requests(), process.worker().systemWriter().records()) > process.worker().hardCaps().amount(dimension)) {
                throw new IllegalArgumentException("service publication requests consume the original Source writer reserve");
            }
        }
        for (var dimension : List.of(CapacityDimension.PHYSICAL_BYTES, CapacityDimension.ZOMBIE_BYTES)) {
            if (Math.addExact(dispatch.bytes(), process.worker().systemWriter().bytes()) > process.worker().hardCaps().amount(dimension)) {
                throw new IllegalArgumentException("service publication bytes consume the original Source writer reserve");
            }
        }
        return result;
    }

    private static int boundedCount(int value, int maximum, String name) {
        if (value < 1 || value > maximum) throw new IllegalArgumentException("service " + name + " exceed their finite bound");
        return value;
    }
    private static PulsarClientConfiguration loadPulsarClientConfiguration(Path path) throws IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        final var settings = new PulsarClientArtifactOwnedBackendGroup.Settings(f.text("client.allocationId"),
                f.text("client.serviceUrl"), f.text("client.clusterId"), f.integer("client.ioThreads"),
                f.integer("client.listenerThreads"), f.integer("client.connectionsPerBroker"),
                f.integer("client.maximumConnections"), f.integer("client.maximumUsers"), f.number("client.memoryBytes"),
                f.integer("client.operationTimeoutMs"), f.integer("client.maximumLookupRedirects"));
        final var result = new PulsarClientConfiguration(settings, f.hex("sdk.sha256", 32, 32), f.capacity("client.cost."));
        f.requireComplete(); return result;
    }
    private static long positive(long value, long maximum, String name) {
        if (value < 1 || value > maximum) throw new IllegalArgumentException("service " + name + " exceed their finite bound");
        return value;
    }
    private static void requireSdkCost(CapacityVector cost) {
        for (var dimension : CapacityDimension.values()) {
            if (cost.amount(dimension) == Long.MAX_VALUE || !SDK_DIMENSIONS.contains(dimension) && cost.amount(dimension) != 0) {
                throw new IllegalArgumentException("service SDK cost has an unsupported or unbounded dimension: " + dimension);
            }
        }
    }
    private static void requirePulsarClientCost(PulsarClientArtifactOwnedBackendGroup.Settings settings, CapacityVector cost) {
        requireMinimumCost(cost, settings.maximumConnections(),
                Math.addExact(Math.multiplyExact(3L, settings.ioThreads()), settings.listenerThreads() + 4L),
                0, 0, settings.memoryBytes());
    }
    private static void requireMinimumCost(CapacityVector cost, long connections, long threads,
            long producers, long messages, long bytes) {
        final long[] minimum = new long[CapacityDimension.COUNT];
        minimum[CapacityDimension.OPEN_FILES.wireValue() - 1] = connections;
        minimum[CapacityDimension.ADAPTER_CONNECTIONS.wireValue() - 1] = connections;
        minimum[CapacityDimension.ADAPTER_THREADS.wireValue() - 1] = threads;
        minimum[CapacityDimension.ADAPTER_PRODUCERS.wireValue() - 1] = producers;
        minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = messages;
        minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = bytes;
        if (!cost.covers(new CapacityVector(minimum))) {
            throw new IllegalArgumentException("service SDK cost cannot cover its actual typed client settings");
        }
    }
    private static void uniqueAllocation(Set<String> identities, String identity) {
        if (!identities.add(identity)) throw new IllegalArgumentException("duplicate original SDK allocation identity");
    }
    private static Path resolve(Path base, String value) {
        final Path path = Path.of(value);
        return (path.isAbsolute() ? path : base.resolve(path)).toAbsolutePath().normalize();
    }
    private static void requireDisjoint(Path first, Path second, String message) {
        if (first.startsWith(second) || second.startsWith(first)) throw new IllegalArgumentException(message);
    }
    private static void requireUnambiguousPath(Path path) throws IOException {
        final Path absolute = path.toAbsolutePath().normalize();
        Path cursor = absolute.getRoot();
        for (Path component : absolute) {
            cursor = cursor.resolve(component);
            if (Files.isSymbolicLink(cursor)) throw new IOException("service path has a symbolic-link ambiguity: " + cursor);
            if (!cursor.equals(absolute) && Files.exists(cursor, LinkOption.NOFOLLOW_LINKS)
                    && !Files.isDirectory(cursor, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException("service path traverses an ordinary file: " + cursor);
            }
        }
    }
    private static void requireDirectoryPath(Path path) throws IOException {
        requireUnambiguousPath(path);
        if (Files.exists(path, LinkOption.NOFOLLOW_LINKS) && !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("service directory path is an ordinary file: " + path);
        }
    }

    private record SourceResource(Transport transport, String cluster, String resource, int partition) {}
    private record FileImage(long bytes, byte[] digest, int bound) {}

    private static final class InputFiles {
        private final Map<Path, FileImage> files = new HashMap<>();
        private int maximumFiles = MAXIMUM_CONFIGURATION_FILES;
        private long maximumBytes = MAXIMUM_CONFIGURATION_BYTES;
        private long bytes;

        Path add(Path path, int bound) throws IOException {
            final Path absolute = Objects.requireNonNull(path).toAbsolutePath().normalize();
            requireUnambiguousPath(absolute);
            if (!files.containsKey(absolute)) {
                final byte[] image = TargetWorkerRuntimeConfigLoader.loadBoundedImage(absolute, bound);
                files.put(absolute, new FileImage(image.length, Bytes.sha256(image), bound)); bytes = Math.addExact(bytes, image.length);
                requireWithinBounds();
            } else if (files.get(absolute).bytes() > bound) {
                throw new IllegalArgumentException("service input exceeds its narrower protocol bound");
            }
            return absolute;
        }
        byte[] image(Path path, int bound) throws IOException {
            return TargetWorkerRuntimeConfigLoader.loadBoundedImage(add(path, bound), bound);
        }
        void references(Path configuration, String... keys) throws IOException {
            final var fields = TargetWorkerRuntimeConfigLoader.loadFields(configuration);
            for (String key : keys) add(fields.absolutePath(key), MAXIMUM_FILE_BYTES);
        }
        void managementReferences(Path configuration) throws IOException {
            final var fields = TargetWorkerRuntimeConfigLoader.loadFields(configuration);
            add(fields.absolutePath("management.artifactFile"), 65536);
            add(fields.absolutePath("control.privateKeyFile"), 1024);
            final int count = boundedCount(fields.integer("receipts.count"), MAXIMUM_CONFIGURATION_FILES, "receipt artifacts");
            for (int i = 0; i < count; i++) add(fields.absolutePath("receipts." + i + ".artifactFile"), 65536);
        }
        void requireWithinBounds() {
            if (files.size() > maximumFiles || bytes > maximumBytes) {
                throw new IllegalArgumentException("service deployment input files exceed their declared aggregate bound");
            }
        }
        void requireUnchanged() throws IOException {
            for (var entry : files.entrySet()) {
                requireUnambiguousPath(entry.getKey());
                final var original = entry.getValue();
                final byte[] current = TargetWorkerRuntimeConfigLoader.loadBoundedImage(entry.getKey(), original.bound());
                if (current.length != original.bytes() || !Arrays.equals(Bytes.sha256(current), original.digest())) {
                    throw new IOException("service deployment input changed during complete preflight");
                }
            }
        }
    }
}
