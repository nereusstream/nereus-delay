package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch;
import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.ownership.TargetWorkerSourceStartup;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.KafkaReceiptSlotResource;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog;
import com.nereusstream.delay.runtime.ProfileCatalog;
import com.nereusstream.delay.store.BoundedReadBudget;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** File-backed actual K2 startup; original Source facts, CONTROL protection and native owners remain mandatory. */
public final class KafkaClientArtifactTargetReceiptFactory {
    private KafkaClientArtifactTargetReceiptFactory() {}

    /** A selector, never a proof: each requested phase is independently reread from the original Source Store. */
    public record RecoveryAdmission(SystemMutation image, TargetQuotaAttemptBudget.Phase phase) {
        public RecoveryAdmission {
            Objects.requireNonNull(image);
            if (phase != TargetQuotaAttemptBudget.Phase.ADMITTED && phase != TargetQuotaAttemptBudget.Phase.UNKNOWN) {
                throw new IllegalArgumentException("K2 cold opening requires an original ADMITTED or UNKNOWN selector");
            }
        }
    }

    public record Configuration(KafkaClientArtifactTargetReceiptRuntime.Settings settings,
            Map<String,String> producer, Map<String,String> reader, byte[] sdkJar,
            UUID receiptTopicId, int receiptLaneSlot, long slotGeneration, byte[] protectionDigest) {
        public Configuration {
            Objects.requireNonNull(settings); Objects.requireNonNull(receiptTopicId);
            producer = literal(producer); reader = literal(reader);
            Bytes.requireLength(sdkJar, 32, "K2 SDK artifact hash"); sdkJar = Bytes.copy(sdkJar);
            Bytes.requireLength(protectionDigest, 32, "original K2 protection digest"); protectionDigest = Bytes.copy(protectionDigest);
            if (receiptLaneSlot < 0 || receiptLaneSlot == Integer.MAX_VALUE || slotGeneration == 0) {
                throw new IllegalArgumentException("K2 receipt slot/generation is outside its finite native geometry");
            }
        }
        @Override public byte[] sdkJar() { return Bytes.copy(sdkJar); }
        @Override public byte[] protectionDigest() { return Bytes.copy(protectionDigest); }
        @Override public String toString() { return "ConfiguredK2Deployment"; }
        private static Map<String,String> literal(Map<String,String> values) {
            final var result = Map.copyOf(values);
            if (result.size() > 64) throw new IllegalArgumentException("K2 operational field bound exceeded");
            result.forEach((key, value) -> {
                if (key.isBlank() || key.length() > 128 || value.isBlank() || value.length() > 4096) {
                    throw new IllegalArgumentException("K2 operational settings require finite literals");
                }
            });
            return result;
        }
    }

    public static Configuration load(Path path) throws IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        final var result = new Configuration(KafkaClientArtifactTargetReceiptRuntime.Settings.load(f.prefixed("runtime.", 128)),
                f.prefixed("producer.", 64), f.prefixed("reader.", 64), f.hex("sdk.sha256", 32, 32),
                UUID.fromString(f.text("receipt.topicId")), f.integer("receipt.laneSlot"), f.unsigned64("receipt.slotGeneration"),
                f.hex("receipt.protectionDigest", 32, 32));
        f.requireComplete(); return result;
    }

    /** No supplied Runtime/Owner/mapping is accepted: a cold mapping is read from its actual original Admission. */
    public static Opening open(TargetWorkerSourceStartup startup, TargetWorkerShardRuntime worker,
            TargetWorkerPublicationDispatch dispatch, byte[] channelDigest, Path configuration,
            RecoveryAdmission originalAdmission, LocalBrokerMtlsResolver resolver, ProfileCatalog profiles,
            CredentialAttestationTrustSet trust, OxiaSyncTargetKafkaReceiptProtectionCatalog catalog,
            KafkaClientArtifactTargetReceiptProvider.AnchorPublisher controlPublisher, TargetDestinationSdkBudget budget,
            Supplier<TrustedUtcIntervalEvidence> time, Supplier<BoundedReadBudget> reads, LongSupplier ownerClock) throws IOException {
        final var config = load(configuration);
        final byte[] digest = Bytes.copy(channelDigest);
        try (var construction = startup.beginWorkerResourceConstruction(worker)) {
            construction.requireCurrent(); startup.retainPublicationDispatch(worker, dispatch);
            final var entry = startup.publicationChannel(worker, digest, originalAdmission == null);
            final var channel = entry.identity();
            if (channel.context().kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT) {
                throw new IllegalArgumentException("K2 startup requires its exact Source receipt channel");
            }
            final var owner = startup.publicationOwnerIdentity(worker);
            final var original = originalAdmission == null ? null : Objects.requireNonNull(switch (originalAdmission.phase()) {
                case ADMITTED -> worker.readRecoveryAdmission(reads.get(), originalAdmission.image(), ownerClock).kafkaMapping();
                case UNKNOWN -> worker.readKafkaUnknown(reads.get(), originalAdmission.image(), ownerClock).mapping();
                default -> throw new IllegalArgumentException("unregistered cold K2 phase");
            }, "original Source K2 mapping");
            final var resource = new KafkaReceiptSlotResource(entry.physical().resource().kafka().authenticatedClusterId(),
                    config.receiptTopicId(), channel.context().sourceShard().routeIncarnation(),
                    channel.context().sourceShard().partition(), config.receiptLaneSlot(), config.slotGeneration());
            if (original != null) {
                channel.requireExactFrozenIdentity(original.admission().channel());
                final var binding = original.admission().kafkaReceipt();
                if (!resource.equals(binding.receiptResource())
                        || !Arrays.equals(config.protectionDigest(), binding.retentionProtectionDigest())
                        || !config.settings().targetPhysicalTopic().equals(binding.targetPhysicalTopic())) {
                    throw new IllegalArgumentException("cold K2 configuration changes its original Source binding");
                }
            }
            // Material/Registry I/O does not hold a Startup or Store monitor.
            final var material = resolver.resolve(channel, entry.physical(), profiles, trust, time);
            construction.requireCurrent(); startup.publicationChannel(worker, digest, original == null);
            final var provider = new KafkaClientArtifactTargetReceiptProvider(worker, owner, channel, catalog,
                    config.protectionDigest(), resource, profiles, trust, material, time, ownerClock,
                    config.sdkJar(), reads, original == null);
            if (original != null) provider.requireBinding(original.admission().kafkaReceipt());
            final KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher = original == null
                    ? Objects.requireNonNull(controlPublisher, "actual restricted CONTROL anchor publisher")
                    : (observation, target) -> { throw new IllegalStateException("cold K2 mapping cannot publish a replacement anchor"); };
            final var graph = startup.graph();
            final var runtime = new KafkaClientArtifactTargetReceiptRuntime(worker, channel, entry.physical(), provider,
                    publisher, budget, dispatch.physical(), graph.classes(), graph.resources(), config.settings(), graph.io(),
                    new HashMap<>(config.producer()), new HashMap<>(config.reader()), reads, ownerClock, original);
            construction.requireCurrent();
            return new Opening(startup, worker, dispatch, runtime, original == null);
        } catch (RuntimeException | Error failure) { startup.failResourceStartup(failure); throw failure; }
    }

    /** Observes only the original registered native opening, including its late failed-cleanup obligations. */
    public static final class Opening {
        private final TargetWorkerSourceStartup startup;
        private final TargetWorkerShardRuntime worker;
        private final TargetWorkerPublicationDispatch dispatch;
        private final KafkaClientArtifactTargetReceiptRuntime runtime;
        private final boolean newWork;
        private boolean failed;
        private Opening(TargetWorkerSourceStartup startup, TargetWorkerShardRuntime worker,
                TargetWorkerPublicationDispatch dispatch, KafkaClientArtifactTargetReceiptRuntime runtime, boolean newWork) {
            this.startup = startup; this.worker = worker; this.dispatch = dispatch; this.runtime = runtime; this.newWork = newWork;
        }
        public synchronized KafkaClientArtifactTargetReceiptRuntime.Turn advance() {
            if (failed) throw new IllegalStateException("original configured K2 startup failed");
            try {
                startup.requireCurrent(); startup.requireAdmittedWorker(worker);
                final var turn = runtime.runStartupTurn();
                if (turn.failure() != null || turn.status() == KafkaClientArtifactTargetReceiptRuntime.Status.FAILED) {
                    throw new IllegalStateException("actual configured K2 opening failed", turn.failure());
                }
                startup.requireCurrent(); startup.requireAdmittedWorker(worker);
                if (turn.status() == KafkaClientArtifactTargetReceiptRuntime.Status.READY && newWork) runtime.bindDispatch(dispatch);
                return turn;
            } catch (RuntimeException | Error failure) { failed = true; startup.failResourceStartup(failure); throw failure; }
        }
        public KafkaClientArtifactTargetReceiptRuntime runtime() { return runtime; }
    }
}
