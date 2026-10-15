package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetWorkerPublicationDispatch;
import com.nereusstream.delay.ownership.TargetWorkerRuntimeConfigLoader;
import com.nereusstream.delay.ownership.TargetWorkerShardRuntime;
import com.nereusstream.delay.ownership.TargetWorkerSourceStartup;
import com.nereusstream.delay.ownership.TargetWorkerHostRuntime;
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
import java.util.Optional;
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
            final var reopen = original == null ? null : new ReopenInputs(config, channel, entry.physical(), owner,
                    originalAdmission.image(), original, resolver, profiles, trust, catalog, budget, time, reads, ownerClock);
            return new Opening(startup, worker, dispatch, runtime, original == null, reopen);
        } catch (RuntimeException | Error failure) { startup.failResourceStartup(failure); throw failure; }
    }

    private record ReopenInputs(Configuration config, com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            com.nereusstream.delay.protocol.CanonicalTargetPartition physical, com.nereusstream.delay.protocol.OwnerIdentity owner,
            SystemMutation admission, com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping,
            LocalBrokerMtlsResolver resolver, ProfileCatalog profiles, CredentialAttestationTrustSet trust,
            OxiaSyncTargetKafkaReceiptProtectionCatalog catalog, TargetDestinationSdkBudget budget,
            Supplier<TrustedUtcIntervalEvidence> time, Supplier<BoundedReadBudget> reads, LongSupplier clock) {}

    /** Observes only the original registered native opening, including its late failed-cleanup obligations. */
    public static final class Opening {
        private final TargetWorkerSourceStartup startup;
        private final TargetWorkerShardRuntime worker;
        private final TargetWorkerPublicationDispatch dispatch;
        private final KafkaClientArtifactTargetReceiptRuntime runtime;
        private final boolean newWork;
        private final ReopenInputs reopen;
        private TargetWorkerHostRuntime reopenHost;
        private volatile Opening successor;
        private volatile boolean failed;
        private Opening(TargetWorkerSourceStartup startup, TargetWorkerShardRuntime worker,
                TargetWorkerPublicationDispatch dispatch, KafkaClientArtifactTargetReceiptRuntime runtime, boolean newWork,
                ReopenInputs reopen) {
            this.startup = startup; this.worker = worker; this.dispatch = dispatch; this.runtime = runtime; this.newWork = newWork;
            this.reopen = reopen;
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
        /** One caller-driven transition on the same Host; no new SDK is allocated until original Source and exit proofs hold. */
        public synchronized Optional<Opening> tryReopenForNewWork(TargetWorkerHostRuntime host,
                KafkaClientArtifactTargetReceiptProvider.AnchorPublisher publisher) {
            if (failed || newWork || reopen == null) throw new IllegalStateException("only the original cold K2 opening can hand off");
            Objects.requireNonNull(publisher, "actual restricted CONTROL anchor publisher");
            if (reopenHost != null && reopenHost != host) throw new IllegalArgumentException("K2 successor changed its original Host");
            final var admitted = host.tryBeginPublicationConstruction(worker);
            if (admitted.isEmpty()) return Optional.empty();
            try (var construction = admitted.orElseThrow()) {
                construction.requireCurrent(); reopenHost = host;
                if (successor != null) return Optional.of(successor);
                worker.requirePublicationOwner(reopen.owner(), reopen.clock());
                if (!worker.kafkaMappingSettled(reopen.reads().get(), reopen.admission(), reopen.mapping(), reopen.clock())) {
                    return Optional.empty();
                }
                if (!runtime.settleClose()) return Optional.empty();
                final var actual = worker.requirePublicationChannel(reopen.channel(), true, reopen.reads().get(), reopen.clock());
                if (!Arrays.equals(actual.canonicalBytes(), reopen.physical().canonicalBytes())) {
                    throw new IllegalStateException("K2 successor changes its original physical Target");
                }
                final var material = reopen.resolver().resolve(reopen.channel(), actual, reopen.profiles(), reopen.trust(), reopen.time());
                construction.requireCurrent();
                if (!worker.kafkaMappingSettled(reopen.reads().get(), reopen.admission(), reopen.mapping(), reopen.clock())) {
                    throw new IllegalStateException("K2 successor lost its Source settlement");
                }
                final var provider = new KafkaClientArtifactTargetReceiptProvider(worker, reopen.owner(), reopen.channel(),
                        reopen.catalog(), reopen.config().protectionDigest(), reopen.mapping().admission().kafkaReceipt().receiptResource(),
                        reopen.profiles(), reopen.trust(), material, reopen.time(), reopen.clock(), reopen.config().sdkJar(), reopen.reads(), true);
                final var graph = startup.graph();
                final var active = new KafkaClientArtifactTargetReceiptRuntime(worker, reopen.channel(), actual, provider, publisher,
                        reopen.budget(), dispatch.physical(), graph.classes(), graph.resources(), reopen.config().settings(), graph.io(),
                        new HashMap<>(reopen.config().producer()), new HashMap<>(reopen.config().reader()), reopen.reads(), reopen.clock(), null);
                // Registration already transferred ownership to the original Worker before this final Host recheck.
                successor = new Opening(startup, worker, dispatch, active, true, null);
                construction.requireCurrent(); return Optional.of(successor);
            } catch (TargetWorkerHostRuntime.PublicationConstruction.Unavailable stopping) {
                return Optional.empty();
            } catch (com.nereusstream.delay.store.ReadIncompleteException yield) {
                return Optional.empty();
            } catch (RuntimeException | Error failure) {
                failed = true; startup.failResourceStartup(failure); throw failure;
            }
        }

        /** Actual SEND preparation can follow the one privately retained successor; cold recovery retains runtime(). */
        public Optional<KafkaClientArtifactTargetReceiptRuntime> newWorkRuntime() {
            if (failed) return Optional.empty();
            final var next = successor;
            if (!newWork) return next == null ? Optional.empty() : next.newWorkRuntime();
            return runtime.readyForNewWork() ? Optional.of(runtime) : Optional.empty();
        }
        public KafkaClientArtifactTargetReceiptRuntime runtime() { return runtime; }
    }
}
