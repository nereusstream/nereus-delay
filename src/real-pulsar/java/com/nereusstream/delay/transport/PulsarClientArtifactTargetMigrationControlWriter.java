package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import com.nereusstream.delay.ownership.OwnedSourceCursor;
import com.nereusstream.delay.ownership.ShardLogMutationAppender;
import com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization;
import com.nereusstream.delay.ownership.SystemWriterAppendBudget;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetMigrationAppendPermit;
import com.nereusstream.delay.ownership.TargetMigrationControlWriter;
import com.nereusstream.delay.ownership.TargetMigrationControlMutationFactory;
import com.nereusstream.delay.ownership.TargetMigrationProcessDriver;
import com.nereusstream.delay.ownership.TargetMigrationSourceOpening;
import com.nereusstream.delay.ownership.TargetScopeExposureSource;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetMigrationControlBody;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.time.Duration;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

/** Independent migration CONTROL producer retained on the original guarded P1 Source client. */
public final class PulsarClientArtifactTargetMigrationControlWriter implements TargetMigrationControlWriter {
    private final Object gate = new Object();
    private final TargetMigrationSourceOpening opening;
    private final PulsarClientArtifactOwnedBackendGroup group;
    private final PulsarClient client;
    private final PulsarClientArtifactRecoverySourceCursor reader;
    private final GuardedConsumer<byte[]> source;
    private final LocalBrokerMtlsResolver.SourceReceipt credential;
    private final Supplier<TrustedUtcIntervalEvidence> time;
    private final Duration timeout;
    private final SystemWriterAppendBudget.Writer writer;
    private final TargetDestinationSdkBudget.Client allocation;
    private final CompletableFuture<Producer<byte[]>> creating = new CompletableFuture<>();
    private final OwnedSourceCursor<Producer<byte[]>> nativeOpening;
    private final Map<CompletableFuture<MessageId>, CompletableFuture<MessageId>> completions = new IdentityHashMap<>();
    private Producer<byte[]> producer;
    private PulsarClientArtifactShardLogMutationAppender appender;
    private PulsarClientArtifactProducerClose closing;
    private TargetScopeExposureSource.CopyContract copyContract;
    private boolean openingExited, creationBridgeExited, nativeCreationExited, closeRequested;
    private boolean nativeClosed, untrackedSdkOutcome, released;
    private int activeAppends, activeNativeCalls;

    private PulsarClientArtifactTargetMigrationControlWriter(TargetMigrationSourceOpening opening,
            PulsarClientArtifactOwnedBackendGroup group, PulsarClient client,
            PulsarClientArtifactRecoverySourceCursor reader, GuardedConsumer<byte[]> source,
            LocalBrokerMtlsResolver.SourceReceipt credential, Supplier<TrustedUtcIntervalEvidence> time,
            Duration timeout, SystemWriterAppendBudget.Writer writer, TargetDestinationSdkBudget.Client allocation) {
        this.opening = opening; this.group = group; this.client = client; this.reader = reader; this.source = source;
        this.credential = credential; this.time = time; this.timeout = timeout; this.writer = writer; this.allocation = allocation;
        nativeOpening = new OwnedSourceCursor<>(creating, value -> {
            writer.beginClose();
            final PulsarClientArtifactProducerClose original;
            synchronized (gate) {
                closeRequested = true;
                if (closing == null) closing = new PulsarClientArtifactProducerClose(client, value, timeout, allocation);
                original = closing;
            }
            // Shared backend termination is cleanup only; it cannot make a normal recovery handoff successful.
            return original.normalCloseFuture().thenRun(() -> releaseNative());
        }, group);
    }

    /** Runs in the already-owned strict preparation I/O task; no nested submission to that executor is made. */
    public static PulsarClientArtifactTargetMigrationControlWriter open(String allocationId,
            TargetMigrationSourceOpening opening, PulsarClientArtifactOwnedBackendGroup group,
            Duration timeout, Supplier<TrustedUtcIntervalEvidence> time, TargetDestinationSdkBudget budget,
            CapacityVector cost, LocalBrokerMtlsResolver.SourceReceipt credential,
            PulsarClientArtifactRecoverySourceCursor reader) {
        Objects.requireNonNull(opening).requireCurrent();
        Objects.requireNonNull(timeout); Objects.requireNonNull(time); Objects.requireNonNull(group);
        if (timeout.toMillis() < 1 || timeout.toMillis() > 60_000) {
            throw new IllegalArgumentException("migration CONTROL requires a finite native open/send/close timeout");
        }
        final var graph = opening.graph();
        group.requireWorkerGraph(graph.classes(), graph.resources()); budget.requireWorker(graph.classes(), graph.resources());
        if (budget != opening.sdkBudget()) throw new IllegalArgumentException("migration producer changes its original Process SDK pool");
        opening.retainBackend(group);
        if (group.applicationReady()) throw new IllegalStateException("migration CONTROL requires its original pre-Host SDK owner");
        Objects.requireNonNull(credential, "original migration Source credential");
        credential.authorization().requireAssignment(opening.assignment());
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
        final var source = reader.migrationConsumer(opening, group, credential);
        final var resource = credential.authorization().resource().pulsar();
        final long[] minimum = new long[CapacityDimension.COUNT];
        minimum[CapacityDimension.ADAPTER_PRODUCERS.wireValue() - 1] = 1;
        minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = 1;
        minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = graph.config().maximumSourceFrameBytes();
        if (!Objects.requireNonNull(cost).covers(new CapacityVector(minimum))) {
            throw new IllegalArgumentException("migration CONTROL lacks its original finite producer/frame envelope");
        }
        final var client = group.nativeClientForFactory();
        final var allocation = budget.reserve(allocationId, cost);
        final SystemWriterAppendBudget.Writer writer;
        try { writer = opening.systemWriterBudget().registerWriter(opening.assignment().shardId()); }
        catch (RuntimeException | Error failure) { allocation.releaseUnstarted(); throw failure; }
        final var owned = new PulsarClientArtifactTargetMigrationControlWriter(opening, group, client, reader, source,
                credential, time, timeout, writer, allocation);
        boolean nativeStarted = false;
        try {
            group.registerCleanup(owned, owned::localCallsExited, owned::resourcesTerminated, proof -> {
                proof.requireClient(client); writer.beginClose(); owned.nativeOpening.requestClose();
                owned.nativeOpening.backendShutdownCompleted(group); owned.releaseNative();
            });
            opening.retainControlWriter(owned);
            opening.requireCurrent(); reader.requireCurrent(); owned.requireMaterial();
            final var builder = PulsarClientArtifactProducerFactory.builder(client, resource.authenticatedClusterId(),
                    resource.resourceIncarnation(), resource.physicalTopic(), resource.physicalTopicCreationTimestamp(),
                    "nereus-source-migration-control-" + java.util.UUID.randomUUID())
                    .guardedNativeSendLimit(2)
                    .sendTimeout(Math.toIntExact(timeout.toMillis()), TimeUnit.MILLISECONDS);
            final var nativeFuture = group.createNative(() -> {
                allocation.beginConstruction(); return builder.createAsync();
            }, null);
            nativeStarted = true;
            nativeFuture.whenComplete((value, failure) -> {
                try {
                    if (failure == null) owned.creating.complete(value);
                    else owned.creating.completeExceptionally(failure);
                } finally {
                    synchronized (owned.gate) { owned.creationBridgeExited = true; }
                    group.cleanupProgress();
                }
            });
            owned.nativeOpening.read(timeout.toMillis(), value -> {
                synchronized (owned.gate) { owned.producer = value; }
                opening.requireCurrent(); reader.requireCurrent(); owned.requireMaterial();
                group.requireProducerArtifact(value);
                final var contract = value.guardedNativeSendContract().orElseThrow(
                        () -> new IllegalStateException("migration producer lacks the original finite SDK submission contract"));
                if (contract.maximumNativeSubmissions() != 2) {
                    throw new IllegalStateException("migration producer changed its original SDK native submission bound");
                }
                if (!resource.physicalTopic().equals(value.getTopic())) {
                    throw new IllegalStateException("migration CONTROL producer changed its signed physical topic");
                }
                final var copies = new TargetScopeExposureSource.CopyContract(contract.maximumNativeSubmissions(),
                        Bytes.sha256(Bytes.utf8("nereus-p1-source-native-custody-v1\n"), group.activatedSdkJarSha256(),
                                contract.sha256(), Bytes.u64be(timeout.toMillis())));
                synchronized (owned.gate) { owned.copyContract = copies; }
                final var appender = new PulsarClientArtifactShardLogMutationAppender(value, source,
                        opening.assignment().shardId(), resource.authenticatedClusterId(), resource.resourceIncarnation(),
                        resource.physicalTopic(), resource.physicalTopicCreationTimestamp(), timeout, writer, null, copies, null);
                synchronized (owned.gate) { owned.appender = appender; }
                return appender;
            });
            group.nativeCreationExit(owned.producer).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            synchronized (owned.gate) { owned.nativeCreationExited = true; }
            opening.requireCurrent(); reader.requireCurrent(); owned.requireMaterial();
            return owned;
        } catch (Exception | Error failure) {
            if (failure instanceof InterruptedException) Thread.currentThread().interrupt();
            synchronized (owned.gate) { owned.closeRequested = true; }
            if (!nativeStarted) {
                try {
                    allocation.releaseUnstarted(); writer.abandonUnstarted(); owned.nativeOpening.abandonBeforeOpen();
                    synchronized (owned.gate) { owned.nativeClosed = true; owned.released = true; }
                } catch (IllegalStateException started) {
                    if (!owned.creating.isDone()) owned.creating.completeExceptionally(failure);
                    owned.nativeOpening.requestClose();
                }
                synchronized (owned.gate) { owned.creationBridgeExited = true; }
            } else owned.nativeOpening.requestClose();
            if (failure instanceof Error fatal) throw fatal;
            if (failure instanceof RuntimeException runtime) throw runtime;
            throw new IllegalStateException("original migration CONTROL producer opening is unconfirmed", failure);
        } finally {
            synchronized (owned.gate) { owned.openingExited = true; }
            group.cleanupProgress();
        }
    }

    @Override public void requireBinding(TargetMigrationSourceOpening expected) {
        if (opening != expected) throw new IllegalStateException("migration CONTROL writer changed its original input opening");
    }
    @Override public PulsarClientArtifactOwnedBackendGroup backend() { return group; }
    @Override public TargetScopeExposureSource.CopyContract copyContract() {
        synchronized (gate) { return Objects.requireNonNull(copyContract, "original P1 SDK copy contract is not yet established"); }
    }

    private void requireMaterial() {
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
    }

    @Override public ShardLogMutationAppender.AppendOutcome append(TargetMigrationControlMutationFactory.Prepared exact,
            TargetMigrationAppendPermit permit) {
        Objects.requireNonNull(exact);
        if (exact.mutation().type() != SystemMutationType.APPLY_SHARD_CONTROL
                || !TargetMigrationControlBody.decode(exact.mutation().canonicalBody()).request().scope().equals(opening.configuration().scope())) {
            throw new IllegalArgumentException("migration writer accepts only its exact CONTROL operation");
        }
        Objects.requireNonNull(permit).beforeSdk(this, exact);
        opening.requireCurrent(); reader.requireCurrent(); requireMaterial();
        final PulsarClientArtifactShardLogMutationAppender original;
        synchronized (gate) {
            pruneCompletions();
            if (!openingExited || !nativeCreationExited || appender == null || closeRequested || nativeClosed) {
                throw new IllegalStateException("original migration CONTROL producer is not open for append");
            }
            original = appender; activeAppends++;
        }
        try {
            final var outcome = original.appendMigrationControl(exact, permit, this);
            opening.requireCurrent(); reader.requireCurrent(); requireMaterial();
            return outcome;
        } finally {
            synchronized (gate) { activeAppends--; }
            group.cleanupProgress();
        }
    }

    /** Called only from the existing SystemWriter SDK handoff, after the authoritative Root checks above. */
    CompletableFuture<MessageId> nativeSend(PulsarClientArtifactShardLogMutationAppender original,
            Producer<?> actual, TargetMigrationControlMutationFactory.Prepared exact,
            TargetMigrationAppendPermit permit, Supplier<CompletableFuture<MessageId>> send) {
        synchronized (gate) {
            if (appender != original || producer != actual || activeAppends == 0 || closeRequested || nativeClosed) {
                throw new IllegalStateException("migration CONTROL SEND changed its original native producer/call");
            }
            activeNativeCalls++;
        }
        try {
            requireMaterial();
            final var completion = group.migrationControlSend(this, actual, exact, permit, send);
            synchronized (gate) {
                if (completion == null) untrackedSdkOutcome = true;
                else {
                    if (completions.containsKey(completion)) throw new IllegalStateException("migration CONTROL reused a native send completion");
                    completions.put(completion, null);
                }
            }
            return completion;
        } catch (RuntimeException | Error failure) {
            synchronized (gate) { untrackedSdkOutcome = true; }
            throw failure;
        } finally { synchronized (gate) { activeNativeCalls--; } }
    }

    /** The dependent stage completes only after the original append-budget callback has actually returned. */
    void trackSdkCompletion(CompletableFuture<MessageId> original, CompletableFuture<MessageId> accountingExit) {
        synchronized (gate) {
            if (!completions.containsKey(original) || completions.get(original) != null) {
                throw new IllegalStateException("migration CONTROL lost its original SDK completion");
            }
            completions.put(original, Objects.requireNonNull(accountingExit));
            pruneCompletions();
        }
        accountingExit.whenComplete((__, failure) -> group.cleanupProgress());
    }

    private void pruneCompletions() {
        completions.entrySet().removeIf(entry -> {
            if (entry.getValue() == null || !entry.getValue().isDone()) return false;
            if (entry.getKey().isCancelled()) untrackedSdkOutcome = true;
            return true;
        });
    }

    GuardedConsumer<byte[]> requireNativeSendBinding(PulsarClientArtifactOwnedBackendGroup expected, Producer<?> actual) {
        synchronized (gate) {
            if (group != expected || producer != actual || activeAppends == 0 || closeRequested || nativeClosed) {
                throw new IllegalStateException("migration CONTROL has no original registered native send binding");
            }
        }
        reader.requireRecoveryFenceProof(source);
        return source;
    }

    private boolean localCallsExited() {
        synchronized (gate) {
            pruneCompletions();
            return openingExited && creationBridgeExited && activeAppends == 0 && activeNativeCalls == 0 && completions.isEmpty()
                    && nativeOpening.operationExited();
        }
    }
    @Override public boolean operationsExited() {
        if (!localCallsExited()) return false;
        synchronized (gate) {
            return (nativeCreationExited || nativeClosed) && (!untrackedSdkOutcome || nativeClosed)
                    && (!closeRequested || nativeOpening.cleanupComplete());
        }
    }
    private boolean resourcesTerminated() {
        synchronized (gate) {
            if (!released && nativeClosed && localCallsExited() && nativeOpening.cleanupComplete()) {
                writer.producerClosed(); allocation.closed(); released = true;
            }
            return released;
        }
    }
    private void releaseNative() {
        synchronized (gate) {
            if (nativeClosed) return;
            nativeClosed = true;
            if (appender != null) appender.producerNativeClosed();
        }
    }

    @Override public void requestStop() {
        synchronized (gate) { closeRequested = true; }
        writer.beginClose(); nativeOpening.requestClose(); group.cleanupProgress();
    }
    @Override public boolean cleanupComplete() { return resourcesTerminated(); }
}
