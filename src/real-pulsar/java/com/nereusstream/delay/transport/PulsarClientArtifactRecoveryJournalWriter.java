package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.adapter.PulsarJournalResource;
import com.nereusstream.delay.adapter.TargetPulsarAttemptJournalRecordCodec;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetSourceRecoveryInputs;
import com.nereusstream.delay.ownership.TargetWorkerSourceStartup;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.pulsar.client.api.PulsarClient;

/** One original recovery writer; normal producer close retains the input hold and shared-SDK reservation. */
public final class PulsarClientArtifactRecoveryJournalWriter implements TargetSourceRecoveryInputs.JournalWriter {
    private final TargetWorkerSourceStartup.RecoveryInputsOpening opening;
    private final PulsarClientArtifactOwnedBackendGroup group;
    private final PulsarClient client;
    private final PulsarClientArtifactOwnedTargetJournal.Configuration configuration;
    private final TargetDestinationSdkBudget.Client allocation;
    private volatile PulsarClientArtifactAttemptJournal wire;
    private volatile boolean openingExited;
    private TargetWorkerSourceStartup.JournalHandoff handoff;
    private long closeStarted;

    private PulsarClientArtifactRecoveryJournalWriter(TargetWorkerSourceStartup.RecoveryInputsOpening opening,
            PulsarClientArtifactOwnedBackendGroup group, PulsarClient client,
            PulsarClientArtifactOwnedTargetJournal.Configuration configuration, TargetDestinationSdkBudget.Client allocation) {
        this.opening = opening; this.group = group; this.client = client;
        this.configuration = configuration; this.allocation = allocation;
    }

    /** Called by the original bounded recovery-input provider; registers ownership before actual SDK I/O. */
    public static PulsarClientArtifactRecoveryJournalWriter open(TargetWorkerSourceStartup.RecoveryInputsOpening opening,
            PulsarClientArtifactOwnedBackendGroup group, PulsarClientArtifactOwnedTargetJournal.Configuration configuration,
            TargetDestinationSdkBudget budget, CapacityVector cost,
            PulsarClientArtifactAttemptJournal.TargetWriterGuard writerGuard,
            PulsarAttemptJournal.NegativeReplayAuthority originalNegativeReader, Supplier<TrustedUtcIntervalEvidence> time) {
        Objects.requireNonNull(opening); Objects.requireNonNull(configuration); Objects.requireNonNull(writerGuard);
        Objects.requireNonNull(originalNegativeReader); Objects.requireNonNull(time);
        final var startup = opening.startup();
        final var graph = startup.graph();
        group.requireWorkerGraph(graph.classes(), graph.resources()); budget.requireWorker(graph.classes(), graph.resources());
        startup.retainRecoveryInputBackend(group);
        if (group.applicationReady()) throw new IllegalStateException("recovery Journal requires its original pre-Host SDK owner");
        final var receipt = Objects.requireNonNull(group.credentialReceipt(), "actual recovery Journal mTLS material");
        group.requireLoadedMaterial(receipt); receipt.requireActive(time.get());
        if (!configuration.resource().authenticatedClusterId().equals(group.authenticatedClusterId())
                || configuration.resource().partition() != opening.assignment().shardId().partition()) {
            throw new IllegalArgumentException("recovery Journal differs from its exact original Shard/SDK cluster");
        }
        final long[] minimum = new long[CapacityDimension.COUNT];
        minimum[CapacityDimension.ADAPTER_PRODUCERS.wireValue() - 1] = 1;
        minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = Math.addExact(configuration.maximumRecords(), 2L);
        minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = Math.multiplyExact(3L, configuration.maximumRecordBytes());
        if (!cost.covers(new CapacityVector(minimum))) throw new IllegalArgumentException("recovery Journal lacks its finite replay/native reservation");
        final var client = group.nativeClientForFactory();
        final var reserved = budget.reserve(configuration.allocationId(), cost);
        final var owned = new PulsarClientArtifactRecoveryJournalWriter(opening, group, client, configuration, reserved);
        try {
            group.registerCleanup(owned, owned::operationsExited, reserved::released, proof -> {
                proof.requireClient(owned.client);
                if (owned.wire != null) owned.wire.backendTerminated(proof);
                reserved.closed();
            });
            opening.retainJournalWriter(owned);
            final var guarded = new PulsarClientArtifactAttemptJournal.TargetWriterGuard() {
                @Override public void requireActive(com.nereusstream.delay.protocol.ShardId shard, PulsarJournalResource resource) {
                    opening.requireJournalOpening(owned); receipt.requireActive(time.get());
                    writerGuard.requireActive(shard, resource);
                }
                @Override public void requireRecordVersion(com.nereusstream.delay.protocol.ShardId shard, PulsarJournalResource resource, int version) {
                    opening.requireJournalOpening(owned); receipt.requireActive(time.get());
                    writerGuard.requireRecordVersion(shard, resource, version);
                }
            };
            guarded.requireActive(opening.assignment().shardId(), configuration.resource());
            guarded.requireRecordVersion(opening.assignment().shardId(), configuration.resource(), TargetPulsarAttemptJournalRecordCodec.SETTLEMENT_VERSION);
            reserved.beginConstruction();
            try {
                owned.wire = PulsarClientArtifactAttemptJournal.openTarget(owned.client, opening.assignment().shardId(),
                        configuration.resource(), configuration.producerName(), configuration.replaySubscription(),
                        configuration.timeout(), configuration.maximumRecords(), configuration.maximumRecordBytes(), guarded, originalNegativeReader);
                owned.wire.bindRecoveryWriter(owned);
            } catch (org.apache.pulsar.client.api.PulsarClientException failure) {
                throw new java.util.concurrent.CompletionException(failure);
            }
            opening.requireJournalOpening(owned); receipt.requireActive(time.get());
            return owned;
        } catch (RuntimeException | Error failure) {
            try { reserved.releaseUnstarted(); } catch (IllegalStateException started) { /* actual shared SDK still owns the reservation */ }
            startup.retain(failure); throw failure;
        } finally { owned.openingExited = true; group.cleanupProgress(); }
    }

    @Override public void requireBinding(TargetWorkerSourceStartup.RecoveryInputsOpening expected) {
        if (opening != expected || allocation.released()) throw new IllegalStateException("recovery writer belongs to another original input/SDK allocation");
    }
    @Override public PulsarAttemptJournal journal() {
        final var actual = wire;
        if (!openingExited || actual == null) throw new IllegalStateException("original recovery Journal opening has not exited");
        return actual.journal();
    }
    @Override public PulsarClientArtifactOwnedBackendGroup backend() { return group; }
    @Override public PulsarJournalResource resource() { return configuration.resource(); }
    @Override public String producerName() { return configuration.producerName(); }
    private boolean operationsExited() { return openingExited && (wire == null || wire.operationsExited()); }

    <T> T nativeSend(PulsarClientArtifactAttemptJournal original, org.apache.pulsar.client.api.Producer<?> producer,
            Supplier<T> send) {
        if (wire != original || !openingExited) throw new IllegalStateException("recovery send lacks its original opened wire");
        opening.requireJournalOpening(this);
        return group.recoveryJournalSend(producer, send);
    }

    @Override public boolean closeForHandoff(TargetWorkerSourceStartup.JournalHandoff permit) {
        Objects.requireNonNull(permit).requireWriter(this);
        synchronized (this) {
            if (handoff == null) { handoff = permit; closeStarted = System.nanoTime(); }
            if (handoff != permit) throw new IllegalStateException("original recovery writer close permit cannot be replaced");
        }
        if (!openingExited || wire == null) throw new IllegalStateException("original recovery Journal opening has not exited");
        wire.requestManagedClose();
        group.requireNormalJournalHandoff(client);
        final boolean closed = wire.normallyClosedForHandoff();
        group.requireNormalJournalHandoff(client); permit.requireWriter(this);
        if (closed) return true;
        if (System.nanoTime() - closeStarted >= configuration.timeout().toNanos()) {
            throw new IllegalStateException("original recovery writer close exceeded its finite handoff deadline");
        }
        return false;
    }
}
