package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.LocalBrokerMtlsResolver;
import com.nereusstream.delay.ownership.OwnedSourceCursor;
import com.nereusstream.delay.ownership.ShardLogMutationAppender;
import com.nereusstream.delay.ownership.SourceTransportCredentialAuthorization;
import com.nereusstream.delay.ownership.SystemWriterAppendBudget;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.ownership.TargetRecoveryFenceWriter;
import com.nereusstream.delay.ownership.TargetTimeFenceMutationFactory;
import com.nereusstream.delay.ownership.TargetWorkerSourceStartup;
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

/** One Source-role type4 producer during protected recovery, before Claim takeover and active Host admission. */
public final class PulsarClientArtifactRecoveryFenceWriter implements TargetRecoveryFenceWriter {
    private final Object gate = new Object();
    private final TargetWorkerSourceStartup.RecoveryInputsOpening opening;
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
    private TargetWorkerSourceStartup.RecoveryFenceHandoff handoff;
    private boolean openingExited, creationBridgeExited, nativeCreationExited, closeRequested;
    private boolean nativeClosed, normallyClosed, untrackedSdkOutcome;
    private int activeAppends, activeNativeCalls;
    private long closeStarted;

    private PulsarClientArtifactRecoveryFenceWriter(
            TargetWorkerSourceStartup.RecoveryInputsOpening opening,
            PulsarClientArtifactOwnedBackendGroup group,
            PulsarClient client,
            PulsarClientArtifactRecoverySourceCursor reader,
            GuardedConsumer<byte[]> source,
            LocalBrokerMtlsResolver.SourceReceipt credential,
            Supplier<TrustedUtcIntervalEvidence> time,
            Duration timeout,
            SystemWriterAppendBudget.Writer writer,
            TargetDestinationSdkBudget.Client allocation) {
        this.opening = opening;
        this.group = group;
        this.client = client;
        this.reader = reader;
        this.source = source;
        this.credential = credential;
        this.time = time;
        this.timeout = timeout;
        this.writer = writer;
        this.allocation = allocation;
        nativeOpening = new OwnedSourceCursor<>(
                creating,
                value -> {
                    writer.beginClose();
                    final PulsarClientArtifactProducerClose original;
                    synchronized (gate) {
                        closeRequested = true;
                        if (closing == null) {
                            closing = new PulsarClientArtifactProducerClose(client, value, timeout, allocation);
                        }
                        original = closing;
                    }
                    // Shared backend termination is cleanup only; it cannot make a normal recovery handoff successful.
                    return original.normalCloseFuture().thenRun(() -> releaseNative(true));
                },
                group);
    }

    /** Runs in the already-owned strict preparation I/O task; no nested submission to that executor is made. */
    public static PulsarClientArtifactRecoveryFenceWriter open(
            String allocationId,
            TargetWorkerSourceStartup.RecoveryInputsOpening opening,
            PulsarClientArtifactOwnedBackendGroup group,
            Duration timeout,
            Supplier<TrustedUtcIntervalEvidence> time,
            TargetDestinationSdkBudget budget,
            CapacityVector cost) {
        Objects.requireNonNull(opening).requireFenceFactory();
        Objects.requireNonNull(timeout);
        Objects.requireNonNull(time);
        Objects.requireNonNull(group);
        if (timeout.toMillis() < 1 || timeout.toMillis() > 60_000) {
            throw new IllegalArgumentException(
                    "recovery Source Fence requires a finite native open/send/close timeout");
        }
        final var startup = opening.startup();
        final var graph = startup.graph();
        group.requireWorkerGraph(graph.classes(), graph.resources());
        budget.requireWorker(graph.classes(), graph.resources());
        startup.retainRecoveryInputBackend(group);
        if (group.applicationReady()) {
            throw new IllegalStateException("recovery Source Fence requires its original pre-Host SDK owner");
        }
        final var credential = Objects.requireNonNull(opening.credential(), "original recovery Source credential");
        credential.authorization().requireAssignment(opening.assignment());
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
        final var actualReader = startup.recoverySourceForFence(opening);
        if (!(actualReader instanceof PulsarClientArtifactRecoverySourceCursor reader)) {
            throw new IllegalArgumentException("P1 recovery Fence requires its original bound native recovery cursor");
        }
        final var source = reader.recoveryFenceConsumer(opening, group, credential);
        final var resource = credential.authorization().resource().pulsar();
        final long[] minimum = new long[CapacityDimension.COUNT];
        minimum[CapacityDimension.ADAPTER_PRODUCERS.wireValue() - 1] = 1;
        minimum[CapacityDimension.BUFFERED_MESSAGES.wireValue() - 1] = 1;
        minimum[CapacityDimension.BUFFERED_BYTES.wireValue() - 1] = opening.maximumFrameBytes();
        if (!Objects.requireNonNull(cost).covers(new CapacityVector(minimum))) {
            throw new IllegalArgumentException(
                    "recovery Source Fence lacks its original finite producer/frame envelope");
        }
        final var client = group.nativeClientForFactory();
        final var allocation = budget.reserve(allocationId, cost);
        final SystemWriterAppendBudget.Writer writer;
        try {
            writer = startup.systemWriterBudget()
                    .registerWriter(opening.assignment().shardId());
        } catch (RuntimeException | Error failure) {
            allocation.releaseUnstarted();
            throw failure;
        }
        final var owned = new PulsarClientArtifactRecoveryFenceWriter(
                opening, group, client, reader, source, credential, time, timeout, writer, allocation);
        boolean nativeStarted = false;
        try {
            group.registerCleanup(owned, owned::localCallsExited, owned::resourcesTerminated, proof -> {
                proof.requireClient(client);
                writer.beginClose();
                owned.nativeOpening.requestClose();
                owned.nativeOpening.backendShutdownCompleted(group);
                owned.releaseNative(false);
            });
            opening.retainFenceWriter(owned);
            opening.requireFenceOpening(owned);
            reader.requireCurrent();
            owned.requireMaterial();
            final var builder = PulsarClientArtifactProducerFactory.builder(
                            client,
                            resource.authenticatedClusterId(),
                            resource.resourceIncarnation(),
                            resource.physicalTopic(),
                            resource.physicalTopicCreationTimestamp(),
                            "nereus-source-recovery-fence-" + java.util.UUID.randomUUID())
                    .sendTimeout(Math.toIntExact(timeout.toMillis()), TimeUnit.MILLISECONDS);
            final var nativeFuture = PulsarClientArtifactOwnedBackendGroup.createProducer(client, builder, allocation);
            nativeStarted = true;
            nativeFuture.whenComplete((value, failure) -> {
                try {
                    if (failure == null) {
                        owned.creating.complete(value);
                    } else {
                        owned.creating.completeExceptionally(failure);
                    }
                } finally {
                    synchronized (owned.gate) {
                        owned.creationBridgeExited = true;
                    }
                    group.cleanupProgress();
                }
            });
            owned.nativeOpening.read(timeout.toMillis(), value -> {
                synchronized (owned.gate) {
                    owned.producer = value;
                }
                opening.requireFenceOpening(owned);
                reader.requireCurrent();
                owned.requireMaterial();
                group.requireProducerArtifact(value);
                if (!resource.physicalTopic().equals(value.getTopic())) {
                    throw new IllegalStateException("recovery Source Fence producer changed its signed physical topic");
                }
                final var appender = new PulsarClientArtifactShardLogMutationAppender(
                        value,
                        source,
                        opening.assignment().shardId(),
                        resource.authenticatedClusterId(),
                        resource.resourceIncarnation(),
                        resource.physicalTopic(),
                        resource.physicalTopicCreationTimestamp(),
                        timeout,
                        writer);
                synchronized (owned.gate) {
                    owned.appender = appender;
                }
                return appender;
            });
            group.nativeCreationExit(owned.producer).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
            synchronized (owned.gate) {
                owned.nativeCreationExited = true;
            }
            opening.requireFenceOpening(owned);
            reader.requireCurrent();
            owned.requireMaterial();
            return owned;
        } catch (Exception | Error failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            synchronized (owned.gate) {
                owned.closeRequested = true;
            }
            if (!nativeStarted) {
                try {
                    allocation.releaseUnstarted();
                    writer.abandonUnstarted();
                    owned.nativeOpening.abandonBeforeOpen();
                    synchronized (owned.gate) {
                        owned.nativeClosed = true;
                    }
                } catch (IllegalStateException started) {
                    if (!owned.creating.isDone()) {
                        owned.creating.completeExceptionally(failure);
                    }
                    owned.nativeOpening.requestClose();
                }
                synchronized (owned.gate) {
                    owned.creationBridgeExited = true;
                }
            } else {
                owned.nativeOpening.requestClose();
            }
            startup.retain(failure);
            if (failure instanceof Error fatal) {
                throw fatal;
            }
            if (failure instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException("original recovery Source Fence producer opening is unconfirmed", failure);
        } finally {
            synchronized (owned.gate) {
                owned.openingExited = true;
            }
            group.cleanupProgress();
        }
    }

    @Override
    public void requireBinding(TargetWorkerSourceStartup.RecoveryInputsOpening expected) {
        if (opening != expected) {
            throw new IllegalStateException("recovery Source Fence writer changed its original input opening");
        }
    }

    @Override
    public PulsarClientArtifactOwnedBackendGroup backend() {
        return group;
    }

    private void requireMaterial() {
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.SYSTEM_APPEND, time.get());
        credential.requireOperation(SourceTransportCredentialAuthorization.Operation.LOOKUP, time.get());
    }

    @Override
    public ShardLogMutationAppender.AppendOutcome append(
            TargetTimeFenceMutationFactory.Prepared exact, TargetWorkerSourceStartup.RecoveryFenceAppend permit) {
        Objects.requireNonNull(permit).beforeSdk(this, Objects.requireNonNull(exact));
        opening.requireFenceOpening(this);
        reader.requireCurrent();
        requireMaterial();
        final PulsarClientArtifactShardLogMutationAppender original;
        synchronized (gate) {
            pruneCompletions();
            if (!openingExited || !nativeCreationExited || appender == null || closeRequested || nativeClosed) {
                throw new IllegalStateException("original recovery Source Fence producer is not open for append");
            }
            original = appender;
            activeAppends++;
        }
        try {
            final var outcome = original.appendRecoveryFence(exact, permit, this);
            opening.requireFenceOpening(this);
            reader.requireCurrent();
            requireMaterial();
            return outcome;
        } finally {
            synchronized (gate) {
                activeAppends--;
            }
            group.cleanupProgress();
        }
    }

    /** Called only from the existing SystemWriter SDK handoff, after the authoritative Root checks above. */
    CompletableFuture<MessageId> nativeSend(
            PulsarClientArtifactShardLogMutationAppender original,
            Producer<?> actual,
            TargetTimeFenceMutationFactory.Prepared exact,
            TargetWorkerSourceStartup.RecoveryFenceAppend permit,
            Supplier<CompletableFuture<MessageId>> send) {
        synchronized (gate) {
            if (appender != original || producer != actual || activeAppends == 0 || closeRequested || nativeClosed) {
                throw new IllegalStateException("recovery Source Fence SEND changed its original native producer/call");
            }
            activeNativeCalls++;
        }
        try {
            requireMaterial();
            final var completion = group.recoveryFenceSend(this, actual, exact, permit, send);
            synchronized (gate) {
                if (completion == null) {
                    untrackedSdkOutcome = true;
                } else {
                    if (completions.containsKey(completion)) {
                        throw new IllegalStateException("recovery Source Fence reused a native send completion");
                    }
                    completions.put(completion, null);
                }
            }
            return completion;
        } catch (RuntimeException | Error failure) {
            synchronized (gate) {
                untrackedSdkOutcome = true;
            }
            throw failure;
        } finally {
            synchronized (gate) {
                activeNativeCalls--;
            }
        }
    }

    /** The dependent stage completes only after the original append-budget callback has actually returned. */
    void trackSdkCompletion(CompletableFuture<MessageId> original, CompletableFuture<MessageId> accountingExit) {
        synchronized (gate) {
            if (!completions.containsKey(original) || completions.get(original) != null) {
                throw new IllegalStateException("recovery Source Fence lost its original SDK completion");
            }
            completions.put(original, Objects.requireNonNull(accountingExit));
            pruneCompletions();
        }
        accountingExit.whenComplete((__, failure) -> group.cleanupProgress());
    }

    private void pruneCompletions() {
        completions.entrySet().removeIf(entry -> {
            if (entry.getValue() == null || !entry.getValue().isDone()) {
                return false;
            }
            if (entry.getKey().isCancelled()) {
                untrackedSdkOutcome = true;
            }
            return true;
        });
    }

    GuardedConsumer<byte[]> requireNativeSendBinding(
            PulsarClientArtifactOwnedBackendGroup expected, Producer<?> actual) {
        synchronized (gate) {
            if (group != expected || producer != actual || activeAppends == 0 || closeRequested || nativeClosed) {
                throw new IllegalStateException("recovery Source Fence has no original registered native send binding");
            }
        }
        reader.requireRecoveryFenceProof(source);
        return source;
    }

    private boolean localCallsExited() {
        synchronized (gate) {
            pruneCompletions();
            return openingExited
                    && creationBridgeExited
                    && activeAppends == 0
                    && activeNativeCalls == 0
                    && completions.isEmpty()
                    && nativeOpening.operationExited();
        }
    }

    @Override
    public boolean operationsExited() {
        if (!localCallsExited()) {
            return false;
        }
        synchronized (gate) {
            return (nativeCreationExited || nativeClosed)
                    && (!untrackedSdkOutcome || nativeClosed)
                    && (!closeRequested || nativeOpening.cleanupComplete());
        }
    }

    private boolean resourcesTerminated() {
        synchronized (gate) {
            return nativeClosed && localCallsExited() && nativeOpening.cleanupComplete();
        }
    }

    private void releaseNative(boolean normal) {
        synchronized (gate) {
            if (nativeClosed) {
                return;
            }
            writer.producerClosed();
            allocation.closed();
            nativeClosed = true;
            normallyClosed = normal;
        }
    }

    @Override
    public boolean closeForHandoff(TargetWorkerSourceStartup.RecoveryFenceHandoff permit) {
        Objects.requireNonNull(permit).requireWriter(this);
        synchronized (gate) {
            if (handoff == null) {
                handoff = permit;
                closeStarted = System.nanoTime();
                closeRequested = true;
            }
            if (handoff != permit || !openingExited || appender == null) {
                throw new IllegalStateException(
                        "recovery Source Fence handoff changed its original opening/close permit");
            }
        }
        writer.beginClose();
        nativeOpening.requestClose();
        group.requireNormalJournalHandoff(client);
        permit.requireWriter(this);
        final boolean closed;
        synchronized (gate) {
            closed = normallyClosed && nativeOpening.cleanupComplete();
        }
        if (closed && operationsExited()) {
            group.requireNormalJournalHandoff(client);
            permit.requireWriter(this);
            return true;
        }
        if (System.nanoTime() - closeStarted >= timeout.toNanos()) {
            throw new IllegalStateException(
                    "original recovery Source Fence producer close exceeded its finite handoff deadline");
        }
        return false;
    }
}
