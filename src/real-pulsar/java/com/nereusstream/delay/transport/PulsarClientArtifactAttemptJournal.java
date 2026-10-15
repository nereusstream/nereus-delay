package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.adapter.PulsarAttemptJournalRecordCodec;
import com.nereusstream.delay.adapter.PulsarJournalResource;
import com.nereusstream.delay.adapter.TargetPulsarAttemptJournalRecordCodec;
import com.nereusstream.delay.protocol.ShardId;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.GuardedMessageId;
import org.apache.pulsar.client.api.GuardedSendSuccessEvidence;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;

/** Source-locked P1 transport and contiguous startup replay for one Attempt Journal. */
public final class PulsarClientArtifactAttemptJournal implements PulsarAttemptJournal.DurableAppender, AutoCloseable {
    private final PulsarClient client;
    private final Producer<byte[]> producer;
    private final PulsarClientArtifactProducerClose producerClose;
    private final PulsarJournalResource resource;
    private final TopicResourceGuard expectedGuard;
    private final String replaySubscriptionName;
    private final Duration responseTimeout;
    private final int responseTimeoutMs;
    private final ShardId shard;
    private final PulsarAttemptJournal journal;
    private final int replayedRecords;
    private final boolean targetNamespace;
    private final int maximumRecords;
    private final long maximumRecordBytes;
    private final TargetWriterGuard targetWriterGuard;
    private final PulsarAttemptJournal.NegativeReplayAuthority negativeReplayAuthority;
    private final Object lifecycle = new Object();
    private final java.util.Set<CompletableFuture<Void>> readerCloses =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private final java.util.Set<CompletableFuture<MessageId>> nativeAppends =
            java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    private Throwable readerOpeningFailure;
    private int activeAppends;
    private boolean stopping, backendStopped;
    private CompletableFuture<Void> managedProducerClose;
    private PulsarClientArtifactRecoveryJournalWriter recoveryWriter;
    private int responseLossRecoveries;

    /** Opens the guarded producer and reconstructs the complete Journal from its earliest retained record. */
    public static PulsarClientArtifactAttemptJournal open(
            final PulsarClient client,
            final ShardId shard,
            final PulsarJournalResource resource,
            final String journalProducerName,
            final String replaySubscriptionName,
            final Duration responseTimeout)
            throws PulsarClientException {
        Objects.requireNonNull(client, "client");
        requirePositive(responseTimeout);
        final PulsarJournalResource exactResource = Objects.requireNonNull(resource, "resource");
        final Producer<byte[]> producer = PulsarClientArtifactProducerFactory.create(
                client,
                exactResource.authenticatedClusterId(),
                exactResource.resourceIncarnation(),
                exactResource.physicalTopic(),
                exactResource.physicalTopicCreationTimestamp(),
                journalProducerName);
        return openWithProducerForTesting(
                client, shard, exactResource, producer, replaySubscriptionName, responseTimeout);
    }

    /** Opens a distinct source-protected Target Journal namespace; never use an active Lane Journal resource. */
    public static PulsarClientArtifactAttemptJournal openTarget(
            final PulsarClient client, final ShardId shard, final PulsarJournalResource resource,
            final String journalProducerName, final String replaySubscriptionName, final Duration responseTimeout,
            final int maximumRecords, final long maximumRecordBytes, final TargetWriterGuard writerGuard)
            throws PulsarClientException {
        return openTarget(client, shard, resource, journalProducerName, replaySubscriptionName, responseTimeout,
                maximumRecords, maximumRecordBytes, writerGuard, null);
    }

    public static PulsarClientArtifactAttemptJournal openTarget(
            final PulsarClient client, final ShardId shard, final PulsarJournalResource resource,
            final String journalProducerName, final String replaySubscriptionName, final Duration responseTimeout,
            final int maximumRecords, final long maximumRecordBytes, final TargetWriterGuard writerGuard,
            final PulsarAttemptJournal.NegativeReplayAuthority negativeReplayAuthority)
            throws PulsarClientException {
        Objects.requireNonNull(client, "client");
        requirePositive(responseTimeout);
        if (maximumRecords <= 0 || maximumRecordBytes <= 0
                || maximumRecords == Integer.MAX_VALUE || maximumRecordBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Target Journal replay limits must be finite and positive");
        }
        final var exactResource = Objects.requireNonNull(resource, "resource");
        final var guard = Objects.requireNonNull(writerGuard, "writerGuard");
        guard.requireActive(shard, exactResource);
        final var producer = PulsarClientArtifactProducerFactory.createExclusiveJournal(
                client, exactResource.authenticatedClusterId(), exactResource.resourceIncarnation(),
                exactResource.physicalTopic(), exactResource.physicalTopicCreationTimestamp(), journalProducerName);
        try {
            return new PulsarClientArtifactAttemptJournal(client, producer, shard, exactResource,
                    replaySubscriptionName, responseTimeout, true, maximumRecords, maximumRecordBytes, guard, negativeReplayAuthority);
        } catch (RuntimeException | PulsarClientException | Error failure) {
            PulsarClientArtifactProducerClose.afterFailedCreation(client, producer, responseTimeout, failure);
            throw failure;
        }
    }

    /**
     * Opens an Attempt Journal around an already guarded producer. This
     * package-scoped seam exists so the real P1 smoke can discard a committed
     * client response without weakening the production producer factory.
     * Ownership of {@code producer} transfers to the returned Journal.
     */
    static PulsarClientArtifactAttemptJournal openWithProducerForTesting(
            final PulsarClient client,
            final ShardId shard,
            final PulsarJournalResource resource,
            final Producer<byte[]> producer,
            final String replaySubscriptionName,
            final Duration responseTimeout)
            throws PulsarClientException {
        Objects.requireNonNull(client, "client");
        requirePositive(responseTimeout);
        final PulsarJournalResource exactResource = Objects.requireNonNull(resource, "resource");
        final Producer<byte[]> exactProducer = Objects.requireNonNull(producer, "producer");
        try {
            return new PulsarClientArtifactAttemptJournal(
                    client,
                    exactProducer,
                    Objects.requireNonNull(shard, "shard"),
                    exactResource,
                    replaySubscriptionName,
                    responseTimeout);
        } catch (RuntimeException | PulsarClientException | Error failure) {
            PulsarClientArtifactProducerClose.afterFailedCreation(client, exactProducer, responseTimeout, failure);
            throw failure;
        }
    }

    private PulsarClientArtifactAttemptJournal(
            final PulsarClient client, final Producer<byte[]> producer, final ShardId shard,
            final PulsarJournalResource resource, final String replaySubscriptionName, final Duration responseTimeout)
            throws PulsarClientException {
        this(client, producer, shard, resource, replaySubscriptionName, responseTimeout,
                false, Integer.MAX_VALUE, Long.MAX_VALUE, null);
    }

    private PulsarClientArtifactAttemptJournal(
            final PulsarClient client, final Producer<byte[]> producer, final ShardId shard,
            final PulsarJournalResource resource, final String replaySubscriptionName, final Duration responseTimeout,
            final boolean targetNamespace, final int maximumRecords, final long maximumRecordBytes,
            final TargetWriterGuard targetWriterGuard)
            throws PulsarClientException {
        this(client, producer, shard, resource, replaySubscriptionName, responseTimeout, targetNamespace,
                maximumRecords, maximumRecordBytes, targetWriterGuard, null);
    }

    private PulsarClientArtifactAttemptJournal(
            final PulsarClient client, final Producer<byte[]> producer, final ShardId shard,
            final PulsarJournalResource resource, final String replaySubscriptionName, final Duration responseTimeout,
            final boolean targetNamespace, final int maximumRecords, final long maximumRecordBytes,
            final TargetWriterGuard targetWriterGuard,
            final PulsarAttemptJournal.NegativeReplayAuthority negativeReplayAuthority)
            throws PulsarClientException {
        this.client = Objects.requireNonNull(client, "client");
        this.producer = Objects.requireNonNull(producer, "producer");
        this.producerClose = new PulsarClientArtifactProducerClose(client, producer, responseTimeout);
        this.shard = Objects.requireNonNull(shard, "shard");
        this.resource = Objects.requireNonNull(resource, "resource");
        this.expectedGuard = new TopicResourceGuard(
                resource.authenticatedClusterId(),
                resource.resourceIncarnation(),
                resource.physicalTopicCreationTimestamp());
        this.responseTimeout = requirePositive(responseTimeout);
        this.responseTimeoutMs = Math.toIntExact(this.responseTimeout.toMillis());
        this.replaySubscriptionName = Objects.requireNonNull(replaySubscriptionName, "replaySubscriptionName");
        if (!resource.physicalTopic().equals(producer.getTopic()) || resource.partition() != shard.partition()) {
            throw new IllegalArgumentException("Attempt Journal producer/resource/shard binding differs");
        }
        this.targetNamespace = targetNamespace;
        this.maximumRecords = maximumRecords;
        this.maximumRecordBytes = maximumRecordBytes;
        this.targetWriterGuard = targetWriterGuard;
        this.negativeReplayAuthority = negativeReplayAuthority;
        requireTargetWriter();
        this.journal = targetNamespace
                ? PulsarAttemptJournal.forTargets(this.shard, this, resource, maximumRecords, maximumRecordBytes)
                : new PulsarAttemptJournal(this.shard, this, resource);
        this.replayedRecords = replay(this.client, this.replaySubscriptionName);
        requireTargetWriter();
    }

    public PulsarAttemptJournal journal() {
        return journal;
    }

    public int replayedRecords() {
        return replayedRecords;
    }

    /** Number of ambiguous appends resolved by exact contiguous Journal readback. */
    public synchronized int responseLossRecoveries() {
        return responseLossRecoveries;
    }

    /**
     * Closes and reopens this exact Journal as a fresh process would. This
     * package-scoped certification seam exercises the production {@link
     * #open(PulsarClient, ShardId, PulsarJournalResource, String, String,
     * Duration)} path with the same durable replay subscription.
     */
    PulsarClientArtifactAttemptJournal reopenAfterCloseForTesting() throws PulsarClientException {
        final String producerName = producer.getProducerName();
        close();
        if (targetNamespace) {
            return openTarget(client, shard, resource, producerName, replaySubscriptionName, responseTimeout,
                    maximumRecords, maximumRecordBytes, targetWriterGuard);
        }
        return open(client, shard, resource, producerName, replaySubscriptionName, responseTimeout);
    }

    @Override
    public PulsarAttemptJournal.JournalPosition append(final PulsarAttemptJournal.AppendRequest request) {
        synchronized (lifecycle) {
            if (stopping || !readerCloses.isEmpty()) throw new IllegalStateException("original Journal is closing or retains an unclosed reader");
            activeAppends++;
        }
        try { return appendOwned(request); }
        finally {
            synchronized (lifecycle) { activeAppends--; }
            final var backend = PulsarClientArtifactOwnedBackendGroup.find(client);
            if (backend != null) backend.cleanupProgress();
        }
    }

    private PulsarAttemptJournal.JournalPosition appendOwned(final PulsarAttemptJournal.AppendRequest request) {
        Objects.requireNonNull(request, "request");
        if (request.mapping().producer().isTarget() != targetNamespace) {
            throw new IllegalArgumentException("Attempt Journal physical namespace differs from mapping");
        }
        if (request.kind() == PulsarAttemptJournal.RecordKind.RESOLVED_NOT_PUBLISHED) {
            if (!targetNamespace || negativeReplayAuthority == null) {
                throw new IllegalStateException("Target v2 writer requires configured authenticated v2 recovery reader");
            }
            targetWriterGuard.requireRecordVersion(shard, resource, TargetPulsarAttemptJournalRecordCodec.SETTLEMENT_VERSION);
        }
        final byte[] payload = targetNamespace ? TargetPulsarAttemptJournalRecordCodec.encode(request)
                : PulsarAttemptJournalRecordCodec.encode(request);
        if (targetNamespace && payload.length > maximumRecordBytes) throw new IllegalStateException("Target Journal append exceeds its byte bound");
        requireTargetWriter();
        final CompletableFuture<MessageId> completion;
        try {
            final var recovering = recoveryWriter;
            completion = recovering == null
                    ? PulsarClientArtifactOwnedBackendGroup.nativeSend(producer, () -> producer.newMessage().value(payload).sendAsync())
                    : recovering.nativeSend(this, producer, () -> producer.newMessage().value(payload).sendAsync());
        } catch (RuntimeException failure) {
            throw new IllegalStateException("Attempt Journal append did not reach a durable acknowledgement", failure);
        }
        synchronized (lifecycle) { nativeAppends.add(completion); }
        completion.whenComplete((ignored, failure) -> { synchronized (lifecycle) { nativeAppends.remove(completion); } });
        final MessageId messageId;
        try {
            messageId = completion.get(responseTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting Attempt Journal acknowledgement", interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            final Optional<PulsarAttemptJournal.JournalPosition> recovered;
            try {
                recovered = recoverCommitted(payload);
            } catch (RuntimeException recoveryFailure) {
                failure.addSuppressed(recoveryFailure);
                throw new IllegalStateException("Attempt Journal append outcome is unknown", failure);
            }
            if (recovered.isPresent()) {
                requireTargetWriter();
                responseLossRecoveries++;
                return recovered.get();
            }
            throw new IllegalStateException("Attempt Journal append outcome is unknown", failure);
        }
        final var position = acknowledgedPosition(messageId);
        requireTargetWriter();
        return position;
    }

    /**
     * Resolves a committed-response loss from the same guarded, contiguous
     * replay subscription used at startup. Only the exact canonical Journal
     * payload can produce a durable position; absence at the captured tail
     * remains unknown because an in-flight send may still complete later.
     */
    private Optional<PulsarAttemptJournal.JournalPosition> recoverCommitted(final byte[] expectedPayload) {
        GuardedConsumer<byte[]> consumer = null;
        Throwable primary = null;
        try {
            requireTargetWriter();
            consumer = PulsarClientArtifactSourceConsumerFactory.create(client, expectedGuard, resource.physicalTopic(), replaySubscriptionName);
            PulsarClientArtifactRecoverySourcePositioner.awaitStableProof(
                    consumer, expectedGuard, resource.physicalTopic(), resource.partition(), responseTimeout);
            @SuppressWarnings("deprecation")
            final MessageId replayThrough = consumer.getLastMessageId();
            if (MessageId.earliest.equals(replayThrough)) {
                return Optional.empty();
            }
            PulsarAttemptJournal.JournalPosition exact = null;
            int scanned = 0;
            long scannedBytes = 0;
            while (true) {
                requireTargetWriter();
                final Message<byte[]> message = consumer.receive(responseTimeoutMs, TimeUnit.MILLISECONDS);
                if (message == null) {
                    throw new IllegalStateException("Attempt Journal readback ended before its captured tail");
                }
                final PulsarAttemptJournal.JournalPosition position = replayPosition(message);
                if (targetNamespace) {
                    scannedBytes = Math.addExact(scannedBytes, message.getData().length);
                    if (++scanned > maximumRecords || scannedBytes > maximumRecordBytes) {
                        throw new IllegalStateException("Target Journal readback completeness exceeds its budget");
                    }
                }
                // Decode every traversed record before acknowledging it so a
                // corrupt or foreign body cannot be skipped by readback.
                decode(message.getData(), position);
                if (exact == null && Arrays.equals(expectedPayload, message.getData())) {
                    exact = position;
                }
                consumer.acknowledge(message);
                if (message.getMessageId().compareTo(replayThrough) >= 0) {
                    return Optional.ofNullable(exact);
                }
            }
        } catch (PulsarClientException failure) {
            if (consumer == null) synchronized (lifecycle) { readerOpeningFailure = failure; }
            primary = failure;
            throw new IllegalStateException("Attempt Journal response-loss readback failed", failure);
        } catch (RuntimeException | Error failure) {
            if (consumer == null) synchronized (lifecycle) { readerOpeningFailure = failure; }
            primary = failure; throw failure;
        } finally {
            if (consumer != null) closeReaderPreserving(consumer, primary);
        }
    }

    @Override
    public void close() {
        synchronized (lifecycle) { stopping = true; }
        producerClose.close();
    }

    void requestManagedClose() {
        synchronized (lifecycle) { stopping = true; }
        journal.sealWriter(this);
        progressManagedClose();
    }
    void bindRecoveryWriter(PulsarClientArtifactRecoveryJournalWriter original) {
        synchronized (lifecycle) {
            if (stopping || recoveryWriter != null || !targetNamespace) throw new IllegalStateException("original recovery writer cannot be rebound");
            recoveryWriter = Objects.requireNonNull(original);
        }
    }
    boolean operationsExited() { synchronized (lifecycle) { return activeAppends == 0; } }
    boolean managedNativeClosed() {
        synchronized (lifecycle) {
            return stopping && activeAppends == 0 && (backendStopped || readerOpeningFailure == null
                    && nativeAppends.isEmpty() && readerCloses.isEmpty()
                    && managedProducerClose != null && managedProducerClose.isDone() && !managedProducerClose.isCompletedExceptionally());
        }
    }
    boolean normallyClosedForHandoff() {
        synchronized (lifecycle) {
            if (backendStopped) throw new IllegalStateException("shared SDK termination cannot authorize normal Journal handoff");
            if (readerOpeningFailure != null) throw new IllegalStateException("original Journal retains an uncertain reader opening", readerOpeningFailure);
            for (var close : readerCloses) {
                if (close.isCompletedExceptionally()) close.join();
            }
            if (managedProducerClose != null && managedProducerClose.isCompletedExceptionally()) managedProducerClose.join();
            return stopping && activeAppends == 0 && nativeAppends.isEmpty() && readerCloses.isEmpty()
                    && managedProducerClose != null && managedProducerClose.isDone();
        }
    }
    private void progressManagedClose() {
        synchronized (lifecycle) {
            if (!stopping || activeAppends != 0 || backendStopped || managedProducerClose != null) return;
            managedProducerClose = producerClose.normalCloseFuture();
        }
    }
    void backendTerminated(PulsarClientArtifactOwnedBackendGroup.TerminationProof proof) {
        proof.requireClient(client);
        synchronized (lifecycle) {
            if (activeAppends != 0) throw new IllegalStateException("original Journal append has not exited");
            stopping = true; backendStopped = true; readerCloses.clear();
        }
        journal.sealWriter(this);
    }
    private void closeReader(GuardedConsumer<byte[]> consumer) {
        final var closing = PulsarClientArtifactOwnedBackendGroup.closeConsumer(consumer);
        synchronized (lifecycle) { readerCloses.add(closing); }
        closing.whenComplete((ignored, failure) -> {
            if (failure == null) synchronized (lifecycle) { readerCloses.remove(closing); }
        });
        try { closing.get(responseTimeoutMs, TimeUnit.MILLISECONDS); }
        catch (InterruptedException failure) {
            Thread.currentThread().interrupt(); throw new IllegalStateException("original Journal reader close was interrupted", failure);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IllegalStateException("original Journal reader close is not confirmed", failure);
        }
    }
    private void closeReaderPreserving(GuardedConsumer<byte[]> consumer, Throwable primary) {
        try { closeReader(consumer); }
        catch (RuntimeException | Error cleanup) {
            if (primary == null) throw cleanup;
            if (cleanup != primary) primary.addSuppressed(cleanup);
        }
    }

    private int replay(final PulsarClient client, final String subscriptionName) throws PulsarClientException {
        final GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(
                client, expectedGuard, resource.physicalTopic(), subscriptionName);
        Throwable primary = null;
        try {
            PulsarClientArtifactRecoverySourcePositioner.awaitStableProof(
                    consumer, expectedGuard, resource.physicalTopic(), resource.partition(), responseTimeout);
            // InitialPosition.Earliest is only honored when a Pulsar
            // subscription is first created. This durable subscription may
            // already be ACKed to the tail by a prior process, so startup
            // reconstruction must explicitly rewind it. Response-loss
            // readback deliberately remains incremental and does not seek.
            consumer.seek(MessageId.earliest);
            PulsarClientArtifactRecoverySourcePositioner.awaitStableProof(
                    consumer, expectedGuard, resource.physicalTopic(), resource.partition(), responseTimeout);
            @SuppressWarnings("deprecation")
            final MessageId replayThrough = consumer.getLastMessageId();
            if (MessageId.earliest.equals(replayThrough)) {
                return 0;
            }
            int records = 0;
            long replayBytes = 0;
            while (true) {
                requireTargetWriter();
                final Message<byte[]> message = consumer.receive(responseTimeoutMs, TimeUnit.MILLISECONDS);
                if (message == null) {
                    throw new IllegalStateException("Attempt Journal replay ended before its captured tail");
                }
                if (targetNamespace) {
                    replayBytes = Math.addExact(replayBytes, message.getData().length);
                    if (records >= maximumRecords || replayBytes > maximumRecordBytes) {
                        throw new IllegalStateException("complete Target Journal replay exceeds its bounded scan");
                    }
                }
                final PulsarAttemptJournal.JournalPosition position = replayPosition(message);
                final var original = decode(message.getData(), position);
                if (original.kind() == PulsarAttemptJournal.RecordKind.RESOLVED_NOT_PUBLISHED) {
                    if (negativeReplayAuthority == null) {
                        throw new IllegalStateException("Target v2 recovery reader lacks Source/history authentication");
                    }
                    targetWriterGuard.requireRecordVersion(shard, resource, TargetPulsarAttemptJournalRecordCodec.SETTLEMENT_VERSION);
                    journal.replay(original, negativeReplayAuthority);
                } else { journal.replay(original); }
                consumer.acknowledge(message);
                records++;
                if (message.getMessageId().compareTo(replayThrough) >= 0) {
                    return records;
                }
            }
        } catch (PulsarClientException | RuntimeException | Error failure) {
            primary = failure; throw failure;
        } finally {
            closeReaderPreserving(consumer, primary);
        }
    }

    private PulsarAttemptJournal.JournalPosition acknowledgedPosition(final MessageId messageId) {
        if (!(messageId instanceof GuardedMessageId guarded)
                || !(messageId instanceof MessageIdAdv advanced)
                || advanced.getFirstChunkMessageId() != null
                || !expectedGuard.equals(guarded.resourceGuard())
                || !resource.physicalTopic().equals(guarded.physicalTopic())
                || guarded.partition() != resource.partition()) {
            throw new IllegalStateException("Attempt Journal acknowledgement lacks its exact resource proof");
        }
        final GuardedSendSuccessEvidence evidence = guarded.responseEvidence();
        final TopicResourceGuardAttestation attestation =
                new TopicResourceGuardAttestation(expectedGuard, resource.physicalTopic(), resource.partition());
        if (evidence == null
                || !attestation.equals(evidence.attestation())
                || evidence.ledgerId() != advanced.getLedgerId()
                || evidence.entryId() != advanced.getEntryId()
                || evidence.brokerEntryTimestamp() != guarded.brokerEntryTimestamp()) {
            throw new IllegalStateException("Attempt Journal acknowledgement evidence is incomplete or foreign");
        }
        return position(advanced, guarded.brokerEntryTimestamp(), "Attempt Journal acknowledgement");
    }

    private PulsarAttemptJournal.JournalRecord decode(
            final byte[] payload, final PulsarAttemptJournal.JournalPosition position) {
        return targetNamespace ? TargetPulsarAttemptJournalRecordCodec.decode(payload, position)
                : PulsarAttemptJournalRecordCodec.decode(payload, position);
    }

    /** Current Owner/Store/time and source-protected Target namespace; never acquire Shard/Store locks here. */
    @FunctionalInterface
    public interface TargetWriterGuard {
        void requireActive(ShardId shard, PulsarJournalResource resource);
        /** Existing schema/Artifact/eligible-reader activation; old functional guards cannot silently enable v2. */
        default void requireRecordVersion(ShardId shard, PulsarJournalResource resource, int recordVersion) {
            throw new IllegalStateException("Target Journal record version is not activated by this writer/reader guard");
        }

    }

    private void requireTargetWriter() {
        synchronized (lifecycle) {
            if (stopping) throw new IllegalStateException("original Journal writer is sealed");
        }
        if (targetNamespace) {
            Objects.requireNonNull(targetWriterGuard, "targetWriterGuard").requireActive(shard, resource);
        }
    }

    private PulsarAttemptJournal.JournalPosition replayPosition(final Message<byte[]> message) {
        if (!resource.physicalTopic().equals(message.getTopicName())
                || !(message.getMessageId() instanceof MessageIdAdv advanced)
                || advanced.getFirstChunkMessageId() != null) {
            throw new IllegalStateException("Attempt Journal replay record has no exact physical position");
        }
        final int partition = advanced.getPartitionIndex();
        if (partition >= 0 && partition != resource.partition()) {
            throw new IllegalStateException("Attempt Journal replay record belongs to another partition");
        }
        final long brokerTimestamp = message.getBrokerPublishTime()
                .orElseThrow(() -> new IllegalStateException("Attempt Journal replay lacks Broker persistence time"));
        return position(advanced, brokerTimestamp, "Attempt Journal replay");
    }

    private static PulsarAttemptJournal.JournalPosition position(
            final MessageIdAdv messageId, final long brokerTimestamp, final String operation) {
        if (messageId.getLedgerId() < 0 || messageId.getEntryId() < 0 || brokerTimestamp < 0) {
            throw new IllegalStateException(operation + " position is outside the supported domain");
        }
        final int rawBatchIndex = messageId.getBatchIndex();
        final int rawBatchSize = messageId.getBatchSize();
        final boolean batched = rawBatchIndex >= 0;
        final int batchIndex = batched ? rawBatchIndex : 0;
        final int batchSize = batched ? rawBatchSize : 1;
        if (batched && rawBatchSize <= 0 || batchSize <= 0 || Integer.compareUnsigned(batchIndex, batchSize) >= 0) {
            throw new IllegalStateException(operation + " batch position is invalid");
        }
        return new PulsarAttemptJournal.JournalPosition(
                messageId.getLedgerId(), messageId.getEntryId(), batchIndex, batchSize, brokerTimestamp);
    }

    private static Duration requirePositive(final Duration value) {
        final Duration exact = Objects.requireNonNull(value, "responseTimeout");
        if (exact.isNegative() || exact.isZero() || exact.toMillis() <= 0 || exact.toMillis() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("responseTimeout must fit a positive millisecond int");
        }
        return exact;
    }
}
