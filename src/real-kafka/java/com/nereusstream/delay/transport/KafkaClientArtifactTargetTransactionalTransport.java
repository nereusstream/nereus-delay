package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight;
import com.nereusstream.delay.adapter.DestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPublishRequest;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.TargetOrdinaryPublishRequest;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetKafkaReceiptBinding;
import com.nereusstream.delay.protocol.TargetKafkaReceiptWire;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.producer.GuardedRecordMetadata;
import org.apache.kafka.clients.producer.GuardedTransactionalProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.Uuid;

/** Target v6 business+receipt SEND on one transaction; all blocking SDK work uses the caller's bounded I/O executor. */
public final class KafkaClientArtifactTargetTransactionalTransport implements DestinationPublishAdapter {
    private final GuardedTransactionalProducer<byte[], byte[]> producer;
    private final TargetChannelIdentity channel;
    private final TargetKafkaReceiptBinding receipt;
    private final Executor io;
    private final Map<String, Object> readerConfiguration;
    private final KafkaClientArtifactTargetReceiptReader.Protection protection;
    private final Duration operationTimeout;
    private final Duration pollTimeout;
    private final int maximumRecords;
    private final int maximumBytes;
    private final AtomicBoolean busy = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile KafkaClientArtifactTargetReceiptReader.Observation observation;
    private volatile Throwable firstFailure;
    private KafkaClientArtifactTargetTransactionalFactory.Fence fence;
    private final com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client producerBudget;
    private final java.util.function.Supplier<com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client>
            readerBudgets;
    private Pending pending;
    private boolean producerCloseCompleted;
    private Runnable credentialGate = () -> {};

    void installCredentialGate(Runnable gate) {
        credentialGate = Objects.requireNonNull(gate, "local material gate");
    }

    private static final class Pending {
        final CompletableFuture<DestinationPublishResult> result = new CompletableFuture<>();
        boolean started, exited;
        KafkaClientArtifactTargetReceiptReader reader;
        KafkaClientArtifactTargetCleanupFailure cleanup;
    }

    // Factory-only construction follows actual initTransactions and independent channel/resource authority.
    KafkaClientArtifactTargetTransactionalTransport(
            GuardedTransactionalProducer<byte[], byte[]> producer,
            TargetChannelIdentity channel,
            TargetKafkaReceiptBinding receipt,
            Executor io,
            Map<String, Object> readerConfiguration,
            KafkaClientArtifactTargetReceiptReader.Protection protection,
            Duration operationTimeout,
            Duration pollTimeout,
            int maximumRecords,
            int maximumBytes) {
        this(
                producer,
                channel,
                receipt,
                io,
                readerConfiguration,
                protection,
                operationTimeout,
                pollTimeout,
                maximumRecords,
                maximumBytes,
                null,
                null);
    }

    KafkaClientArtifactTargetTransactionalTransport(
            GuardedTransactionalProducer<byte[], byte[]> producer,
            TargetChannelIdentity channel,
            TargetKafkaReceiptBinding receipt,
            Executor io,
            Map<String, Object> readerConfiguration,
            KafkaClientArtifactTargetReceiptReader.Protection protection,
            Duration operationTimeout,
            Duration pollTimeout,
            int maximumRecords,
            int maximumBytes,
            com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client producerBudget,
            java.util.function.Supplier<com.nereusstream.delay.ownership.TargetDestinationSdkBudget.Client>
                    readerBudgets) {
        this.producerBudget = producerBudget;
        this.readerBudgets = readerBudgets;
        this.producer = Objects.requireNonNull(producer, "initialized guarded producer");
        this.channel = Objects.requireNonNull(channel, "activated channel");
        this.receipt = Objects.requireNonNull(receipt, "protected receipt resource");
        this.io = Objects.requireNonNull(io, "bounded I/O executor");
        this.readerConfiguration = Map.copyOf(readerConfiguration);
        this.protection = Objects.requireNonNull(protection, "receipt authority");
        this.operationTimeout = operationTimeout;
        this.pollTimeout = pollTimeout;
        this.maximumRecords = maximumRecords;
        this.maximumBytes = maximumBytes;
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(DestinationPublishRequest request) {
        return CompletableFuture.completedFuture(
                DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
    }

    @Override
    public CompletionStage<DestinationPublishResult> publishTargetOrdinary(
            TargetOrdinaryPublishRequest request, TargetOrdinaryPublishPreflight gate) {
        requireRequest(Objects.requireNonNull(request, "actual Source request"));
        Objects.requireNonNull(gate, "last ownership gate");
        final Pending accepted;
        synchronized (this) {
            if (closed.get() || fence != null || !busy.compareAndSet(false, true)) {
                return CompletableFuture.completedFuture(
                        DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
            }
            accepted = new Pending();
            pending = accepted;
        }
        try {
            io.execute(() -> send(request, gate, accepted));
        } catch (RuntimeException | Error rejected) {
            synchronized (this) {
                accepted.exited = true;
                busy.set(false);
            }
            throw rejected;
        }
        return accepted.result.copy();
    }

    private void requireRequest(TargetOrdinaryPublishRequest request) {
        final var mapping = Objects.requireNonNull(request.applied().kafkaMapping(), "actual v6 Source mapping");
        mapping.requireOriginal(request.applied().image(), request.source());
        if (request.publication().channel().context().kind() != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT
                || !Arrays.equals(
                        channel.context().producerIdentity(),
                        request.publication().channel().context().producerIdentity())
                || !Arrays.equals(
                        receipt.receiptResource().canonicalBytes(),
                        mapping.admission().kafkaReceipt().receiptResource().canonicalBytes())
                || !receipt.targetPhysicalTopic()
                        .equals(mapping.admission().kafkaReceipt().targetPhysicalTopic())
                || !receipt.receiptPhysicalTopic()
                        .equals(mapping.admission().kafkaReceipt().receiptPhysicalTopic())
                || receipt.receiptPartition()
                        != mapping.admission().kafkaReceipt().receiptPartition()
                || receipt.slotsPerShard() != mapping.admission().kafkaReceipt().slotsPerShard()) {
            throw new IllegalArgumentException(
                    "Target transaction changes its activated stable Producer or physical resources");
        }
        channel.requireExactFrozenIdentity(request.publication().channel());
        protection.requireCurrent(mapping, mapping.admission().kafkaReceipt().scanStartOffset());
    }

    private void send(TargetOrdinaryPublishRequest request, TargetOrdinaryPublishPreflight gate, Pending accepted) {
        synchronized (this) {
            if (accepted.exited) {
                return;
            }
            accepted.started = true;
        }
        boolean transaction = false;
        DestinationPublishResult outcome = null;
        try {
            if (closed.get()) {
                outcome = unknown();
                return;
            }
            requireRequest(request);
            observation = null;
            final var mapping = request.applied().kafkaMapping();
            final var binding = mapping.admission().kafkaReceipt();
            final var p = request.publication();
            final var targetGuard = new ProducerResourceGuard(
                    p.physical().resource().kafka().authenticatedClusterId(),
                    binding.targetPhysicalTopic(),
                    uuid(p.physical().resource().kafka().nativeTopicUuid()),
                    Math.toIntExact(p.physical().physicalPartition()));
            final var receiptGuard = new ProducerResourceGuard(
                    binding.receiptResource().authenticatedClusterId(),
                    binding.receiptPhysicalTopic(),
                    uuid(binding.receiptResource().receiptTopicUuid()),
                    binding.receiptPartition());
            final var business =
                    KafkaClientArtifactTargetRecordEncoder.configure(binding.targetPhysicalTopic(), request);
            final var record = new ProducerRecord<>(
                    binding.receiptPhysicalTopic(),
                    binding.receiptPartition(),
                    null,
                    TargetKafkaReceiptWire.key(mapping),
                    TargetKafkaReceiptWire.value(mapping));
            var denied = gate.check(request);
            if (denied != null) {
                outcome = denied;
                return;
            }
            credentialGate.run();
            producer.beginTransaction();
            transaction = true;
            denied = gate.check(request);
            if (denied != null) {
                producer.abortTransaction();
                outcome = denied;
                return;
            }
            final long deadline = System.nanoTime() + operationTimeout.toNanos();
            credentialGate.run();
            final var target = producer.sendGuardedInTransaction(business, targetGuard);
            denied = gate.check(request);
            if (denied != null) {
                producer.abortTransaction();
                outcome = unknown();
                return;
            }
            credentialGate.run();
            final var receiptSend = producer.sendGuardedInTransaction(record, receiptGuard);
            final var targetAck = await(target, deadline);
            final var receiptAck = await(receiptSend, deadline);
            requireAck(targetAck, targetGuard, p.payloadSha256());
            requireAck(receiptAck, receiptGuard, Bytes.sha256(record.value()));
            if (receiptAck.recordMetadata().offset() < binding.scanStartOffset()) {
                throw new IllegalStateException("K2 receipt precedes the original protected range");
            }
            requireRequest(request);
            producer.commitTransaction();
            transaction = false;
            // The callbacks and successful EndTxn alone do not prove committed visibility.
            // A new reader searches from durable Source mapping, never from callback receiptOffset.
            KafkaClientArtifactTargetReceiptReader.Observation published = null;
            final var reserved = readerBudgets == null ? null : readerBudgets.get();
            try {
                accepted.reader = new KafkaClientArtifactTargetReceiptReader(
                        readerConfiguration,
                        mapping,
                        protection,
                        pollTimeout,
                        maximumRecords,
                        maximumBytes,
                        false,
                        reserved);
            } catch (RuntimeException | Error failure) {
                if (reserved != null && !reserved.released()) {
                    try {
                        reserved.releaseUnstarted();
                    } catch (IllegalStateException held) {
                        /* started client retains its native cleanup handle */
                    }
                }
                throw failure;
            }
            final var reader = accepted.reader;
            try {
                final long readDeadline = System.nanoTime() + operationTimeout.toNanos();
                while (System.nanoTime() < readDeadline) {
                    final var turn = reader.runTurn();
                    if (turn.status() == KafkaClientArtifactTargetReceiptReader.Status.PUBLISHED) {
                        final var actual = turn.observation();
                        if (actual.brokerPersistenceTime()
                                != receiptAck.responseEvidence().logAppendTimeMs()) {
                            throw new IllegalStateException(
                                    "Target receipt read differs from its actual guarded ACK time");
                        }
                        published = actual;
                        break;
                    }
                }
            } finally {
                reader.close();
                accepted.reader = null;
            }
            if (published == null) {
                outcome = unknown();
            } else {
                observation = published;
                protection.requireCurrent(mapping, published.nextOffset());
                outcome = DestinationPublishResult.published(
                        p.physical().resource(),
                        Math.toIntExact(p.physical().physicalPartition()),
                        p.publishAttemptId(),
                        targetAck.responseEvidence().logAppendTimeMs(),
                        published.evidence().canonicalBytes());
            }
        } catch (Exception | Error failure) {
            firstFailure = failure;
            if (failure instanceof KafkaClientArtifactTargetCleanupFailure held) {
                accepted.cleanup = held;
            }
            // A partial SEND, failed abort or lost EndTxn response retains Source mapping/slot and becomes UNKNOWN.
            // Close the actual SDK before reporting physical completion. Failed teardown leaves the stage unresolved.
            if (transaction) {
                try {
                    producer.abortTransaction();
                } catch (RuntimeException | Error abortFailure) {
                    failure.addSuppressed(abortFailure);
                }
            }
            try {
                closeProducer();
            } catch (RuntimeException | Error closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            outcome = unknown();
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (failure instanceof Error fatal) {
                throw fatal;
            }
        } finally {
            if (accepted.reader != null) {
                try {
                    accepted.reader.close();
                    accepted.reader = null;
                } catch (RuntimeException | Error held) {
                    if (firstFailure == null) {
                        firstFailure = held;
                    }
                }
            }
            synchronized (this) {
                accepted.exited = true;
                busy.set(false);
            }
            if (outcome != null
                    && accepted.reader == null
                    && accepted.cleanup == null
                    && (firstFailure == null || producerCloseCompleted)) {
                accepted.result.complete(outcome);
            }
        }
    }

    /** Actual private guarded read for the independent Outcome/Artifact/retention authority. */
    public KafkaClientArtifactTargetReceiptReader.Observation requireObservation(
            TargetOrdinaryPublishRequest request, DestinationPublishResult result) {
        final var actual = Objects.requireNonNull(observation, "actual committed receipt observation");
        if (!Arrays.equals(
                        actual.mapping().canonicalBytes(),
                        request.applied().kafkaMapping().canonicalBytes())
                || result.disposition() != DestinationPublishResult.Disposition.PUBLISHED) {
            throw new IllegalArgumentException("Outcome does not refer to the actual transaction's retained mapping");
        }
        actual.requireEvidence(com.nereusstream.delay.protocol.PublishEvidence.decode(result.evidence()));
        protection.requireCurrent(actual.mapping(), actual.nextOffset());
        return actual;
    }

    public Optional<Throwable> firstFailure() {
        return Optional.ofNullable(firstFailure);
    }

    /** A same-ID recovery fence permanently seals this resource against business SEND. */
    synchronized void installFence(KafkaClientArtifactTargetTransactionalFactory.Fence actual) {
        if (fence != null || closed.get() || busy.get()) {
            throw new IllegalStateException("Fence cannot replace an active resource");
        }
        fence = Objects.requireNonNull(actual, "actual same-ID fencing observation");
    }

    public synchronized Optional<KafkaClientArtifactTargetTransactionalFactory.Fence> fence() {
        if (closed.get()) {
            throw new IllegalStateException("fencing producer is closed");
        }
        return Optional.ofNullable(fence);
    }

    private static GuardedRecordMetadata await(Future<GuardedRecordMetadata> future, long deadline) throws Exception {
        final long remaining = deadline - System.nanoTime();
        if (remaining <= 0) {
            throw new java.util.concurrent.TimeoutException("Target transaction callback deadline");
        }
        return future.get(remaining, TimeUnit.NANOSECONDS);
    }

    private static void requireAck(GuardedRecordMetadata metadata, ProducerResourceGuard guard, byte[] valueHash) {
        if (metadata == null || metadata.recordMetadata() == null || metadata.responseEvidence() == null) {
            throw new IllegalStateException("Target transaction lacks an actual guarded callback");
        }
        final var e = metadata.responseEvidence();
        final var m = metadata.recordMetadata();
        if (!guard.equals(metadata.resourceGuard())
                || e.errorCode() != 0
                || e.requestVersion() < 13
                || !guard.authenticatedClusterId().equals(e.authenticatedClusterId())
                || !guard.canonicalTopic().equals(e.canonicalTopic())
                || !guard.expectedTopicId().equals(e.expectedTopicId())
                || guard.partition() != e.partition()
                || !guard.canonicalTopic().equals(m.topic())
                || guard.partition() != m.partition()
                || m.offset() < 0
                || e.baseOffset() < 0
                || m.offset() != Math.addExact(e.baseOffset(), e.selectedBatchRecordIndex())
                || e.logAppendTimeMs() < 0
                || !Bytes.constantTimeEquals(valueHash, e.selectedRecordValueSha256())) {
            throw new IllegalStateException("Target transaction callback changes exact resource/offset/record bytes");
        }
    }

    private static Uuid uuid(java.util.UUID id) {
        return new Uuid(id.getMostSignificantBits(), id.getLeastSignificantBits());
    }

    private static DestinationPublishResult unknown() {
        return DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null);
    }

    private synchronized void closeProducer() {
        closed.set(true);
        if (!producerCloseCompleted) {
            KafkaClientArtifactProducerCleanup.close(producer, operationTimeout);
            producerCloseCompleted = true;
        }
        if (producerBudget != null) {
            producerBudget.closed();
        }
    }
    /** Close fences queued work; it never marks a running consumer or its physical stage complete early. */
    @Override
    public void close() {
        final Pending retained;
        synchronized (this) {
            closed.set(true);
            retained = pending;
            if (retained != null && !retained.started && !retained.exited) {
                retained.exited = true;
                busy.set(false);
                retained.result.complete(unknown());
            }
        }
        closeProducer();
        if (retained != null) {
            synchronized (this) {
                if (!retained.exited) {
                    throw new IllegalStateException("Target transaction I/O is still running");
                }
            }
            if (retained.reader != null) {
                retained.reader.close();
                retained.reader = null;
            }
            if (retained.cleanup != null) {
                retained.cleanup.retryClose();
                retained.cleanup = null;
            }
            retained.result.complete(unknown());
        }
    }

    public synchronized boolean closeCompleted() {
        return producerCloseCompleted
                && (producerBudget == null || producerBudget.released())
                && (pending == null || pending.exited && pending.reader == null && pending.cleanup == null);
    }
}
