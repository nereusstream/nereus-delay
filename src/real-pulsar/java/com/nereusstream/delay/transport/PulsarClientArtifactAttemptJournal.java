package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.PulsarAttemptJournal;
import com.nereusstream.delay.adapter.PulsarAttemptJournalRecordCodec;
import com.nereusstream.delay.adapter.PulsarJournalResource;
import com.nereusstream.delay.protocol.ShardId;
import java.time.Duration;
import java.util.Objects;
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
    private final Producer<byte[]> producer;
    private final PulsarJournalResource resource;
    private final TopicResourceGuard expectedGuard;
    private final Duration responseTimeout;
    private final int responseTimeoutMs;
    private final PulsarAttemptJournal journal;
    private final int replayedRecords;

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
        final PulsarJournalResource exactResource = Objects.requireNonNull(resource, "resource");
        final Producer<byte[]> producer = PulsarClientArtifactProducerFactory.create(
                client,
                exactResource.authenticatedClusterId(),
                exactResource.resourceIncarnation(),
                exactResource.physicalTopic(),
                exactResource.physicalTopicCreationTimestamp(),
                journalProducerName);
        try {
            return new PulsarClientArtifactAttemptJournal(
                    client,
                    producer,
                    Objects.requireNonNull(shard, "shard"),
                    exactResource,
                    replaySubscriptionName,
                    responseTimeout);
        } catch (RuntimeException | PulsarClientException failure) {
            try {
                producer.close();
            } catch (PulsarClientException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private PulsarClientArtifactAttemptJournal(
            final PulsarClient client,
            final Producer<byte[]> producer,
            final ShardId shard,
            final PulsarJournalResource resource,
            final String replaySubscriptionName,
            final Duration responseTimeout)
            throws PulsarClientException {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.resource = Objects.requireNonNull(resource, "resource");
        this.expectedGuard = new TopicResourceGuard(
                resource.authenticatedClusterId(),
                resource.resourceIncarnation(),
                resource.physicalTopicCreationTimestamp());
        this.responseTimeout = requirePositive(responseTimeout);
        this.responseTimeoutMs = Math.toIntExact(this.responseTimeout.toMillis());
        if (!resource.physicalTopic().equals(producer.getTopic()) || resource.partition() != shard.partition()) {
            throw new IllegalArgumentException("Attempt Journal producer/resource/shard binding differs");
        }
        this.journal = new PulsarAttemptJournal(shard, this, resource);
        this.replayedRecords = replay(
                Objects.requireNonNull(client, "client"),
                Objects.requireNonNull(replaySubscriptionName, "replaySubscriptionName"));
    }

    public PulsarAttemptJournal journal() {
        return journal;
    }

    public int replayedRecords() {
        return replayedRecords;
    }

    @Override
    public PulsarAttemptJournal.JournalPosition append(final PulsarAttemptJournal.AppendRequest request) {
        final byte[] payload = PulsarAttemptJournalRecordCodec.encode(Objects.requireNonNull(request, "request"));
        final CompletableFuture<MessageId> completion;
        try {
            completion = producer.newMessage().value(payload).sendAsync();
        } catch (RuntimeException failure) {
            throw new IllegalStateException("Attempt Journal append did not reach a durable acknowledgement", failure);
        }
        final MessageId messageId;
        try {
            messageId = completion.get(responseTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while awaiting Attempt Journal acknowledgement", interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IllegalStateException("Attempt Journal append outcome is unknown", failure);
        }
        return acknowledgedPosition(messageId);
    }

    @Override
    public void close() {
        try {
            producer.close();
        } catch (PulsarClientException failure) {
            throw new IllegalStateException("Attempt Journal producer close failed", failure);
        }
    }

    private int replay(final PulsarClient client, final String subscriptionName) throws PulsarClientException {
        final GuardedConsumer<byte[]> consumer = PulsarClientArtifactSourceConsumerFactory.create(
                client, expectedGuard, resource.physicalTopic(), subscriptionName);
        try {
            PulsarClientArtifactRecoverySourcePositioner.awaitStableProof(
                    consumer, expectedGuard, resource.physicalTopic(), resource.partition(), responseTimeout);
            @SuppressWarnings("deprecation")
            final MessageId replayThrough = consumer.getLastMessageId();
            if (MessageId.earliest.equals(replayThrough)) {
                return 0;
            }
            int records = 0;
            while (true) {
                final Message<byte[]> message = consumer.receive(responseTimeoutMs, TimeUnit.MILLISECONDS);
                if (message == null) {
                    throw new IllegalStateException("Attempt Journal replay ended before its captured tail");
                }
                final PulsarAttemptJournal.JournalPosition position = replayPosition(message);
                journal.replay(PulsarAttemptJournalRecordCodec.decode(message.getData(), position));
                consumer.acknowledge(message);
                records++;
                if (message.getMessageId().compareTo(replayThrough) >= 0) {
                    return records;
                }
            }
        } finally {
            consumer.close();
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
