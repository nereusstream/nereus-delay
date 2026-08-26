package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.SourceAcknowledgement;
import com.nereusstream.delay.ownership.SourceRecordConsumer;
import com.nereusstream.delay.ownership.SourceReplayEntry;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.ShardId;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.GuardedConsumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;

/**
 * Source-set Pulsar binding for one physical Shard Log topic.
 *
 * <p>The native cursor is advanced only by a synchronous acknowledgement with
 * broker acknowledgement receipts enabled on the consumer. The adapter
 * carries the exact guarded SUBSCRIBE proof into every source replay entry;
 * a changed connection generation is therefore an activation-boundary change,
 * not a transparent client-side detail.</p>
 */
public final class PulsarClientArtifactSourceRecordConsumer implements SourceRecordConsumer {
    private final GuardedConsumer<byte[]> consumer;
    private final TopicResourceGuard expectedGuard;
    private final ShardId shard;
    private final String physicalTopic;
    private final int receiveTimeoutMs;
    private Message<byte[]> buffered;
    private SourceConnectionProof bufferedProof;
    private Message<byte[]> inFlight;
    private boolean closed;

    public PulsarClientArtifactSourceRecordConsumer(
            final GuardedConsumer<byte[]> consumer,
            final TopicResourceGuard expectedGuard,
            final ShardId shard,
            final String physicalTopic,
            final Duration receiveTimeout) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.expectedGuard = Objects.requireNonNull(expectedGuard, "expectedGuard");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.physicalTopic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        Objects.requireNonNull(receiveTimeout, "receiveTimeout");
        final long timeoutMs = receiveTimeout.toMillis();
        if (timeoutMs <= 0 || timeoutMs > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("receiveTimeout must fit a positive millisecond int");
        }
        this.receiveTimeoutMs = (int) timeoutMs;
        if (!expectedGuard.equals(consumer.resourceGuard()) || !physicalTopic.equals(consumer.getTopic())) {
            throw new IllegalArgumentException("guarded source consumer is bound to a different topic identity");
        }
        requireProof();
    }

    @Override
    public synchronized Optional<PolledSourceRecord> poll() {
        ensureOpen();
        if (inFlight != null) {
            throw new IllegalStateException("previous Pulsar source record has not been ACKED");
        }
        final SourceConnectionProof beforeReceive = currentProofOrEmpty().orElse(null);
        if (beforeReceive == null) {
            return Optional.empty();
        }
        final Message<byte[]> message;
        if (buffered != null) {
            if (!beforeReceive.equals(bufferedProof)) {
                // The old connection was closed before this unacknowledged message
                // could be committed. Drop the local handle and let the broker
                // redeliver it on the new guarded SUBSCRIBE connection.
                buffered = null;
                bufferedProof = null;
                return Optional.empty();
            }
            message = buffered;
        } else {
            try {
                message = consumer.receive(receiveTimeoutMs, TimeUnit.MILLISECONDS);
            } catch (PulsarClientException failure) {
                if (currentProofOrEmpty().isEmpty()) {
                    return Optional.empty();
                }
                throw new IllegalStateException("Pulsar guarded source receive failed", failure);
            }
        }
        if (message == null) {
            return Optional.empty();
        }
        try {
            final SourceConnectionProof afterReceive = currentProofOrEmpty().orElse(null);
            if (afterReceive == null || !beforeReceive.equals(afterReceive)) {
                // A connection switch may race with receive(). The message has
                // not been ACKed, so its replay on the new guarded connection is
                // the only safe authority; do not carry an old proof across it.
                buffered = null;
                bufferedProof = null;
                return Optional.empty();
            }
            final SourceReplayEntry entry = decodeReplayRecord(
                    message,
                    shard,
                    physicalTopic,
                    afterReceive.attestation(),
                    afterReceive.generation(),
                    afterReceive.digest());
            buffered = null;
            bufferedProof = null;
            inFlight = message;
            return Optional.of(new PolledSourceRecord(
                    entry, (candidate, ignoredOutcome) -> acknowledge(message, entry, candidate)));
        } catch (RuntimeException | Error failure) {
            // The record was removed from the client receive path but has not
            // been ACKed. Keep it as the only retry authority in this adapter.
            buffered = message;
            bufferedProof = beforeReceive;
            throw failure;
        }
    }

    @Override
    public synchronized void close() {
        if (inFlight != null || buffered != null) {
            throw new IllegalStateException("cannot close Pulsar source with a pending source record");
        }
        if (!closed) {
            closed = true;
            try {
                consumer.close();
            } catch (PulsarClientException failure) {
                throw new IllegalStateException("Pulsar guarded source close failed", failure);
            }
        }
    }

    /** Canonical digest carried by Pulsar source connection proofs. */
    public static byte[] attestationDigest(final TopicResourceGuardAttestation attestation) {
        Objects.requireNonNull(attestation, "attestation");
        return Bytes.sha256(
                Bytes.u32be(attestation.guardVersion()),
                Bytes.lp32(Bytes.utf8(attestation.authenticatedClusterId())),
                Bytes.lp32(attestation.resourceIncarnation()),
                Bytes.u64beBits(attestation.topicCreationTimestamp()),
                Bytes.lp32(Bytes.utf8(attestation.physicalTopic())),
                Bytes.u32be(attestation.partition()));
    }

    private SourceAcknowledgement.AcknowledgementResult acknowledge(
            final Message<byte[]> message,
            final SourceReplayEntry expected,
            final com.nereusstream.delay.ownership.SourceReplayEntry candidate) {
        synchronized (this) {
            if (candidate != expected || inFlight != message || closed) {
                return SourceAcknowledgement.AcknowledgementResult.unknown(
                        new IllegalStateException("Pulsar source ACK entry identity changed"));
            }
        }
        try {
            consumer.acknowledge(message);
            synchronized (this) {
                if (inFlight != message) {
                    return SourceAcknowledgement.AcknowledgementResult.unknown(
                            new IllegalStateException("Pulsar source ACK state changed"));
                }
                inFlight = null;
            }
            return SourceAcknowledgement.AcknowledgementResult.acked();
        } catch (PulsarClientException | RuntimeException | Error failure) {
            return SourceAcknowledgement.AcknowledgementResult.unknown(failure);
        }
    }

    /** Decodes one guarded message for both active ACK and recovery replay paths. */
    static SourceReplayEntry decodeReplayRecord(
            final Message<byte[]> message,
            final ShardId shard,
            final String physicalTopic,
            final TopicResourceGuardAttestation attestation,
            final long generation,
            final byte[] digest) {
        if (!physicalTopic.equals(message.getTopicName())) {
            throw new IllegalArgumentException("Pulsar source message belongs to another physical topic");
        }
        return PulsarClientArtifactSourceRecordDecoder.decode(
                requireData(message), shard, position(message, shard, physicalTopic, attestation), generation, digest);
    }

    private static PulsarSourcePosition position(
            final Message<byte[]> message,
            final ShardId shard,
            final String physicalTopic,
            final TopicResourceGuardAttestation attestation) {
        if (!(message.getMessageId() instanceof MessageIdAdv messageId)) {
            throw new IllegalArgumentException("Pulsar source message lacks an advanced message id");
        }
        if (messageId.getFirstChunkMessageId() != null) {
            throw new IllegalArgumentException("chunked Pulsar source messages are not positions");
        }
        final long ledgerId = messageId.getLedgerId();
        final long entryId = messageId.getEntryId();
        if (ledgerId < 0 || entryId < 0) {
            throw new IllegalArgumentException("Pulsar source message lacks a bounded ledger position");
        }
        final int expectedPartition = Math.max(0, shard.partition());
        if (attestation.partition() != expectedPartition) {
            throw new IllegalArgumentException("Pulsar source attestation partition mismatch");
        }
        final int partitionIndex = messageId.getPartitionIndex();
        if (partitionIndex >= 0 && partitionIndex != shard.partition()) {
            throw new IllegalArgumentException("Pulsar source message partition mismatch");
        }
        final int rawBatchIndex = messageId.getBatchIndex();
        final int rawBatchSize = messageId.getBatchSize();
        final boolean batched = rawBatchIndex >= 0;
        final int batchIndex = batched ? rawBatchIndex : 0;
        final int batchSize = batched ? rawBatchSize : 1;
        if (batched && rawBatchSize <= 0) {
            throw new IllegalArgumentException("Pulsar batch source position lacks its batch size");
        }
        if (batchSize <= 0 || Integer.compareUnsigned(batchIndex, batchSize) >= 0) {
            throw new IllegalArgumentException("Pulsar source batch position is out of range");
        }
        final long brokerEntryTimestamp = message.getBrokerPublishTime()
                .orElseThrow(() -> new IllegalArgumentException("Pulsar source lacks Broker entry timestamp"));
        if (brokerEntryTimestamp < 0) {
            throw new IllegalArgumentException("Pulsar source Broker entry timestamp is negative");
        }
        return new PulsarSourcePosition(
                shard,
                attestation.resourceIncarnation(),
                physicalTopic,
                ledgerId,
                entryId,
                batchIndex,
                batchSize,
                batched ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH,
                brokerEntryTimestamp);
    }

    private SourceConnectionProof requireProof() {
        return currentProofOrEmpty()
                .orElseThrow(() -> new IllegalStateException("Pulsar guarded source has no current connection proof"));
    }

    private Optional<SourceConnectionProof> currentProofOrEmpty() {
        final Optional<TopicResourceGuardAttestation> attestation = consumer.resourceGuardAttestation();
        final long generation = consumer.connectionGeneration();
        if (attestation.isEmpty() || generation == 0) {
            return Optional.empty();
        }
        final TopicResourceGuardAttestation current = attestation.get();
        final TopicResourceGuard observed = new TopicResourceGuard(
                current.authenticatedClusterId(), current.resourceIncarnation(), current.topicCreationTimestamp());
        if (!expectedGuard.equals(observed)
                || !physicalTopic.equals(current.physicalTopic())
                || current.partition() != Math.max(0, shard.partition())) {
            throw new IllegalStateException("Pulsar guarded source returned foreign connection proof");
        }
        return Optional.of(new SourceConnectionProof(generation, current, attestationDigest(current)));
    }

    static byte[] requireData(final Message<byte[]> message) {
        final byte[] data = message.getData();
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Pulsar source message has no NDL1 payload");
        }
        return data;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Pulsar source consumer is closed");
        }
    }

    private record SourceConnectionProof(long generation, TopicResourceGuardAttestation attestation, byte[] digest) {
        private SourceConnectionProof {
            digest = Bytes.copy(digest);
        }

        @Override
        public byte[] digest() {
            return Bytes.copy(digest);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof SourceConnectionProof that
                    && generation == that.generation
                    && attestation.equals(that.attestation)
                    && Arrays.equals(digest, that.digest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generation, attestation, Arrays.hashCode(digest));
        }
    }
}
