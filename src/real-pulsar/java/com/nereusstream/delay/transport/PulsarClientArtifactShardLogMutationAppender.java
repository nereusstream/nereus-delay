package com.nereusstream.delay.transport;

import com.nereusstream.delay.ownership.ShardLogMutationAppender;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SystemMutation;
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
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TopicResourceGuard;
import org.apache.pulsar.client.api.TopicResourceGuardAttestation;
import org.apache.pulsar.client.api.TopicResourceGuardException;

/**
 * Guarded P1 append authority for signed System Mutations on one Shard Log
 * topic.
 *
 * <p>The producer is resource guarded, while the source consumer supplies the
 * current guarded SUBSCRIBE connection proof required by the active Pulsar
 * barrier. A proof change during the send makes the append UNKNOWN even when
 * the broker accepted the bytes; recovery must then reconcile the exact
 * mutation from the Shard Log.</p>
 */
public final class PulsarClientArtifactShardLogMutationAppender implements ShardLogMutationAppender, AutoCloseable {
    private final Producer<byte[]> producer;
    private final GuardedConsumer<?> sourceConsumer;
    private final ShardId shard;
    private final TopicResourceGuard expectedGuard;
    private final String physicalTopic;
    private final int partition;
    private final Duration responseTimeout;

    public PulsarClientArtifactShardLogMutationAppender(
            final Producer<byte[]> producer,
            final GuardedConsumer<?> sourceConsumer,
            final ShardId shard,
            final String authenticatedClusterId,
            final byte[] resourceIncarnation,
            final String physicalTopic,
            final long physicalTopicCreationTimestamp,
            final Duration responseTimeout) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.sourceConsumer = Objects.requireNonNull(sourceConsumer, "sourceConsumer");
        this.shard = Objects.requireNonNull(shard, "shard");
        this.expectedGuard = new TopicResourceGuard(
                Objects.requireNonNull(authenticatedClusterId, "authenticatedClusterId"),
                resourceIncarnation,
                physicalTopicCreationTimestamp);
        this.physicalTopic = requireText(physicalTopic, "physicalTopic");
        this.partition = shard.partition();
        this.responseTimeout = Objects.requireNonNull(responseTimeout, "responseTimeout");
        if (responseTimeout.isZero() || responseTimeout.isNegative() || responseTimeout.toMillis() <= 0) {
            throw new IllegalArgumentException("responseTimeout must be positive");
        }
        if (!physicalTopic.equals(producer.getTopic())
                || !physicalTopic.equals(sourceConsumer.getTopic())
                || !expectedGuard.equals(sourceConsumer.resourceGuard())) {
            throw new IllegalArgumentException("Pulsar mutation binding is not pinned to one guarded topic");
        }
    }

    @Override
    public AppendOutcome append(final SystemMutation mutation) {
        final SystemMutation exact = Objects.requireNonNull(mutation, "mutation");
        if (!shard.equals(exact.shardId())) {
            throw new IllegalArgumentException("Pulsar Shard Log mutation belongs to another Shard");
        }
        final SourceProof before;
        try {
            before = sourceProof();
        } catch (RuntimeException failure) {
            return AppendOutcome.unknown();
        }
        final CompletableFuture<MessageId> completion;
        try {
            completion = producer.newMessage().value(exact.encodeFrame()).sendAsync();
        } catch (RuntimeException failure) {
            return failure(failure);
        }
        final MessageId messageId;
        try {
            messageId = completion.get(responseTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return AppendOutcome.unknown();
        } catch (ExecutionException execution) {
            return failure(execution.getCause());
        } catch (TimeoutException timeout) {
            return AppendOutcome.unknown();
        }
        final SourceProof after;
        try {
            after = sourceProof();
        } catch (RuntimeException failure) {
            return AppendOutcome.unknown();
        }
        if (!before.equals(after)) {
            return AppendOutcome.unknown();
        }
        return persisted(messageId, after);
    }

    @Override
    public void close() {
        try {
            producer.close();
        } catch (org.apache.pulsar.client.api.PulsarClientException failure) {
            throw new IllegalStateException("Pulsar guarded mutation producer close failed", failure);
        }
    }

    private AppendOutcome persisted(final MessageId messageId, final SourceProof sourceProof) {
        if (!(messageId instanceof GuardedMessageId guarded)
                || !(messageId instanceof MessageIdAdv advanced)
                || !expectedGuard.equals(guarded.resourceGuard())
                || !physicalTopic.equals(guarded.physicalTopic())
                || guarded.partition() != partition
                || guarded.brokerEntryTimestamp() < 0
                || guarded.responseEvidence() == null
                || advanced.getFirstChunkMessageId() != null) {
            return AppendOutcome.unknown();
        }
        final long ledgerId = advanced.getLedgerId();
        final long entryId = advanced.getEntryId();
        if (ledgerId < 0 || entryId < 0) {
            return AppendOutcome.unknown();
        }
        final int rawBatchIndex = advanced.getBatchIndex();
        final int rawBatchSize = advanced.getBatchSize();
        final boolean batched = rawBatchIndex >= 0;
        final int batchIndex = batched ? rawBatchIndex : 0;
        final int batchSize = batched ? rawBatchSize : 1;
        if (batched && rawBatchSize <= 0 || batchSize <= 0 || Integer.compareUnsigned(batchIndex, batchSize) >= 0) {
            return AppendOutcome.unknown();
        }
        final GuardedSendSuccessEvidence evidence = guarded.responseEvidence();
        final TopicResourceGuardAttestation expectedAttestation =
                new TopicResourceGuardAttestation(expectedGuard, physicalTopic, partition);
        if (!expectedAttestation.equals(evidence.attestation())
                || evidence.ledgerId() != ledgerId
                || evidence.entryId() != entryId
                || evidence.brokerEntryTimestamp() != guarded.brokerEntryTimestamp()) {
            return AppendOutcome.unknown();
        }
        final PulsarSourcePosition position = new PulsarSourcePosition(
                shard,
                expectedGuard.resourceIncarnation(),
                physicalTopic,
                ledgerId,
                entryId,
                batchIndex,
                batchSize,
                batched ? PulsarSourcePosition.EntryKind.BATCH : PulsarSourcePosition.EntryKind.NON_BATCH,
                guarded.brokerEntryTimestamp());
        return AppendOutcome.persisted(position, sourceProof.connectionGeneration(), sourceProof.digest());
    }

    private AppendOutcome failure(final Throwable failure) {
        final TopicResourceGuardException guardFailure = unwrap(failure);
        return guardFailure != null
                        && expectedGuard.equals(guardFailure.expectedGuard())
                        && guardFailure.definitelyNotPersisted()
                        && guardFailure.responseEvidence().isPresent()
                ? AppendOutcome.definitelyNotPersisted()
                : AppendOutcome.unknown();
    }

    private SourceProof sourceProof() {
        if (!expectedGuard.equals(sourceConsumer.resourceGuard()) || !physicalTopic.equals(sourceConsumer.getTopic())) {
            throw new IllegalStateException("Pulsar source consumer guard binding changed");
        }
        final Optional<TopicResourceGuardAttestation> attestation = sourceConsumer.resourceGuardAttestation();
        final long generation = sourceConsumer.connectionGeneration();
        if (attestation.isEmpty() || generation <= 0) {
            throw new IllegalStateException("Pulsar source consumer has no guarded connection proof");
        }
        final TopicResourceGuardAttestation exact = attestation.orElseThrow();
        if (!new TopicResourceGuardAttestation(expectedGuard, physicalTopic, partition).equals(exact)) {
            throw new IllegalStateException("Pulsar source consumer returned a foreign connection proof");
        }
        return new SourceProof(generation, PulsarClientArtifactSourceRecordConsumer.attestationDigest(exact));
    }

    private static TopicResourceGuardException unwrap(final Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            if (current instanceof TopicResourceGuardException guardFailure) {
                return guardFailure;
            }
            current = current.getCause();
        }
        return null;
    }

    private static String requireText(final String value, final String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be nonblank");
        }
        return value;
    }

    private record SourceProof(long connectionGeneration, byte[] digest) {
        private SourceProof {
            digest = digest.clone();
        }

        @Override
        public byte[] digest() {
            return digest.clone();
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof SourceProof that
                    && connectionGeneration == that.connectionGeneration
                    && Arrays.equals(digest, that.digest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectionGeneration, Arrays.hashCode(digest));
        }
    }
}
