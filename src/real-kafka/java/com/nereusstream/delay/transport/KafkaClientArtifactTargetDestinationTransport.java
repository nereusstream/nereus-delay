package com.nereusstream.delay.transport;

import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter.TargetOrdinaryPublishPreflight;
import com.nereusstream.delay.adapter.DestinationPublishResult;
import com.nereusstream.delay.adapter.KafkaDestinationRequest;
import com.nereusstream.delay.adapter.KafkaTargetResource;
import com.nereusstream.delay.adapter.PinnedKafkaDestinationAdapter;
import com.nereusstream.delay.adapter.TargetOrdinaryPublishRequest;
import com.nereusstream.delay.ownership.TargetDestinationSdkBudget;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ChannelKind;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.GuardedRecordMetadata;
import org.apache.kafka.clients.producer.ProducerResourceGuard;
import org.apache.kafka.common.Uuid;

/** Actual K1 baseline Target SEND, with complete frozen metadata and the final ownership gate. */
public final class KafkaClientArtifactTargetDestinationTransport
        implements PinnedKafkaDestinationAdapter.KafkaDestinationTransport {
    private final GuardedProducer<byte[], byte[]> producer;
    private final KafkaTargetResource resource;
    private final String physicalTopic;
    private final TargetChannelIdentity.Context context;
    private final ProducerResourceGuard guard;
    private final Duration closeTimeout;
    private final TargetDestinationSdkBudget.Client producerBudget;
    private boolean closing;
    private boolean nativeClosed;
    private final Object closeLock = new Object();

    public KafkaClientArtifactTargetDestinationTransport(final GuardedProducer<byte[], byte[]> producer,
            final KafkaTargetResource resource, final String physicalTopic,
            final TargetChannelIdentity.Context context) {
        this(producer, resource, physicalTopic, context, Duration.ofSeconds(30), null);
    }

    public KafkaClientArtifactTargetDestinationTransport(final GuardedProducer<byte[], byte[]> producer,
            final KafkaTargetResource resource, final String physicalTopic,
            final TargetChannelIdentity.Context context, final Duration closeTimeout,
            final TargetDestinationSdkBudget.Client producerBudget) {
        this.producer = Objects.requireNonNull(producer, "producer");
        this.resource = Objects.requireNonNull(resource, "resource");
        this.physicalTopic = Objects.requireNonNull(physicalTopic, "physicalTopic");
        this.context = Objects.requireNonNull(context, "context");
        this.closeTimeout = Objects.requireNonNull(closeTimeout, "bounded native close timeout");
        this.producerBudget = producerBudget;
        if (closeTimeout.toMillis() <= 0 || closeTimeout.toMillis() > 60_000) {
            throw new IllegalArgumentException("K1 native close timeout must be finite");
        }
        if (context.kind() != ChannelKind.BASELINE_PRODUCER || resource.partition() < 0) {
            throw new IllegalArgumentException("ordinary Kafka transport requires its baseline Target context");
        }
        guard = new ProducerResourceGuard(resource.authenticatedClusterId(), physicalTopic,
                new Uuid(resource.nativeTopicUuid().getMostSignificantBits(),
                        resource.nativeTopicUuid().getLeastSignificantBits()), resource.partition());
    }

    @Override
    public CompletionStage<DestinationPublishResult> publish(final KafkaDestinationRequest request) {
        return CompletableFuture.completedFuture(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
    }

    @Override
    public synchronized CompletionStage<DestinationPublishResult> publishTargetOrdinary(
            final TargetOrdinaryPublishRequest request, final TargetOrdinaryPublishPreflight gate) {
        if (closing) {
            return CompletableFuture.completedFuture(DestinationPublishResult.unknown(StableCode.CAPABILITY_UNAVAILABLE, null));
        }
        final var p = Objects.requireNonNull(request, "request").publication();
        final var target = p.physical().resource();
        if (target.kind() != com.nereusstream.delay.protocol.BrokerResourceIdentity.Kind.KAFKA
                || !target.kafka().authenticatedClusterId().equals(resource.authenticatedClusterId())
                || !target.kafka().nativeTopicUuid().equals(resource.nativeTopicUuid())
                || p.physical().physicalPartition() != resource.partition()
                || p.channel().context().kind() != ChannelKind.BASELINE_PRODUCER
                || !java.util.Arrays.equals(context.producerIdentity(), p.channel().context().producerIdentity())) {
            return CompletableFuture.completedFuture(DestinationPublishResult.unknown(StableCode.PREPARED_SUBMISSION_MISMATCH, null));
        }
        final var record = KafkaClientArtifactTargetRecordEncoder.configure(physicalTopic, request);
        final var denied = Objects.requireNonNull(gate, "ownership gate").check(request);
        if (denied != null) { return CompletableFuture.completedFuture(denied); }
        final CompletableFuture<DestinationPublishResult> result = new CompletableFuture<>();
        // Failure thrown by sendGuarded is not a completion observation; the bounded adapter retains its charge.
        producer.sendGuarded(record, guard, (metadata, failure) -> result.complete(failure == null
                ? success(request, metadata)
                : DestinationPublishResult.unknown(StableCode.DESTINATION_OUTCOME_UNKNOWN, null)));
        return result;
    }

    private DestinationPublishResult success(final TargetOrdinaryPublishRequest request,
            final GuardedRecordMetadata metadata) {
        if (metadata == null || metadata.recordMetadata() == null || metadata.responseEvidence() == null) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        final var e = metadata.responseEvidence();
        final var m = metadata.recordMetadata();
        if (!resource.authenticatedClusterId().equals(e.authenticatedClusterId())
                || !physicalTopic.equals(e.canonicalTopic())
                || !guard.expectedTopicId().equals(e.expectedTopicId())
                || resource.partition() != e.partition() || e.errorCode() != 0
                || !physicalTopic.equals(m.topic()) || m.partition() != resource.partition()
                || m.offset() < 0 || e.baseOffset() < 0
                || m.offset() != e.baseOffset() + e.selectedBatchRecordIndex()
                || e.logAppendTimeMs() < 0
                || !Bytes.constantTimeEquals(request.publication().payloadSha256(), e.selectedRecordValueSha256())) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
        try {
            final var p = request.publication();
            final var branch = CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, p.physical().resource().canonicalBytes());
                CanonicalProtobuf.uint32(out, 2, resource.partition());
                CanonicalProtobuf.uint64(out, 3, m.offset());
                if (e.responseLeaderEpoch().isPresent()) {
                    CanonicalProtobuf.uint32(out, 4, e.responseLeaderEpoch().getAsInt());
                }
                CanonicalProtobuf.int64(out, 5, e.logAppendTimeMs());
                CanonicalProtobuf.bytes(out, 6, ExternalDeliveryIdentity.publishAttempt(p.publishAttemptId()).canonicalBytes());
                CanonicalProtobuf.bytes(out, 7, p.preparedPublishHash());
                CanonicalProtobuf.bytes(out, 8, e.produceResponseBodySha256());
            });
            final var typed = PublishEvidence.create(PublishEvidenceKind.KAFKA_PRODUCE_ACK,
                    EvidenceVerificationStatus.VERIFIED_PUBLISHED, branch);
            typed.requireOrdinaryTargetPublishedBinding(p);
            return DestinationPublishResult.published(p.physical().resource(), resource.partition(),
                    p.publishAttemptId(), e.logAppendTimeMs(), typed.canonicalBytes());
        } catch (RuntimeException invalidEvidence) {
            return DestinationPublishResult.unknown(StableCode.INTEGRITY_ERROR, null);
        }
    }

    @Override
    public void close() {
        synchronized (this) { closing = true; }
        synchronized (closeLock) {
            if (!nativeClosed) {
                producer.close(closeTimeout);
                nativeClosed = true;
            }
            if (producerBudget != null) { producerBudget.closed(); }
        }
    }
}
