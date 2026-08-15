package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import io.nereusstream.delay.protocol.ExternalDeliveryIdentityV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;

import java.util.Objects;

/** Builds the typed V1 Pulsar SEND acknowledgement evidence branch. */
public final class PulsarSendAckEvidence {
    private static final int HASH_LENGTH = 32;

    private PulsarSendAckEvidence() {
    }

    /** Creates a verified PUBLISHED branch bound to one exact prepared attempt. */
    public static PublishEvidenceV1 published(final PulsarDestinationRequest request,
                                              final byte[] preparedPublishHash,
                                              final byte[] producerNameHash,
                                              final long ledgerId,
                                              final long entryId,
                                              final int normalizedBatchIndex,
                                              final long brokerPersistenceTime,
                                              final long sequenceId,
                                              final byte[] authenticatedResponseSha256) {
        Objects.requireNonNull(request, "request");
        Bytes.requireLength(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
        Bytes.requireLength(producerNameHash, HASH_LENGTH, "producerNameHash");
        Bytes.requireLength(authenticatedResponseSha256, HASH_LENGTH, "authenticatedResponseSha256");
        if (request.partition() < 0 || ledgerId < 0 || entryId < 0 || normalizedBatchIndex < 0
                || brokerPersistenceTime < 0 || sequenceId < 0) {
            throw new IllegalArgumentException("Pulsar SEND ACK position values must be non-negative");
        }
        final BrokerResourceIdentityV1 target = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1(request.authenticatedClusterId(), request.resourceIncarnation(),
                        request.physicalTopic(), request.physicalTopicCreationTimestamp()));
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, target.canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, request.partition());
            CanonicalProtobuf.uint64(output, 3, ledgerId);
            CanonicalProtobuf.uint64(output, 4, entryId);
            CanonicalProtobuf.uint32(output, 5, normalizedBatchIndex);
            CanonicalProtobuf.int64(output, 6, brokerPersistenceTime);
            CanonicalProtobuf.bytes(output, 7, producerNameHash);
            CanonicalProtobuf.uint64(output, 8, sequenceId);
            CanonicalProtobuf.bytes(output, 9,
                    ExternalDeliveryIdentityV1.publishAttempt(request.publishAttemptId()).canonicalBytes());
            CanonicalProtobuf.bytes(output, 10, preparedPublishHash);
            CanonicalProtobuf.bytes(output, 11, authenticatedResponseSha256);
        });
        return PublishEvidenceV1.create(PublishEvidenceKindV1.PULSAR_SEND_ACK,
                EvidenceVerificationStatusV1.VERIFIED_PUBLISHED, branch);
    }
}
