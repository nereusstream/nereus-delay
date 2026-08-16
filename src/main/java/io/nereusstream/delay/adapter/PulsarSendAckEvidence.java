package io.nereusstream.delay.adapter;

import io.nereusstream.delay.protocol.BrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.EvidenceVerificationStatusV1;
import io.nereusstream.delay.protocol.ExternalDeliveryIdentityV1;
import io.nereusstream.delay.protocol.PulsarBrokerResourceIdentityV1;
import io.nereusstream.delay.protocol.PublishEvidenceKindV1;
import io.nereusstream.delay.protocol.PublishEvidenceV1;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    /**
     * Verifies provider-returned evidence before an uncertain SEND is
     * promoted to PUBLISHED.  The provider supplies the broker reread; this
     * method binds that reread to the exact destination attempt and timestamp.
     */
    public static void requireExactBinding(final PublishEvidenceV1 evidence,
                                           final PulsarDestinationRequest request,
                                           final byte[] preparedPublishHash,
                                           final byte[] producerNameHash,
                                           final long brokerPersistenceTime) {
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(request, "request");
        Bytes.requireLength(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
        Bytes.requireLength(producerNameHash, HASH_LENGTH, "producerNameHash");
        if (evidence.evidenceKind() != PublishEvidenceKindV1.PULSAR_SEND_ACK
                || evidence.verificationStatus() != EvidenceVerificationStatusV1.VERIFIED_PUBLISHED) {
            throw new IllegalArgumentException("Pulsar SEND evidence has the wrong branch");
        }
        evidence.requireBusinessMutation(request.publishAttemptId(), true);
        if (brokerPersistenceTime < 0) {
            throw new IllegalArgumentException("brokerPersistenceTime must be non-negative");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = branchFields(evidence.branch());
        final BrokerResourceIdentityV1 expectedTarget = BrokerResourceIdentityV1.pulsar(
                new PulsarBrokerResourceIdentityV1(request.authenticatedClusterId(), request.resourceIncarnation(),
                        request.physicalTopic(), request.physicalTopicCreationTimestamp()));
        if (!BrokerResourceIdentityV1.decode(bytes(fields.get(0), 1)).equals(expectedTarget)
                || uint(fields.get(1), 2) != request.partition()
                || uint(fields.get(5), 6) != brokerPersistenceTime
                || !Arrays.equals(bytes(fields.get(6), 7), producerNameHash)
                || !Arrays.equals(bytes(fields.get(9), 10), preparedPublishHash)) {
            throw new IllegalArgumentException("Pulsar SEND evidence does not match the exact request");
        }
    }

    private static List<CanonicalProtobuf.Reader.Field> branchFields(final byte[] branch) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(branch);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() != 11) {
            throw new IllegalArgumentException("Pulsar SEND evidence branch has an invalid shape");
        }
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("Pulsar SEND evidence branch has an invalid field order");
            }
        }
        return fields;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("Pulsar SEND evidence field is not bytes: " + number);
        }
        return field.rawValue();
    }

    private static long uint(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0) {
            throw new IllegalArgumentException("Pulsar SEND evidence field is not uint: " + number);
        }
        final long value = field.unsignedValue();
        if (value < 0) {
            throw new IllegalArgumentException("Pulsar SEND evidence uint is out of range");
        }
        return value;
    }
}
