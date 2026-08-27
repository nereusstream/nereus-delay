package com.nereusstream.delay.adapter;

import com.nereusstream.delay.protocol.ArtifactGenerationSet;
import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.EvidenceVerificationStatus;
import com.nereusstream.delay.protocol.ExternalDeliveryIdentity;
import com.nereusstream.delay.protocol.PublishEvidence;
import com.nereusstream.delay.protocol.PublishEvidenceKind;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarPreparedRecord;
import com.nereusstream.delay.protocol.PulsarSequenceAuthority;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Builds the typed Pulsar SEND acknowledgement evidence branch. */
public final class PulsarSendAckEvidence {
    private static final int HASH_LENGTH = 32;

    private PulsarSendAckEvidence() {}

    /** Creates a verified PUBLISHED branch bound to one exact prepared attempt. */
    public static PublishEvidence published(
            final PulsarDestinationRequest request,
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
        if (request.partition() < 0
                || ledgerId < 0
                || entryId < 0
                || normalizedBatchIndex < 0
                || brokerPersistenceTime < 0
                || sequenceId < 0) {
            throw new IllegalArgumentException("Pulsar SEND ACK position values must be non-negative");
        }
        final BrokerResourceIdentity target = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                request.authenticatedClusterId(),
                request.resourceIncarnation(),
                request.physicalTopic(),
                request.physicalTopicCreationTimestamp()));
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, target.canonicalBytes());
            CanonicalProtobuf.uint32(output, 2, request.partition());
            CanonicalProtobuf.uint64(output, 3, ledgerId);
            CanonicalProtobuf.uint64(output, 4, entryId);
            CanonicalProtobuf.uint32(output, 5, normalizedBatchIndex);
            CanonicalProtobuf.int64(output, 6, brokerPersistenceTime);
            CanonicalProtobuf.bytes(output, 7, producerNameHash);
            CanonicalProtobuf.uint64(output, 8, sequenceId);
            CanonicalProtobuf.bytes(
                    output,
                    9,
                    ExternalDeliveryIdentity.publishAttempt(request.publishAttemptId())
                            .canonicalBytes());
            CanonicalProtobuf.bytes(output, 10, preparedPublishHash);
            CanonicalProtobuf.bytes(output, 11, authenticatedResponseSha256);
        });
        return PublishEvidence.create(
                PublishEvidenceKind.PULSAR_SEND_ACK, EvidenceVerificationStatus.VERIFIED_PUBLISHED, branch);
    }

    /**
     * Creates the generation-2 ACK branch for the exact final Pulsar record.
     * The artifact set supplies the separately committed P1 source-lock
     * digest; the record supplies the logical target, identity and sequence
     * authority. No envelope bytes are used as the business payload here.
     */
    public static PublishEvidence publishedRecord(
            final PulsarPreparedRecord record,
            final ArtifactGenerationSet artifacts,
            final byte[] producerNameHash,
            final long ledgerId,
            final long entryId,
            final int normalizedBatchIndex,
            final int batchSize,
            final long brokerPersistenceTime,
            final int p1ProtocolVersion,
            final long connectionGeneration,
            final long producerId,
            final long actualSequenceId,
            final byte[] sendCommandSha256,
            final byte[] authenticatedResponseSha256) {
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(artifacts, "artifacts");
        Bytes.requireLength(producerNameHash, HASH_LENGTH, "producerNameHash");
        Bytes.requireLength(sendCommandSha256, HASH_LENGTH, "sendCommandSha256");
        Bytes.requireLength(authenticatedResponseSha256, HASH_LENGTH, "authenticatedResponseSha256");
        if (!Arrays.equals(record.artifactGenerationSetDigest(), artifacts.setDigest())) {
            throw new IllegalArgumentException("Pulsar ACK artifact generation set mismatch");
        }
        if (record.template().targetResource().kind() != BrokerResourceIdentity.Kind.PULSAR
                || record.template().physicalPartition() < 0
                || record.template().physicalPartition() > 0xffff_ffffL) {
            throw new IllegalArgumentException("Pulsar ACK target is invalid");
        }
        if (ledgerId < 0
                || entryId < 0
                || normalizedBatchIndex < 0
                || batchSize <= 0
                || Integer.compareUnsigned(normalizedBatchIndex, batchSize) >= 0
                || brokerPersistenceTime < 0
                || p1ProtocolVersion <= 0
                || connectionGeneration < 0
                || producerId < 0
                || actualSequenceId < 0) {
            throw new IllegalArgumentException("Pulsar generation-2 ACK values are invalid");
        }
        final ExternalDeliveryIdentity identity = record.externalIdentity();
        final PulsarSequenceAuthority authority = record.sequenceAuthority();
        if (identity.kind() == ExternalDeliveryIdentity.Kind.PUBLISH_ATTEMPT) {
            if (authority.kind() != PulsarSequenceAuthority.Kind.MANAGED_JOURNAL
                    || authority.sequenceId() != actualSequenceId
                    || !Arrays.equals(authority.producerNameHash(), producerNameHash)) {
                throw new IllegalArgumentException("managed ACK does not match its fixed Journal sequence");
            }
        } else if (identity.kind() != ExternalDeliveryIdentity.Kind.NATIVE_DELIVERY
                || authority.kind() != PulsarSequenceAuthority.Kind.PRODUCER_ASSIGNED) {
            throw new IllegalArgumentException("Pulsar ACK identity and sequence authority disagree");
        }
        final byte[] branch = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 2);
            CanonicalProtobuf.bytes(
                    output, 2, record.template().targetResource().canonicalBytes());
            CanonicalProtobuf.uint32(output, 3, record.template().physicalPartition());
            CanonicalProtobuf.uint64(output, 4, ledgerId);
            CanonicalProtobuf.uint64(output, 5, entryId);
            CanonicalProtobuf.uint32(output, 6, normalizedBatchIndex);
            CanonicalProtobuf.uint32(output, 7, batchSize);
            CanonicalProtobuf.int64(output, 8, brokerPersistenceTime);
            CanonicalProtobuf.bytes(output, 9, producerNameHash);
            CanonicalProtobuf.uint32(output, 10, p1ProtocolVersion);
            CanonicalProtobuf.uint64(output, 11, connectionGeneration);
            CanonicalProtobuf.uint64(output, 12, producerId);
            CanonicalProtobuf.uint64(output, 13, actualSequenceId);
            CanonicalProtobuf.bytes(output, 14, identity.canonicalBytes());
            CanonicalProtobuf.bytes(output, 15, record.preparedIdentityHash());
            CanonicalProtobuf.bytes(output, 16, record.recordTemplateHash());
            CanonicalProtobuf.bytes(output, 17, record.preparedRecordHash());
            CanonicalProtobuf.bytes(output, 18, authority.canonicalBytes());
            CanonicalProtobuf.bytes(output, 19, sendCommandSha256);
            CanonicalProtobuf.bytes(output, 20, authenticatedResponseSha256);
            CanonicalProtobuf.bytes(output, 21, artifacts.p1SourceLockDigest());
            CanonicalProtobuf.bytes(output, 22, record.artifactGenerationSetDigest());
        });
        return PublishEvidence.create(
                PublishEvidenceKind.PULSAR_SEND_ACK, EvidenceVerificationStatus.VERIFIED_PUBLISHED, branch);
    }

    /** Requires a record evidence branch to be byte-identical to the expected record projection. */
    public static void requireExactBindingForRecord(
            final PublishEvidence evidence,
            final PulsarPreparedRecord record,
            final ArtifactGenerationSet artifacts,
            final byte[] producerNameHash,
            final long ledgerId,
            final long entryId,
            final int normalizedBatchIndex,
            final int batchSize,
            final long brokerPersistenceTime,
            final int p1ProtocolVersion,
            final long connectionGeneration,
            final long producerId,
            final long actualSequenceId,
            final byte[] sendCommandSha256,
            final byte[] authenticatedResponseSha256) {
        Objects.requireNonNull(evidence, "evidence");
        if (evidence.evidenceKind() != PublishEvidenceKind.PULSAR_SEND_ACK
                || evidence.verificationStatus() != EvidenceVerificationStatus.VERIFIED_PUBLISHED
                || !Arrays.equals(
                        evidence.canonicalBytes(),
                        publishedRecord(
                                        record,
                                        artifacts,
                                        producerNameHash,
                                        ledgerId,
                                        entryId,
                                        normalizedBatchIndex,
                                        batchSize,
                                        brokerPersistenceTime,
                                        p1ProtocolVersion,
                                        connectionGeneration,
                                        producerId,
                                        actualSequenceId,
                                        sendCommandSha256,
                                        authenticatedResponseSha256)
                                .canonicalBytes())) {
            throw new IllegalArgumentException("Pulsar generation-2 ACK is not bound to the exact record");
        }
    }

    /**
     * Validates a decoded generation-2 ACK returned by a transport. The P1
     * values are taken from the authenticated evidence itself, then rebuilt
     * through the one canonical encoder so a caller cannot merely place
     * unrelated hashes beside a record identity.
     */
    public static void requireExactRecordBinding(
            final PublishEvidence evidence,
            final PulsarPreparedRecord record,
            final ArtifactGenerationSet artifacts,
            final long ledgerId,
            final long entryId,
            final int normalizedBatchIndex,
            final int batchSize,
            final long brokerPersistenceTime) {
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(record, "record");
        Objects.requireNonNull(artifacts, "artifacts");
        final List<CanonicalProtobuf.Reader.Field> fields = branchFieldsForRecord(evidence.branch());
        if (uint(fields.get(0), 1) != 2
                || uint(fields.get(2), 3) != record.template().physicalPartition()
                || uint(fields.get(3), 4) != ledgerId
                || uint(fields.get(4), 5) != entryId
                || uint(fields.get(5), 6) != normalizedBatchIndex
                || uint(fields.get(6), 7) != batchSize
                || uint(fields.get(7), 8) != brokerPersistenceTime
                || !Arrays.equals(
                        BrokerResourceIdentity.decode(bytes(fields.get(1), 2)).canonicalBytes(),
                        record.template().targetResource().canonicalBytes())
                || !Arrays.equals(bytes(fields.get(21), 22), record.artifactGenerationSetDigest())) {
            throw new IllegalArgumentException("Pulsar generation-2 ACK position/target is not exact");
        }
        requireExactBindingForRecord(
                evidence,
                record,
                artifacts,
                bytes(fields.get(8), 9),
                ledgerId,
                entryId,
                normalizedBatchIndex,
                batchSize,
                brokerPersistenceTime,
                boundedInt(uint(fields.get(9), 10), "p1ProtocolVersion"),
                uint(fields.get(10), 11),
                uint(fields.get(11), 12),
                uint(fields.get(12), 13),
                bytes(fields.get(18), 19),
                bytes(fields.get(19), 20));
    }

    /**
     * Verifies provider-returned evidence before an uncertain SEND is
     * promoted to PUBLISHED. The provider supplies the broker reread; this
     * method binds that reread to the exact destination attempt and timestamp.
     */
    public static void requireExactBinding(
            final PublishEvidence evidence,
            final PulsarDestinationRequest request,
            final byte[] preparedPublishHash,
            final byte[] producerNameHash,
            final long brokerPersistenceTime) {
        Objects.requireNonNull(evidence, "evidence");
        Objects.requireNonNull(request, "request");
        Bytes.requireLength(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
        Bytes.requireLength(producerNameHash, HASH_LENGTH, "producerNameHash");
        if (evidence.evidenceKind() != PublishEvidenceKind.PULSAR_SEND_ACK
                || evidence.verificationStatus() != EvidenceVerificationStatus.VERIFIED_PUBLISHED) {
            throw new IllegalArgumentException("Pulsar SEND evidence has the wrong branch");
        }
        evidence.requireBusinessMutation(request.publishAttemptId(), true);
        if (brokerPersistenceTime < 0) {
            throw new IllegalArgumentException("brokerPersistenceTime must be non-negative");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = branchFields(evidence.branch());
        final BrokerResourceIdentity expectedTarget = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                request.authenticatedClusterId(),
                request.resourceIncarnation(),
                request.physicalTopic(),
                request.physicalTopicCreationTimestamp()));
        if (!BrokerResourceIdentity.decode(bytes(fields.get(0), 1)).equals(expectedTarget)
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

    private static List<CanonicalProtobuf.Reader.Field> branchFieldsForRecord(final byte[] branch) {
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(branch);
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        if (fields.size() != 22) {
            throw new IllegalArgumentException("Pulsar generation-2 SEND evidence branch has an invalid shape");
        }
        for (int index = 0; index < fields.size(); index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("Pulsar generation-2 SEND evidence branch has invalid field order");
            }
        }
        return fields;
    }

    private static int boundedInt(final long value, final String name) {
        if (value <= 0 || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " is outside the positive int range");
        }
        return (int) value;
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
