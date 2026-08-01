package io.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Semantic parser for the source-replayable Publish Admission body.
 *
 * <p>The generic System Mutation codec checks the outer field shape. This
 * parser checks the V1 admission equalities that must hold before an attempt
 * can become durable: descriptor/certificate/claim identity projections,
 * canonical descriptor and certificate digests, channel credential binding,
 * and the trusted decision interval.</p>
 */
public final class PublishAdmissionBody {
    private static final int HASH_LENGTH = 32;
    private static final int INCARNATION_LENGTH = 16;
    private static final int MESSAGE_ID_LENGTH = DelayMessageId.LENGTH;

    private final byte[] ownerIdentity;
    private final byte[] storeIncarnation;
    private final byte[] claimId;
    private final byte[] laneId;
    private final byte[] laneIncarnation;
    private final byte[] messageId;
    private final int generation;
    private final byte[] publishAttemptId;
    private final byte[] preparedPublishHash;
    private final byte[] reserveCharge;
    private final byte[] readyCertificateDigest;
    private final Channel channel;
    private final Descriptor descriptor;
    private final ReadyCertificate readyCertificate;
    private final TrustedUtcIntervalEvidence decisionTime;
    private final ClaimPrecondition claimPrecondition;

    private PublishAdmissionBody(final byte[] ownerIdentity, final byte[] storeIncarnation, final byte[] claimId,
                                 final byte[] laneId, final byte[] laneIncarnation, final byte[] messageId,
                                 final int generation, final byte[] publishAttemptId,
                                 final byte[] preparedPublishHash, final byte[] reserveCharge,
                                 final byte[] readyCertificateDigest, final Channel channel,
                                 final Descriptor descriptor, final ReadyCertificate readyCertificate,
                                 final TrustedUtcIntervalEvidence decisionTime,
                                 final ClaimPrecondition claimPrecondition) {
        this.ownerIdentity = copy(ownerIdentity);
        this.storeIncarnation = fixed(storeIncarnation, INCARNATION_LENGTH, "storeIncarnation");
        this.claimId = fixed(claimId, HASH_LENGTH, "claimId");
        this.laneId = fixed(laneId, HASH_LENGTH, "destinationLaneId");
        this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "laneIncarnation");
        this.messageId = fixed(messageId, MESSAGE_ID_LENGTH, "delayMessageId");
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be non-negative");
        }
        this.generation = generation;
        this.publishAttemptId = fixed(publishAttemptId, HASH_LENGTH, "publishAttemptId");
        this.preparedPublishHash = fixed(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
        this.reserveCharge = copy(reserveCharge);
        this.readyCertificateDigest = fixed(readyCertificateDigest, HASH_LENGTH, "readyCertificateDigest");
        this.channel = Objects.requireNonNull(channel, "channel");
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.readyCertificate = Objects.requireNonNull(readyCertificate, "readyCertificate");
        this.decisionTime = Objects.requireNonNull(decisionTime, "decisionTime");
        this.claimPrecondition = Objects.requireNonNull(claimPrecondition, "claimPrecondition");
        validateCrossObjectEqualities();
    }

    /** Parses and validates a canonical PUBLISH_ADMISSION body. */
    public static PublishAdmissionBody decode(final byte[] canonicalBody) {
        final List<CanonicalProtobuf.Reader.Field> fields =
                SystemMutationBodyCodec.fields(SystemMutationType.PUBLISH_ADMISSION, canonicalBody);
        final byte[] owner = nested(field(fields, 10), 10);
        final AuthorIdentity ownerIdentity = AuthorIdentity.decode(owner);
        ownerIdentity.requireFor(SystemMutationType.PUBLISH_ADMISSION);
        final long generation = unsigned(field(fields, 16), 16);
        if (generation > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("generation exceeds the runtime range");
        }
        final byte[] reserveCharge = nested(field(fields, 19), 19);
        validateChargeVector(reserveCharge);
        final Channel channel = decodeChannel(nested(field(fields, 21), 21));
        final Descriptor descriptor = decodeDescriptor(nested(field(fields, 22), 22));
        final ReadyCertificate certificate = decodeReadyCertificate(nested(field(fields, 23), 23));
        final TrustedUtcIntervalEvidence decision = TrustedUtcIntervalEvidence.decode(
                nested(field(fields, 24), 24));
        final ClaimPrecondition claim = decodeClaimPrecondition(nested(field(fields, 25), 25));
        return new PublishAdmissionBody(owner, bytes(field(fields, 11), 11), bytes(field(fields, 12), 12),
                bytes(field(fields, 13), 13), bytes(field(fields, 14), 14), bytes(field(fields, 15), 15),
                (int) generation, bytes(field(fields, 17), 17), bytes(field(fields, 18), 18), reserveCharge,
                bytes(field(fields, 20), 20), channel, descriptor, certificate, decision, claim);
    }

    public byte[] ownerIdentity() {
        return copy(ownerIdentity);
    }

    public byte[] storeIncarnation() {
        return copy(storeIncarnation);
    }

    public byte[] claimId() {
        return copy(claimId);
    }

    public byte[] laneId() {
        return copy(laneId);
    }

    public byte[] laneIncarnation() {
        return copy(laneIncarnation);
    }

    public byte[] messageId() {
        return copy(messageId);
    }

    public int generation() {
        return generation;
    }

    public byte[] publishAttemptId() {
        return copy(publishAttemptId);
    }

    public byte[] preparedPublishHash() {
        return copy(preparedPublishHash);
    }

    public byte[] reserveCharge() {
        return copy(reserveCharge);
    }

    public byte[] readyCertificateDigest() {
        return copy(readyCertificateDigest);
    }

    public Channel channel() {
        return channel;
    }

    public Descriptor descriptor() {
        return descriptor;
    }

    public ReadyCertificate readyCertificate() {
        return readyCertificate;
    }

    public TrustedUtcIntervalEvidence decisionTime() {
        return decisionTime;
    }

    public ClaimPrecondition claimPrecondition() {
        return claimPrecondition;
    }

    /** Requires the body decision interval to prove the supplied message timing. */
    public void requireTiming(final long actionAt, final long expireAt) {
        if (actionAt < 0 || expireAt < actionAt || decisionTime.earliestEpochMs() < actionAt
                || decisionTime.latestEpochMs() >= expireAt
                || decisionTime.latestEpochMs() >= readyCertificate.validUntilEpochMs()) {
            throw new IllegalArgumentException("Publish Admission decision time is stale");
        }
    }

    private void validateCrossObjectEqualities() {
        if (!Arrays.equals(descriptor.destinationLaneId(), laneId)
                || !Arrays.equals(descriptor.laneIncarnation(), laneIncarnation)
                || !Arrays.equals(descriptor.channel(), channel.canonicalBytes())
                || !Arrays.equals(descriptor.messageId(), messageId)
                || descriptor.generation() != generation
                || !Arrays.equals(descriptor.publishAttemptId(), publishAttemptId)) {
            throw new IllegalArgumentException("Publish Admission descriptor identity mismatch");
        }
        if (!Arrays.equals(readyCertificate.ownerIdentity(), ownerIdentity)
                || !Arrays.equals(readyCertificate.storeIncarnation(), storeIncarnation)
                || !Arrays.equals(readyCertificate.destinationLaneId(), laneId)
                || !Arrays.equals(readyCertificate.laneIncarnation(), laneIncarnation)
                || !Arrays.equals(readyCertificate.channel(), channel.canonicalBytes())
                || !Arrays.equals(readyCertificate.certificateDigest(), readyCertificateDigest)) {
            throw new IllegalArgumentException("Publish Admission certificate identity mismatch");
        }
        final ClaimPrecondition claim = claimPrecondition;
        if (!Arrays.equals(claim.claimId(), claimId)
                || !Arrays.equals(claim.messageId(), messageId)
                || claim.generation() != generation
                || !Arrays.equals(claim.destinationLaneId(), laneId)
                || !Arrays.equals(claim.laneIncarnation(), laneIncarnation)
                || !Arrays.equals(claim.ownerIdentity(), ownerIdentity)
                || !Arrays.equals(claim.storeIncarnation(), storeIncarnation)) {
            throw new IllegalArgumentException("Publish Admission Claim precondition mismatch");
        }
        final byte[] expectedMaterialization = descriptor.materializationBytes();
        if (!Arrays.equals(expectedMaterialization, claim.materialization())) {
            throw new IllegalArgumentException("Publish Admission materialization mismatch");
        }
        if (!Arrays.equals(descriptor.preparedPublishHash(), preparedPublishHash)) {
            throw new IllegalArgumentException("Publish Admission descriptor hash mismatch");
        }
        if (readyCertificate.credentialBindingGeneration() != channel.credentialBindingGeneration()
                || !Arrays.equals(readyCertificate.credentialBindingDigest(), channel.credentialBindingDigest())
                || !Arrays.equals(readyCertificate.credentialFingerprint(), channel.credentialFingerprint())) {
            throw new IllegalArgumentException("Publish Admission certificate/channel credential mismatch");
        }
        if (descriptor.deliverAtEpochMs() < 0 || descriptor.expireAtEpochMs() < descriptor.deliverAtEpochMs()
                || descriptor.actionAtEpochMs() < 0 || descriptor.actionAtEpochMs() != descriptor.deliverAtEpochMs()) {
            throw new IllegalArgumentException("unsupported Publish Admission timing");
        }
    }

    private static Channel decodeChannel(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ChannelResourceIdentity");
        requireFieldCountRange(fields, 15, 17, "ChannelResourceIdentity");
        final long adapterKind = unsigned(field(fields, 1), 1);
        if (adapterKind < 1 || adapterKind > 2) {
            throw new IllegalArgumentException("invalid channel adapter kind");
        }
        final long channelKind = unsigned(field(fields, 2), 2);
        if (channelKind < 1 || channelKind > 5) {
            throw new IllegalArgumentException("invalid channel kind");
        }
        fixed(bytes(field(fields, 3), 3), HASH_LENGTH, 3);
        fixed(bytes(field(fields, 4), 4), INCARNATION_LENGTH, 4);
        final byte[] targetResource = nested(field(fields, 5), 5);
        validateBrokerResource(targetResource);
        unsigned(field(fields, 6), 6);
        if (unsigned(field(fields, 7), 7) == 0) {
            throw new IllegalArgumentException("channel generation must be positive");
        }
        unsigned(field(fields, 8), 8);
        final byte[] producerIdentity = bytes(field(fields, 9), 9);
        nonEmpty(producerIdentity, 9);
        final byte[] producerHash = bytes(field(fields, 10), 10);
        if (!Arrays.equals(producerHash, Bytes.sha256(producerIdentity))) {
            throw new IllegalArgumentException("channel producer identity digest mismatch");
        }
        final boolean evidenceResource = has(fields, 11);
        final boolean evidenceGeneration = has(fields, 12);
        if (evidenceResource != evidenceGeneration) {
            throw new IllegalArgumentException("channel evidence fields must be paired");
        }
        final byte[] lease = nested(field(fields, 17), 17);
        final LeaseValues leaseValues = decodeLease(lease);
        final byte[] bindingDigest = bytes(field(fields, 15), 15);
        final byte[] fingerprint = bytes(field(fields, 16), 16);
        if (leaseValues.kind() != 1 || leaseValues.secretGeneration() != unsigned(field(fields, 14), 14)
                || !Arrays.equals(leaseValues.bindingDigest(), bindingDigest)
                || !Arrays.equals(leaseValues.fingerprint(), fingerprint)) {
            throw new IllegalArgumentException("channel credential lease mismatch");
        }
        fixed(bytes(field(fields, 13), 13), HASH_LENGTH, 13);
        return new Channel(encoded, bytes(field(fields, 3), 3), bytes(field(fields, 4), 4),
                targetResource, bytes(field(fields, 9), 9), unsigned(field(fields, 14), 14),
                bindingDigest, fingerprint);
    }

    private static Descriptor decodeDescriptor(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "PreparedPublishDescriptor");
        requireExactFields(fields, 20, "PreparedPublishDescriptor");
        for (int number = 1; number <= 20; number++) {
            field(fields, number);
        }
        if (unsigned(field(fields, 1), 1) != 1 || unsigned(field(fields, 3), 3) != 1) {
            throw new IllegalArgumentException("unsupported PreparedPublishDescriptor version");
        }
        final byte[] destinationProfile = nested(field(fields, 6), 6);
        final byte[] capabilityProfile = nested(field(fields, 7), 7);
        validateProfileRef(destinationProfile);
        validateProfileRef(capabilityProfile);
        final byte[] targetResource = nested(field(fields, 8), 8);
        validateBrokerResource(targetResource);
        final byte[] channel = nested(field(fields, 10), 10);
        decodeChannel(channel);
        final byte[] payload = nested(field(fields, 15), 15);
        validatePayload(payload);
        final byte[] metadata = nested(field(fields, 16), 16);
        validateAdapterMetadata(metadata);
        final int generation = intValue(field(fields, 12), 12);
        final byte[] reserved = nested(field(fields, 17), 17);
        validateReservedMetadata(reserved);
        final List<CanonicalProtobuf.Reader.Field> reservedFields = read(reserved, "ReservedPublishMetadata");
        if (!Arrays.equals(bytes(field(reservedFields, 3), 3), bytes(field(fields, 11), 11))
                || intValue(field(reservedFields, 4), 4) != generation
                || !Arrays.equals(bytes(field(reservedFields, 5), 5), bytes(field(fields, 13), 13))
                || unsigned(field(reservedFields, 8), 8) != unsigned(field(fields, 18), 18)) {
            throw new IllegalArgumentException("reserved publish metadata identity mismatch");
        }
        final int attemptNo = intValue(field(fields, 14), 14);
        if (attemptNo <= 0) {
            throw new IllegalArgumentException("attemptNo must be positive");
        }
        final long deliverAt = unsigned(field(fields, 18), 18);
        final long expireAt = unsigned(field(fields, 19), 19);
        final long actionAt = unsigned(field(fields, 20), 20);
        if (expireAt < deliverAt || actionAt != deliverAt) {
            throw new IllegalArgumentException("invalid PreparedPublishDescriptor timing");
        }
        final byte[] preparedHash = Bytes.sha256(Bytes.utf8("nereus-delay-prepared-publish-v1\0"), encoded);
        return new Descriptor(encoded, preparedHash, bytes(field(fields, 4), 4), bytes(field(fields, 5), 5),
                channel, bytes(field(fields, 11), 11), generation, bytes(field(fields, 13), 13), attemptNo,
                destinationProfile, capabilityProfile, targetResource, payload, metadata, deliverAt, expireAt,
                actionAt);
    }

    private static ReadyCertificate decodeReadyCertificate(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ReadyCertificate");
        requireExactFields(fields, 16, "ReadyCertificate");
        for (int number = 1; number <= 16; number++) {
            field(fields, number);
        }
        if (unsigned(field(fields, 1), 1) != 1) {
            throw new IllegalArgumentException("unsupported ReadyCertificate version");
        }
        final byte[] owner = nested(field(fields, 2), 2);
        AuthorIdentity.decode(owner).requireFor(SystemMutationType.PUBLISH_ADMISSION);
        final byte[] channel = nested(field(fields, 6), 6);
        decodeChannel(channel);
        nested(field(fields, 7), 7);
        nested(field(fields, 8), 8);
        unsigned(field(fields, 9), 9);
        unsigned(field(fields, 10), 10);
        final TrustedUtcIntervalEvidence issuedAt = TrustedUtcIntervalEvidence.decode(
                nested(field(fields, 12), 12));
        final byte[] digest = bytes(field(fields, 16), 16);
        final byte[] expectedDigest = Bytes.sha256(Bytes.utf8("nereus-delay-ready-certificate-v1\0"),
                canonicalFields(fields, 15));
        if (!Arrays.equals(digest, expectedDigest)) {
            throw new IllegalArgumentException("ReadyCertificate digest mismatch");
        }
        return new ReadyCertificate(encoded, owner, bytes(field(fields, 3), 3), bytes(field(fields, 4), 4),
                bytes(field(fields, 5), 5), channel, unsigned(field(fields, 11), 11), issuedAt,
                unsigned(field(fields, 13), 13), bytes(field(fields, 14), 14), bytes(field(fields, 15), 15), digest);
    }

    private static ClaimPrecondition decodeClaimPrecondition(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ClaimPrecondition");
        requireExactFieldsRange(fields, 18, 20, "ClaimPrecondition");
        for (int number = 1; number <= 9; number++) {
            field(fields, number);
        }
        for (int number = 12; number <= 20; number++) {
            field(fields, number);
        }
        final boolean hasMaterialization = has(fields, 10);
        if (hasMaterialization != has(fields, 11)) {
            throw new IllegalArgumentException("Claim materialization fields must be paired");
        }
        if (!hasMaterialization) {
            throw new IllegalArgumentException("Publish Admission requires Claim materialization");
        }
        final byte[] materialization = nested(field(fields, 10), 10);
        validateMaterialization(materialization);
        final byte[] materializationDigest = bytes(field(fields, 11), 11);
        final byte[] expectedDigest = Bytes.sha256(Bytes.utf8("nereus-delay-claim-materialization-v1\0"),
                materialization);
        if (!Arrays.equals(materializationDigest, expectedDigest)) {
            throw new IllegalArgumentException("Claim materialization digest mismatch");
        }
        final byte[] owner = nested(field(fields, 14), 14);
        AuthorIdentity.decode(owner).requireFor(SystemMutationType.PUBLISH_ADMISSION);
        validateChargeVector(nested(field(fields, 12), 12));
        return new ClaimPrecondition(encoded, bytes(field(fields, 1), 1), bytes(field(fields, 2), 2),
                intValue(field(fields, 3), 3), bytes(field(fields, 5), 5), bytes(field(fields, 6), 6), owner,
                bytes(field(fields, 15), 15), materialization);
    }

    private static LeaseValues decodeLease(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "CredentialUseLease");
        requireExactFields(fields, 11, "CredentialUseLease");
        if (unsigned(field(fields, 1), 1) != 1 || unsigned(field(fields, 3), 3) != 1) {
            throw new IllegalArgumentException("unsupported CredentialUseLease version/kind");
        }
        validateProfileRef(nested(field(fields, 2), 2));
        nested(field(fields, 8), 8);
        final long validUntil = unsigned(field(fields, 9), 9);
        final TrustedUtcIntervalEvidence issuedAt = TrustedUtcIntervalEvidence.decode(nested(field(fields, 8), 8));
        if (validUntil <= issuedAt.latestEpochMs() || unsigned(field(fields, 10), 10) == 0) {
            throw new IllegalArgumentException("invalid CredentialUseLease timing");
        }
        final byte[] digest = bytes(field(fields, 11), 11);
        final byte[] expected = Bytes.sha256(Bytes.utf8("nereus-delay-credential-use-lease-v1\0"),
                canonicalFields(fields, 10));
        if (!Arrays.equals(digest, expected)) {
            throw new IllegalArgumentException("CredentialUseLease digest mismatch");
        }
        return new LeaseValues(unsigned(field(fields, 3), 3), unsigned(field(fields, 5), 5),
                bytes(field(fields, 6), 6), bytes(field(fields, 7), 7));
    }

    private static void validateProfileRef(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ProfileRef");
        requireExactFields(fields, 4, "ProfileRef");
        nonEmpty(bytes(field(fields, 1), 1), 1);
        if (unsigned(field(fields, 2), 2) == 0) {
            throw new IllegalArgumentException("ProfileRef version must be positive");
        }
        bytes(field(fields, 3), 3);
        final long kind = unsigned(field(fields, 4), 4);
        if (kind < 1 || kind > 4) {
            throw new IllegalArgumentException("invalid ProfileRef kind");
        }
    }

    private static void validateBrokerResource(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> outer = read(encoded, "BrokerResourceIdentity");
        if (outer.size() != 1 || (outer.get(0).number() != 1 && outer.get(0).number() != 2)) {
            throw new IllegalArgumentException("invalid BrokerResourceIdentity branch");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = read(outer.get(0).rawValue(), "BrokerResource");
        if (outer.get(0).number() == 1) {
            requireExactFields(fields, 2, "KafkaResourceIdentity");
            nonEmpty(bytes(field(fields, 1), 1), 1);
            fixed(bytes(field(fields, 2), 2), 16, 2);
        } else {
            requireExactFields(fields, 4, "PulsarResourceIdentity");
            nonEmpty(bytes(field(fields, 1), 1), 1);
            fixed(bytes(field(fields, 2), 2), HASH_LENGTH, 2);
            nonEmpty(bytes(field(fields, 3), 3), 3);
            unsigned(field(fields, 4), 4);
        }
    }

    private static void validatePayload(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "PayloadForPublish");
        if (fields.size() != 3) {
            throw new IllegalArgumentException("PayloadForPublish must contain one payload branch");
        }
        final long length = unsigned(field(fields, 1), 1);
        final byte[] hash = bytes(field(fields, 2), 2);
        if (!has(fields, 3) && !has(fields, 4)) {
            throw new IllegalArgumentException("PayloadForPublish has no payload branch");
        }
        if (has(fields, 3)) {
            final byte[] inline = bytes(field(fields, 3), 3);
            if (inline.length != length || !Arrays.equals(hash, Bytes.sha256(inline))) {
                throw new IllegalArgumentException("inline payload length/hash mismatch");
            }
        } else {
            final List<CanonicalProtobuf.Reader.Field> object = read(nested(field(fields, 4), 4),
                    "CommittedPayloadDescriptor");
            requireExactFields(fieldsForObject(object), 9, "CommittedPayloadDescriptor");
            nested(field(object, 1), 1);
            nonEmpty(bytes(field(object, 2), 2), 2);
            nonEmpty(bytes(field(object, 3), 3), 3);
            nonEmpty(bytes(field(object, 4), 4), 4);
            unsigned(field(object, 6), 6);
            fixed(bytes(field(object, 7), 7), HASH_LENGTH, 7);
            fixed(bytes(field(object, 8), 8), HASH_LENGTH, 8);
            fixed(bytes(field(object, 9), 9), HASH_LENGTH, 9);
            if (length != unsigned(field(object, 6), 6) || !Arrays.equals(hash, bytes(field(object, 7), 7))) {
                throw new IllegalArgumentException("object payload length/hash mismatch");
            }
        }
    }

    private static void validateAdapterMetadata(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "AdapterMetadata");
        if (fields.size() != 1 || (fields.get(0).number() != 1 && fields.get(0).number() != 2)) {
            throw new IllegalArgumentException("invalid AdapterMetadata branch");
        }
        nested(fields.get(0), fields.get(0).number());
    }

    private static void validateReservedMetadata(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ReservedPublishMetadata");
        requireExactFields(fields, 9, "ReservedPublishMetadata");
        fixed(bytes(field(fields, 1), 1), 16, 1);
        unsigned(field(fields, 2), 2);
        fixed(bytes(field(fields, 3), 3), MESSAGE_ID_LENGTH, 3);
        intValue(field(fields, 4), 4);
        fixed(bytes(field(fields, 5), 5), HASH_LENGTH, 5);
        fixed(bytes(field(fields, 6), 6), HASH_LENGTH, 6);
        fixed(bytes(field(fields, 7), 7), HASH_LENGTH, 7);
        unsigned(field(fields, 8), 8);
        if (unsigned(field(fields, 9), 9) != 1) {
            throw new IllegalArgumentException("only managed DeliveryMode is supported");
        }
    }

    private static void validateMaterialization(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ClaimMaterialization");
        requireExactFields(fields, 11, "ClaimMaterialization");
        validateProfileRef(nested(field(fields, 1), 1));
        validateProfileRef(nested(field(fields, 2), 2));
        validateBrokerResource(nested(field(fields, 3), 3));
        unsigned(field(fields, 4), 4);
        fixed(bytes(field(fields, 5), 5), MESSAGE_ID_LENGTH, 5);
        intValue(field(fields, 6), 6);
        validatePayload(nested(field(fields, 7), 7));
        validateAdapterMetadata(nested(field(fields, 8), 8));
        unsigned(field(fields, 9), 9);
        unsigned(field(fields, 10), 10);
        unsigned(field(fields, 11), 11);
    }

    private static void validateChargeVector(final byte[] encoded) {
        final List<CanonicalProtobuf.Reader.Field> fields = read(encoded, "ChargeVector");
        requireExactFields(fields, 17, "ChargeVector");
        for (int number = 1; number <= 17; number++) {
            unsigned(field(fields, number), number);
        }
    }

    private static byte[] canonicalFields(final List<CanonicalProtobuf.Reader.Field> fields, final int through) {
        return CanonicalProtobuf.message(output -> {
            for (CanonicalProtobuf.Reader.Field field : fields) {
                if (field.number() > through) {
                    break;
                }
                if (field.wireType() == 0) {
                    CanonicalProtobuf.int64(output, field.number(), field.unsignedValue());
                } else {
                    CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
                }
            }
        });
    }

    private static List<CanonicalProtobuf.Reader.Field> read(final byte[] encoded, final String name) {
        Objects.requireNonNull(encoded, name);
        if (encoded.length == 0) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        final List<CanonicalProtobuf.Reader.Field> fields = new ArrayList<>();
        final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
        while (reader.hasRemaining()) {
            fields.add(reader.next());
        }
        return fields;
    }

    private static List<CanonicalProtobuf.Reader.Field> fieldsForObject(
            final List<CanonicalProtobuf.Reader.Field> fields) {
        return fields;
    }

    private static void requireExactFields(final List<CanonicalProtobuf.Reader.Field> fields, final int count,
                                           final String name) {
        if (fields.size() != count) {
            throw new IllegalArgumentException(name + " has unexpected field count");
        }
    }

    private static void requireExactFieldsRange(final List<CanonicalProtobuf.Reader.Field> fields, final int min,
                                                final int max, final String name) {
        if (fields.size() < min || fields.size() > max) {
            throw new IllegalArgumentException(name + " has unexpected field count");
        }
    }

    private static void requireFieldCountRange(final List<CanonicalProtobuf.Reader.Field> fields, final int min,
                                               final int max, final String name) {
        requireExactFieldsRange(fields, min, max, name);
        for (int number = 1; number <= 10; number++) {
            field(fields, number);
        }
        for (int number = 13; number <= 17; number++) {
            field(fields, number);
        }
    }

    private static CanonicalProtobuf.Reader.Field field(final List<CanonicalProtobuf.Reader.Field> fields,
                                                        final int number) {
        for (CanonicalProtobuf.Reader.Field field : fields) {
            if (field.number() == number) {
                return field;
            }
        }
        throw new IllegalArgumentException("missing nested field " + number);
    }

    private static boolean has(final List<CanonicalProtobuf.Reader.Field> fields, final int number) {
        return fields.stream().anyMatch(field -> field.number() == number);
    }

    private static long unsigned(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 0 || field.unsignedValue() < 0) {
            throw new IllegalArgumentException("invalid nested scalar field " + number);
        }
        return field.unsignedValue();
    }

    private static int intValue(final CanonicalProtobuf.Reader.Field field, final int number) {
        final long value = unsigned(field, number);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("nested uint32 exceeds runtime range");
        }
        return (int) value;
    }

    private static byte[] bytes(final CanonicalProtobuf.Reader.Field field, final int number) {
        if (field.number() != number || field.wireType() != 2) {
            throw new IllegalArgumentException("invalid nested bytes field " + number);
        }
        return field.rawValue();
    }

    private static byte[] nested(final CanonicalProtobuf.Reader.Field field, final int number) {
        final byte[] value = bytes(field, number);
        read(value, "nested field " + number);
        return value;
    }

    private static byte[] fixed(final byte[] value, final int length, final int number) {
        Bytes.requireLength(value, length, "nested field " + number);
        return value;
    }

    private static byte[] fixed(final byte[] value, final int length, final String name) {
        Bytes.requireLength(value, length, name);
        return copy(value);
    }

    private static byte[] nonEmpty(final byte[] value, final int number) {
        if (value.length == 0) {
            throw new IllegalArgumentException("nested field " + number + " must not be empty");
        }
        return value;
    }

    private static byte[] copy(final byte[] value) {
        return Bytes.copy(Objects.requireNonNull(value, "value"));
    }

    public static final class Channel {
        private final byte[] canonicalBytes;
        private final byte[] laneId;
        private final byte[] laneIncarnation;
        private final byte[] targetResource;
        private final byte[] producerIdentity;
        private final long credentialBindingGeneration;
        private final byte[] credentialBindingDigest;
        private final byte[] credentialFingerprint;

        private Channel(final byte[] canonicalBytes, final byte[] laneId, final byte[] laneIncarnation,
                        final byte[] targetResource, final byte[] producerIdentity,
                        final long credentialBindingGeneration, final byte[] credentialBindingDigest,
                        final byte[] credentialFingerprint) {
            this.canonicalBytes = copy(canonicalBytes);
            this.laneId = fixed(laneId, HASH_LENGTH, "channel lane id");
            this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "channel lane incarnation");
            this.targetResource = copy(targetResource);
            this.producerIdentity = copy(producerIdentity);
            this.credentialBindingGeneration = credentialBindingGeneration;
            this.credentialBindingDigest = fixed(credentialBindingDigest, HASH_LENGTH,
                    "channel credential binding digest");
            this.credentialFingerprint = fixed(credentialFingerprint, HASH_LENGTH,
                    "channel credential fingerprint");
        }

        public byte[] canonicalBytes() {
            return copy(canonicalBytes);
        }

        public byte[] laneId() {
            return copy(laneId);
        }

        public byte[] laneIncarnation() {
            return copy(laneIncarnation);
        }

        public byte[] targetResource() {
            return copy(targetResource);
        }

        public byte[] producerIdentity() {
            return copy(producerIdentity);
        }

        public long credentialBindingGeneration() {
            return credentialBindingGeneration;
        }

        public byte[] credentialBindingDigest() {
            return copy(credentialBindingDigest);
        }

        public byte[] credentialFingerprint() {
            return copy(credentialFingerprint);
        }
    }

    /** Validates and decodes a registry ChannelResourceIdentityV1 outside an Admission body. */
    public static Channel decodeChannelIdentity(final byte[] encoded) {
        return decodeChannel(Objects.requireNonNull(encoded, "encoded"));
    }

    /** Validates a registry BrokerResourceIdentityV1 outside an Admission body. */
    public static void validateBrokerResourceIdentity(final byte[] encoded) {
        validateBrokerResource(Objects.requireNonNull(encoded, "encoded"));
    }

    public static final class Descriptor {
        private final byte[] canonicalBytes;
        private final byte[] preparedPublishHash;
        private final byte[] destinationLaneId;
        private final byte[] laneIncarnation;
        private final byte[] channel;
        private final byte[] messageId;
        private final int generation;
        private final byte[] publishAttemptId;
        private final int attemptNo;
        private final byte[] destinationProfile;
        private final byte[] capabilityProfile;
        private final byte[] targetResource;
        private final byte[] payload;
        private final byte[] metadata;
        private final long deliverAtEpochMs;
        private final long expireAtEpochMs;
        private final long actionAtEpochMs;

        private Descriptor(final byte[] canonicalBytes, final byte[] preparedPublishHash,
                           final byte[] destinationLaneId, final byte[] laneIncarnation, final byte[] channel,
                           final byte[] messageId, final int generation, final byte[] publishAttemptId,
                           final int attemptNo, final byte[] destinationProfile, final byte[] capabilityProfile,
                           final byte[] targetResource, final byte[] payload, final byte[] metadata,
                           final long deliverAtEpochMs, final long expireAtEpochMs, final long actionAtEpochMs) {
            this.canonicalBytes = copy(canonicalBytes);
            this.preparedPublishHash = fixed(preparedPublishHash, HASH_LENGTH, "preparedPublishHash");
            this.destinationLaneId = fixed(destinationLaneId, HASH_LENGTH, "descriptor lane id");
            this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "descriptor lane incarnation");
            this.channel = copy(channel);
            this.messageId = fixed(messageId, MESSAGE_ID_LENGTH, "descriptor message id");
            this.generation = generation;
            this.publishAttemptId = fixed(publishAttemptId, HASH_LENGTH, "descriptor attempt id");
            this.attemptNo = attemptNo;
            this.destinationProfile = copy(destinationProfile);
            this.capabilityProfile = copy(capabilityProfile);
            this.targetResource = copy(targetResource);
            this.payload = copy(payload);
            this.metadata = copy(metadata);
            this.deliverAtEpochMs = deliverAtEpochMs;
            this.expireAtEpochMs = expireAtEpochMs;
            this.actionAtEpochMs = actionAtEpochMs;
        }

        public byte[] canonicalBytes() {
            return copy(canonicalBytes);
        }

        public byte[] preparedPublishHash() {
            return copy(preparedPublishHash);
        }

        public byte[] destinationLaneId() {
            return copy(destinationLaneId);
        }

        public byte[] laneIncarnation() {
            return copy(laneIncarnation);
        }

        public byte[] channel() {
            return copy(channel);
        }

        public byte[] messageId() {
            return copy(messageId);
        }

        public int generation() {
            return generation;
        }

        public byte[] publishAttemptId() {
            return copy(publishAttemptId);
        }

        public int attemptNo() {
            return attemptNo;
        }

        public byte[] destinationProfile() {
            return copy(destinationProfile);
        }

        public byte[] capabilityProfile() {
            return copy(capabilityProfile);
        }

        public byte[] targetResource() {
            return copy(targetResource);
        }

        public byte[] payload() {
            return copy(payload);
        }

        public byte[] metadata() {
            return copy(metadata);
        }

        public long deliverAtEpochMs() {
            return deliverAtEpochMs;
        }

        public long expireAtEpochMs() {
            return expireAtEpochMs;
        }

        public long actionAtEpochMs() {
            return actionAtEpochMs;
        }

        public byte[] materializationBytes() {
            final List<CanonicalProtobuf.Reader.Field> fields = read(canonicalBytes, "PreparedPublishDescriptor");
            return CanonicalProtobuf.message(output -> {
                emitBytes(output, 1, nested(field(fields, 6), 6));
                emitBytes(output, 2, nested(field(fields, 7), 7));
                emitBytes(output, 3, nested(field(fields, 8), 8));
                emitVarint(output, 4, unsigned(field(fields, 9), 9));
                emitBytes(output, 5, bytes(field(fields, 11), 11));
                emitVarint(output, 6, unsigned(field(fields, 12), 12));
                emitBytes(output, 7, nested(field(fields, 15), 15));
                emitBytes(output, 8, nested(field(fields, 16), 16));
                emitVarint(output, 9, unsigned(field(fields, 18), 18));
                emitVarint(output, 10, unsigned(field(fields, 19), 19));
                emitVarint(output, 11, unsigned(field(fields, 20), 20));
            });
        }
    }

    public static final class ReadyCertificate {
        private final byte[] canonicalBytes;
        private final byte[] ownerIdentity;
        private final byte[] storeIncarnation;
        private final byte[] destinationLaneId;
        private final byte[] laneIncarnation;
        private final byte[] channel;
        private final long validUntilEpochMs;
        private final TrustedUtcIntervalEvidence issuedAt;
        private final long credentialBindingGeneration;
        private final byte[] credentialBindingDigest;
        private final byte[] credentialFingerprint;
        private final byte[] certificateDigest;

        private ReadyCertificate(final byte[] canonicalBytes, final byte[] ownerIdentity,
                                 final byte[] storeIncarnation, final byte[] destinationLaneId,
                                 final byte[] laneIncarnation, final byte[] channel, final long validUntilEpochMs,
                                 final TrustedUtcIntervalEvidence issuedAt, final long credentialBindingGeneration,
                                 final byte[] credentialBindingDigest, final byte[] credentialFingerprint,
                                 final byte[] certificateDigest) {
            this.canonicalBytes = copy(canonicalBytes);
            this.ownerIdentity = copy(ownerIdentity);
            this.storeIncarnation = fixed(storeIncarnation, INCARNATION_LENGTH, "certificate store incarnation");
            this.destinationLaneId = fixed(destinationLaneId, HASH_LENGTH, "certificate lane id");
            this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "certificate lane incarnation");
            this.channel = copy(channel);
            this.validUntilEpochMs = validUntilEpochMs;
            this.issuedAt = issuedAt;
            if (credentialBindingGeneration <= 0) {
                throw new IllegalArgumentException("certificate credential generation must be positive");
            }
            this.credentialBindingGeneration = credentialBindingGeneration;
            this.credentialBindingDigest = fixed(credentialBindingDigest, HASH_LENGTH,
                    "certificate credential binding digest");
            this.credentialFingerprint = fixed(credentialFingerprint, HASH_LENGTH,
                    "certificate credential fingerprint");
            this.certificateDigest = fixed(certificateDigest, HASH_LENGTH, "certificate digest");
        }

        public byte[] canonicalBytes() {
            return copy(canonicalBytes);
        }

        public byte[] ownerIdentity() {
            return copy(ownerIdentity);
        }

        public byte[] storeIncarnation() {
            return copy(storeIncarnation);
        }

        public byte[] destinationLaneId() {
            return copy(destinationLaneId);
        }

        public byte[] laneIncarnation() {
            return copy(laneIncarnation);
        }

        public byte[] channel() {
            return copy(channel);
        }

        public long validUntilEpochMs() {
            return validUntilEpochMs;
        }

        public TrustedUtcIntervalEvidence issuedAt() {
            return issuedAt;
        }

        public long credentialBindingGeneration() {
            return credentialBindingGeneration;
        }

        public byte[] credentialBindingDigest() {
            return copy(credentialBindingDigest);
        }

        public byte[] credentialFingerprint() {
            return copy(credentialFingerprint);
        }

        public byte[] certificateDigest() {
            return copy(certificateDigest);
        }
    }

    public static final class ClaimPrecondition {
        private final byte[] canonicalBytes;
        private final byte[] claimId;
        private final byte[] messageId;
        private final int generation;
        private final byte[] destinationLaneId;
        private final byte[] laneIncarnation;
        private final byte[] ownerIdentity;
        private final byte[] storeIncarnation;
        private final byte[] materialization;

        private ClaimPrecondition(final byte[] canonicalBytes, final byte[] claimId, final byte[] messageId,
                                  final int generation, final byte[] destinationLaneId,
                                  final byte[] laneIncarnation, final byte[] ownerIdentity,
                                  final byte[] storeIncarnation, final byte[] materialization) {
            this.canonicalBytes = copy(canonicalBytes);
            this.claimId = fixed(claimId, HASH_LENGTH, "precondition claim id");
            this.messageId = fixed(messageId, MESSAGE_ID_LENGTH, "precondition message id");
            this.generation = generation;
            this.destinationLaneId = fixed(destinationLaneId, HASH_LENGTH, "precondition lane id");
            this.laneIncarnation = fixed(laneIncarnation, INCARNATION_LENGTH, "precondition lane incarnation");
            this.ownerIdentity = copy(ownerIdentity);
            this.storeIncarnation = fixed(storeIncarnation, INCARNATION_LENGTH, "precondition store incarnation");
            this.materialization = copy(materialization);
        }

        public byte[] canonicalBytes() {
            return copy(canonicalBytes);
        }

        public byte[] claimId() {
            return copy(claimId);
        }

        public byte[] messageId() {
            return copy(messageId);
        }

        public int generation() {
            return generation;
        }

        public byte[] destinationLaneId() {
            return copy(destinationLaneId);
        }

        public byte[] laneIncarnation() {
            return copy(laneIncarnation);
        }

        public byte[] ownerIdentity() {
            return copy(ownerIdentity);
        }

        public byte[] storeIncarnation() {
            return copy(storeIncarnation);
        }

        public byte[] materialization() {
            return copy(materialization);
        }
    }

    private record LeaseValues(long kind, long secretGeneration, byte[] bindingDigest, byte[] fingerprint) {
        private LeaseValues {
            bindingDigest = copy(bindingDigest);
            fingerprint = copy(fingerprint);
        }

        @Override
        public byte[] bindingDigest() {
            return copy(bindingDigest);
        }

        @Override
        public byte[] fingerprint() {
            return copy(fingerprint);
        }
    }

    private static void emitBytes(final java.io.ByteArrayOutputStream output, final int number, final byte[] value) {
        CanonicalProtobuf.bytes(output, number, value);
    }

    private static void emitVarint(final java.io.ByteArrayOutputStream output, final int number, final long value) {
        CanonicalProtobuf.int64(output, number, value);
    }

    private static long unsignedValue(final byte[] encoded) {
        if (encoded.length == 0 || encoded.length > 8) {
            throw new IllegalArgumentException("invalid encoded uint64");
        }
        long value = 0;
        for (int index = 0; index < encoded.length; index++) {
            final int current = encoded[index] & 0xff;
            value |= (long) (current & 0x7f) << (index * 7);
        }
        if (value < 0) {
            throw new IllegalArgumentException("encoded uint64 exceeds signed runtime range");
        }
        return value;
    }
}
