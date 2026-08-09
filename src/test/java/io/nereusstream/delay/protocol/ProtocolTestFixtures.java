package io.nereusstream.delay.protocol;

import java.util.UUID;

/** Small canonical protocol fixtures shared by codec and runtime regression tests. */
public final class ProtocolTestFixtures {
    private ProtocolTestFixtures() {
    }

    public static byte[] baselineKafkaChannel() {
        final byte[] lane = Bytes.sha256(Bytes.utf8("fixture-channel-lane"));
        final byte[] laneIncarnation = new byte[16];
        final BrokerResourceIdentityV1 target = BrokerResourceIdentityV1.kafka(
                new KafkaBrokerResourceIdentityV1("fixture-cluster",
                        UUID.nameUUIDFromBytes(Bytes.utf8("fixture-channel-topic"))));
        final ProfileRefV1 profile = new ProfileRefV1(Bytes.utf8("fixture-destination"), 1,
                Bytes.sha256(Bytes.utf8("fixture-destination-semantic")), ProfileKindV1.DESTINATION);
        final byte[] bindingDigest = Bytes.sha256(Bytes.utf8("fixture-binding"));
        final byte[] fingerprint = Bytes.sha256(Bytes.utf8("fixture-fingerprint"));
        final byte[] prefix = channelFieldsThrough13(AdapterKindV1.KAFKA, ChannelKindV1.BASELINE_PRODUCER,
                lane, laneIncarnation, target, 0, 1, 0, Bytes.utf8("fixture-producer"),
                Bytes.sha256(Bytes.utf8("fixture-guard")));
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(1_000, 1_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8("fixture-clock"),
                1, 1, 1, Bytes.sha256(Bytes.utf8("fixture-time")), 0, null);
        final CredentialUseLeaseV1 lease = new CredentialUseLeaseV1(profile,
                CredentialUseKindV1.DESTINATION_CHANNEL,
                CredentialUseLeaseV1.destinationChannelHolderScope(prefix), 1, bindingDigest, fingerprint,
                issuedAt, 9_000, 1);
        return new ChannelResourceIdentityV1(AdapterKindV1.KAFKA, ChannelKindV1.BASELINE_PRODUCER,
                lane, laneIncarnation, target, 0, 1, 0, Bytes.utf8("fixture-producer"),
                Bytes.sha256(Bytes.utf8("fixture-producer")), null, null,
                Bytes.sha256(Bytes.utf8("fixture-guard")), 1, bindingDigest, fingerprint, lease).canonicalBytes();
    }

    /** Builds the Registry-shaped Kafka Lane tuple used by typed Lane tests. */
    public static byte[] canonicalKafkaLaneTuple(final ProfileRefV1 destination,
                                                  final ProfileRefV1 capability) {
        final byte[] topicUuid = uuidBytes(UUID.nameUUIDFromBytes(Bytes.utf8("fixture-lane-topic")));
        return Bytes.concat(
                Bytes.sha256(Bytes.utf8("fixture-tenant-routing-scope")),
                Bytes.u8(AdapterKindV1.KAFKA.wireValue()),
                Bytes.lp32(Bytes.utf8("fixture-target-cluster")),
                Bytes.u8(1),
                topicUuid,
                Bytes.lp32(topicUuid),
                Bytes.u32be(3),
                Bytes.lp32(destination.profileId()),
                Bytes.u64beBits(destination.version()),
                destination.semanticHash(),
                Bytes.lp32(capability.profileId()),
                Bytes.u64beBits(capability.version()),
                capability.semanticHash(),
                Bytes.u8(1),
                Bytes.sha256(Bytes.utf8("fixture-ordering-domain")));
    }

    private static byte[] channelFieldsThrough13(final AdapterKindV1 adapterKind, final ChannelKindV1 channelKind,
                                                  final byte[] lane, final byte[] laneIncarnation,
                                                  final BrokerResourceIdentityV1 target, final long partition,
                                                  final long generation, final long slot, final byte[] producer,
                                                  final byte[] guardDigest) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, adapterKind.wireValue());
            CanonicalProtobuf.uint32(output, 2, channelKind.wireValue());
            CanonicalProtobuf.bytes(output, 3, lane);
            CanonicalProtobuf.bytes(output, 4, laneIncarnation);
            CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, partition);
            CanonicalProtobuf.uint64(output, 7, generation);
            CanonicalProtobuf.uint32(output, 8, slot);
            CanonicalProtobuf.bytes(output, 9, producer);
            CanonicalProtobuf.bytes(output, 10, Bytes.sha256(producer));
            CanonicalProtobuf.bytes(output, 13, guardDigest);
        });
    }

    private static byte[] uuidBytes(final UUID value) {
        return java.nio.ByteBuffer.allocate(16).putLong(value.getMostSignificantBits())
                .putLong(value.getLeastSignificantBits()).array();
    }
}
