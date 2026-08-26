package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

class ChannelResourceIdentityTest {
    @Test
    void baselineChannelAndLeaseRoundTripCanonically() {
        final byte[] encoded = ProtocolTestFixtures.baselineKafkaChannel();
        final ChannelResourceIdentity channel = ChannelResourceIdentity.decode(encoded);
        final CredentialUseLease lease =
                CredentialUseLease.decode(channel.credentialUseLease().canonicalBytes());

        assertArrayEquals(encoded, channel.canonicalBytes());
        assertEquals(AdapterKind.KAFKA, channel.adapterKind());
        assertEquals(ChannelKind.BASELINE_PRODUCER, channel.channelKind());
        assertEquals(CredentialUseKind.DESTINATION_CHANNEL, lease.kind());
        lease.requireTtlAtMost(8_000);
    }

    @Test
    void rejectsChannelKindThatNeedsEvidenceWithoutEvidenceResource() {
        final byte[] baseline = ProtocolTestFixtures.baselineKafkaChannel();
        final byte[] transactional = rewrite(baseline, 2, 2);

        assertThrows(IllegalArgumentException.class, () -> ChannelResourceIdentity.decode(transactional));
    }

    @Test
    void rejectsProducerDigestAndLeaseDigestDrift() {
        final byte[] baseline = ProtocolTestFixtures.baselineKafkaChannel();
        final byte[] producerDrift = rewriteBytes(baseline, 10, Bytes.sha256(Bytes.utf8("other-producer")));
        assertThrows(IllegalArgumentException.class, () -> ChannelResourceIdentity.decode(producerDrift));

        final ChannelResourceIdentity channel = ChannelResourceIdentity.decode(baseline);
        final byte[] lease = tamperDigest(channel.credentialUseLease().canonicalBytes());
        final byte[] channelWithBadLease = replaceNested(baseline, 17, lease);
        assertThrows(IllegalArgumentException.class, () -> ChannelResourceIdentity.decode(channelWithBadLease));
    }

    @Test
    void preservesUnsignedChannelAndEvidenceGenerationBits() {
        final long highBitGeneration = Long.MIN_VALUE;
        final ChannelResourceIdentity channel =
                ChannelResourceIdentity.decode(highBitKafkaTransactionalChannel(highBitGeneration));

        assertEquals(highBitGeneration, channel.channelGeneration());
        assertEquals(highBitGeneration, channel.evidenceGeneration());
        assertEquals(Long.MIN_VALUE, channel.credentialBindingGeneration());
        assertEquals(Long.MIN_VALUE, channel.credentialUseLease().secretGeneration());
        assertArrayEquals(
                channel.canonicalBytes(),
                ChannelResourceIdentity.decode(channel.canonicalBytes()).canonicalBytes());
    }

    private static byte[] highBitKafkaTransactionalChannel(final long generation) {
        final byte[] lane = Bytes.sha256(Bytes.utf8("high-bit-channel-lane"));
        final byte[] laneIncarnation = new byte[16];
        final BrokerResourceIdentity target = BrokerResourceIdentity.kafka(new KafkaBrokerResourceIdentity(
                "high-bit-cluster", java.util.UUID.nameUUIDFromBytes(Bytes.utf8("high-bit-channel-topic"))));
        final byte[] producer = Bytes.utf8("high-bit-producer");
        final byte[] guardDigest = Bytes.sha256(Bytes.utf8("high-bit-guard"));
        final byte[] bindingDigest = Bytes.sha256(Bytes.utf8("high-bit-binding"));
        final byte[] fingerprint = Bytes.sha256(Bytes.utf8("high-bit-fingerprint"));
        final long credentialGeneration = Long.MIN_VALUE;
        final byte[] prefix = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, AdapterKind.KAFKA.wireValue());
            CanonicalProtobuf.uint32(output, 2, ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT.wireValue());
            CanonicalProtobuf.bytes(output, 3, lane);
            CanonicalProtobuf.bytes(output, 4, laneIncarnation);
            CanonicalProtobuf.bytes(output, 5, target.canonicalBytes());
            CanonicalProtobuf.uint32(output, 6, 0);
            CanonicalProtobuf.uint64Bits(output, 7, generation);
            CanonicalProtobuf.uint32(output, 8, 0);
            CanonicalProtobuf.bytes(output, 9, producer);
            CanonicalProtobuf.bytes(output, 10, Bytes.sha256(producer));
            CanonicalProtobuf.bytes(output, 11, target.canonicalBytes());
            CanonicalProtobuf.uint64Bits(output, 12, generation);
            CanonicalProtobuf.bytes(output, 13, guardDigest);
        });
        final ProfileRef profile = new ProfileRef(
                Bytes.utf8("high-bit-destination"),
                1,
                Bytes.sha256(Bytes.utf8("high-bit-destination-semantic")),
                ProfileKind.DESTINATION);
        final TrustedUtcIntervalEvidence issuedAt = new TrustedUtcIntervalEvidence(
                1_000,
                1_001,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("high-bit-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("high-bit-time")),
                0,
                null);
        final CredentialUseLease lease = new CredentialUseLease(
                profile,
                CredentialUseKind.DESTINATION_CHANNEL,
                CredentialUseLease.destinationChannelHolderScope(prefix),
                credentialGeneration,
                bindingDigest,
                fingerprint,
                issuedAt,
                9_000,
                1);
        return new ChannelResourceIdentity(
                        AdapterKind.KAFKA,
                        ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT,
                        lane,
                        laneIncarnation,
                        target,
                        0,
                        generation,
                        0,
                        producer,
                        Bytes.sha256(producer),
                        target,
                        generation,
                        guardDigest,
                        credentialGeneration,
                        bindingDigest,
                        fingerprint,
                        lease)
                .canonicalBytes();
    }

    private static byte[] rewrite(final byte[] encoded, final int number, final long value) {
        return CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
            while (reader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = reader.next();
                if (field.number() == number) {
                    CanonicalProtobuf.uint64(output, number, value);
                } else {
                    write(output, field);
                }
            }
        });
    }

    private static byte[] rewriteBytes(final byte[] encoded, final int number, final byte[] value) {
        return CanonicalProtobuf.message(output -> {
            final CanonicalProtobuf.Reader reader = new CanonicalProtobuf.Reader(encoded);
            while (reader.hasRemaining()) {
                final CanonicalProtobuf.Reader.Field field = reader.next();
                if (field.number() == number) {
                    CanonicalProtobuf.bytes(output, number, value);
                } else {
                    write(output, field);
                }
            }
        });
    }

    private static byte[] replaceNested(final byte[] encoded, final int number, final byte[] value) {
        return rewriteBytes(encoded, number, value);
    }

    private static byte[] tamperDigest(final byte[] encoded) {
        return rewriteBytes(encoded, 11, Bytes.sha256(Bytes.utf8("wrong-lease-digest")));
    }

    private static void write(final java.io.ByteArrayOutputStream output, final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64(output, field.number(), field.unsignedValue());
        } else {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }
}
