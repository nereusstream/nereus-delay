package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChannelResourceIdentityV1Test {
    @Test
    void baselineChannelAndLeaseRoundTripCanonically() {
        final byte[] encoded = ProtocolTestFixtures.baselineKafkaChannel();
        final ChannelResourceIdentityV1 channel = ChannelResourceIdentityV1.decode(encoded);
        final CredentialUseLeaseV1 lease = CredentialUseLeaseV1.decode(channel.credentialUseLease().canonicalBytes());

        assertArrayEquals(encoded, channel.canonicalBytes());
        assertEquals(AdapterKindV1.KAFKA, channel.adapterKind());
        assertEquals(ChannelKindV1.BASELINE_PRODUCER, channel.channelKind());
        assertEquals(CredentialUseKindV1.DESTINATION_CHANNEL, lease.kind());
        lease.requireTtlAtMost(8_000);
    }

    @Test
    void rejectsChannelKindThatNeedsEvidenceWithoutEvidenceResource() {
        final byte[] baseline = ProtocolTestFixtures.baselineKafkaChannel();
        final byte[] transactional = rewrite(baseline, 2, 2);

        assertThrows(IllegalArgumentException.class, () -> ChannelResourceIdentityV1.decode(transactional));
    }

    @Test
    void rejectsProducerDigestAndLeaseDigestDrift() {
        final byte[] baseline = ProtocolTestFixtures.baselineKafkaChannel();
        final byte[] producerDrift = rewriteBytes(baseline, 10, Bytes.sha256(Bytes.utf8("other-producer")));
        assertThrows(IllegalArgumentException.class, () -> ChannelResourceIdentityV1.decode(producerDrift));

        final ChannelResourceIdentityV1 channel = ChannelResourceIdentityV1.decode(baseline);
        final byte[] lease = tamperDigest(channel.credentialUseLease().canonicalBytes());
        final byte[] channelWithBadLease = replaceNested(baseline, 17, lease);
        assertThrows(IllegalArgumentException.class, () -> ChannelResourceIdentityV1.decode(channelWithBadLease));
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

    private static void write(final java.io.ByteArrayOutputStream output,
                              final CanonicalProtobuf.Reader.Field field) {
        if (field.wireType() == 0) {
            CanonicalProtobuf.uint64(output, field.number(), field.unsignedValue());
        } else {
            CanonicalProtobuf.bytes(output, field.number(), field.rawValue());
        }
    }
}
