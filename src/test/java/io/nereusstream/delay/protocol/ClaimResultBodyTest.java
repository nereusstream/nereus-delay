package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class ClaimResultBodyTest {
    @Test
    void claimPreconditionAcceptsCompleteUnsignedProfileVersions() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 16);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] destination = new ProfileRefV1(Bytes.utf8("claim-destination"), Long.MIN_VALUE,
                Bytes.sha256(Bytes.utf8("claim-destination-hash")), ProfileKindV1.DESTINATION).canonicalBytes();
        final byte[] capability = new ProfileRefV1(Bytes.utf8("claim-capability"), -1L,
                Bytes.sha256(Bytes.utf8("claim-capability-hash")), ProfileKindV1.DELIVERY_CAPABILITY)
                .canonicalBytes();
        final byte[] materialization = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, destination);
            CanonicalProtobuf.bytes(output, 2, capability);
            CanonicalProtobuf.bytes(output, 3, BrokerResourceIdentityV1.kafka(
                    new KafkaBrokerResourceIdentityV1("claim-cluster", java.util.UUID.nameUUIDFromBytes(
                            Bytes.utf8("claim-topic")))).canonicalBytes());
            CanonicalProtobuf.uint64(output, 4, 0);
            CanonicalProtobuf.bytes(output, 5, messageId.bytes());
            CanonicalProtobuf.uint32(output, 6, 0);
            CanonicalProtobuf.bytes(output, 7, payload());
            CanonicalProtobuf.bytes(output, 8, adapterMetadata());
            CanonicalProtobuf.int64(output, 9, 1_000);
            CanonicalProtobuf.int64(output, 10, 2_000);
            CanonicalProtobuf.int64(output, 11, 1_000);
        });
        final byte[] precondition = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, nonZero(32, 1));
            CanonicalProtobuf.bytes(output, 2, messageId.bytes());
            CanonicalProtobuf.uint32(output, 3, 0);
            CanonicalProtobuf.int64(output, 4, 1);
            CanonicalProtobuf.bytes(output, 5, nonZero(32, 2));
            CanonicalProtobuf.bytes(output, 6, new byte[16]);
            CanonicalProtobuf.int64(output, 7, 1);
            CanonicalProtobuf.int64(output, 8, 1);
            CanonicalProtobuf.bytes(output, 9, Bytes.sha256(Bytes.utf8("timeline")));
            CanonicalProtobuf.bytes(output, 10, materialization);
            CanonicalProtobuf.bytes(output, 11, Bytes.sha256(
                    Bytes.utf8("nereus-delay-claim-materialization-v1\0"), materialization));
            CanonicalProtobuf.bytes(output, 12, chargeVector());
            CanonicalProtobuf.int64(output, 13, 3_000);
            CanonicalProtobuf.bytes(output, 14, AuthorIdentity.owner(Bytes.utf8("claim-deployment"),
                    Bytes.utf8("claim-worker"), 1, Bytes.sha256(Bytes.utf8("claim-lease"))).canonicalBytes());
            CanonicalProtobuf.bytes(output, 15, new byte[16]);
            CanonicalProtobuf.uint32(output, 16, 1);
            CanonicalProtobuf.uint32(output, 17, 0);
            CanonicalProtobuf.uint32(output, 18, 0);
            CanonicalProtobuf.bytes(output, 19, Bytes.sha256(Bytes.utf8("obligations")));
            CanonicalProtobuf.bytes(output, 20, Bytes.sha256(Bytes.utf8("semantic")));
        });

        assertDoesNotThrow(() -> ClaimResultBody.decodePrecondition(precondition));
    }

    private static byte[] payload() {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 0);
            CanonicalProtobuf.bytes(output, 2, Bytes.sha256(new byte[0]));
            CanonicalProtobuf.bytes(output, 3, new byte[0]);
        });
    }

    private static byte[] adapterMetadata() {
        return CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1,
                CanonicalProtobuf.message(inner -> CanonicalProtobuf.bytes(inner, 1, new byte[0]))));
    }

    private static byte[] chargeVector() {
        return CanonicalProtobuf.message(output -> {
            for (int number = 1; number <= 17; number++) {
                CanonicalProtobuf.uint64(output, number, 0);
            }
        });
    }

    private static byte[] nonZero(final int length, final int seed) {
        final byte[] result = new byte[length];
        java.util.Arrays.fill(result, (byte) seed);
        return result;
    }
}
