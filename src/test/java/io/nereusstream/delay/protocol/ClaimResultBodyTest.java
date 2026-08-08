package io.nereusstream.delay.protocol;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
        final byte[] materialization = materialization(messageId, destination, capability,
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("claim-cluster",
                        java.util.UUID.nameUUIDFromBytes(Bytes.utf8("claim-topic")))).canonicalBytes(),
                payload(), adapterMetadata());
        final byte[] precondition = precondition(messageId, materialization);

        assertDoesNotThrow(() -> ClaimResultBody.decodePrecondition(precondition));
    }

    @Test
    void claimMaterializationRejectsNonCanonicalBrokerResourceIdentity() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 16);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] destination = new ProfileRefV1(Bytes.utf8("claim-destination"), 1,
                Bytes.sha256(Bytes.utf8("claim-destination-hash")), ProfileKindV1.DESTINATION)
                .canonicalBytes();
        final byte[] capability = new ProfileRefV1(Bytes.utf8("claim-capability"), 1,
                Bytes.sha256(Bytes.utf8("claim-capability-hash")), ProfileKindV1.DELIVERY_CAPABILITY)
                .canonicalBytes();
        final byte[] broker = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1,
                CanonicalProtobuf.message(inner -> {
                    CanonicalProtobuf.bytes(inner, 1, Bytes.utf8("e\u0301"));
                    CanonicalProtobuf.bytes(inner, 2, new byte[16]);
                })));
        final byte[] materialization = materialization(messageId, destination, capability, broker,
                payload(), adapterMetadata());

        assertThrows(IllegalArgumentException.class,
                () -> ClaimResultBody.decodePrecondition(precondition(messageId, materialization)));
    }

    @Test
    void claimMaterializationRejectsCommittedPayloadWithWrongProfileKind() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 16);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] destination = new ProfileRefV1(Bytes.utf8("claim-destination"), 1,
                Bytes.sha256(Bytes.utf8("claim-destination-hash")), ProfileKindV1.DESTINATION)
                .canonicalBytes();
        final byte[] capability = new ProfileRefV1(Bytes.utf8("claim-capability"), 1,
                Bytes.sha256(Bytes.utf8("claim-capability-hash")), ProfileKindV1.DELIVERY_CAPABILITY)
                .canonicalBytes();
        final byte[] objectPayload = new byte[]{7};
        final byte[] descriptor = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, destination);
            CanonicalProtobuf.bytes(output, 2, Bytes.utf8("container"));
            CanonicalProtobuf.bytes(output, 3, Bytes.utf8("object"));
            CanonicalProtobuf.bytes(output, 4, Bytes.utf8("version"));
            CanonicalProtobuf.uint64(output, 6, objectPayload.length);
            CanonicalProtobuf.bytes(output, 7, Bytes.sha256(objectPayload));
            CanonicalProtobuf.bytes(output, 8, nonZero(32, 3));
            CanonicalProtobuf.bytes(output, 9, nonZero(32, 4));
        });
        final byte[] objectPayloadValue = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint64(output, 1, objectPayload.length);
            CanonicalProtobuf.bytes(output, 2, Bytes.sha256(objectPayload));
            CanonicalProtobuf.bytes(output, 4, descriptor);
        });
        final byte[] materialization = materialization(messageId, destination, capability,
                BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1("claim-cluster",
                        java.util.UUID.nameUUIDFromBytes(Bytes.utf8("claim-topic")))).canonicalBytes(),
                objectPayloadValue, adapterMetadata());

        assertThrows(IllegalArgumentException.class,
                () -> ClaimResultBody.decodePrecondition(precondition(messageId, materialization)));
    }

    private static byte[] materialization(final DelayMessageId messageId, final byte[] destination,
                                          final byte[] capability, final byte[] broker,
                                          final byte[] payload, final byte[] metadata) {
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, destination);
            CanonicalProtobuf.bytes(output, 2, capability);
            CanonicalProtobuf.bytes(output, 3, broker);
            CanonicalProtobuf.uint64(output, 4, 0);
            CanonicalProtobuf.bytes(output, 5, messageId.bytes());
            CanonicalProtobuf.uint32(output, 6, 0);
            CanonicalProtobuf.bytes(output, 7, payload);
            CanonicalProtobuf.bytes(output, 8, metadata);
            CanonicalProtobuf.int64(output, 9, 1_000);
            CanonicalProtobuf.int64(output, 10, 2_000);
            CanonicalProtobuf.int64(output, 11, 1_000);
        });
    }

    private static byte[] precondition(final DelayMessageId messageId, final byte[] materialization) {
        return CanonicalProtobuf.message(output -> {
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
