package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import org.junit.jupiter.api.Test;

class CommitLargeScheduleBodyTest {
    @Test
    void canonicalBodyAndPreparedCommandRoundTrip() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 2);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] reservationId = Bytes.sha256(Bytes.utf8("reservation"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final ProfileRef profile = new ProfileRef(
                Bytes.utf8("object-store"), 1, Bytes.sha256(Bytes.utf8("profile")), ProfileKind.OBJECT_STORE);
        final CanonicalPayloadCommitProof proof = CanonicalPayloadCommitProof.signed(
                reservationId,
                Bytes.sha256(Bytes.utf8("tenant-scope")),
                shard.routeIncarnation().bytes(),
                shard.partition(),
                messageId,
                profile,
                3,
                1,
                Bytes.utf8("bucket"),
                Bytes.utf8("object"),
                Bytes.utf8("version"),
                new byte[0],
                7,
                Bytes.sha256(Bytes.utf8("payload")),
                10_000,
                keyPair.getPrivate());
        final CommitLargeScheduleBody body = new CommitLargeScheduleBody(messageId, 20_000, reservationId, proof);

        assertEquals(body, CommitLargeScheduleBody.decode(body.canonicalBytes()));
        assertEquals(
                body,
                CommandBodies.decodeCommitLarge(CommandBodies.commitLarge(messageId, 20_000, reservationId, proof)));

        final PreparedCommand command = PreparedCommand.commitLarge(shard, messageId, reservationId, proof, 20_000);
        assertEquals(command, CommandCodec.decodeManagedFrame(CommandCodec.encodeManagedFrame(command)));
    }

    @Test
    void rejectsProofIdentityDriftAndNonCanonicalFieldOrder() throws Exception {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 3);
        final DelayMessageId messageId = DelayMessageId.random(shard);
        final byte[] reservationId = Bytes.sha256(Bytes.utf8("reservation"));
        final KeyPairGenerator generator = KeyPairGenerator.getInstance("Ed25519");
        final KeyPair keyPair = generator.generateKeyPair();
        final ProfileRef profile = new ProfileRef(
                Bytes.utf8("object-store"), 1, Bytes.sha256(Bytes.utf8("profile")), ProfileKind.OBJECT_STORE);
        final CanonicalPayloadCommitProof proof = CanonicalPayloadCommitProof.signed(
                reservationId,
                Bytes.sha256(Bytes.utf8("tenant-scope")),
                shard.routeIncarnation().bytes(),
                shard.partition(),
                messageId,
                profile,
                3,
                1,
                Bytes.utf8("bucket"),
                Bytes.utf8("object"),
                Bytes.utf8("version"),
                new byte[0],
                7,
                Bytes.sha256(Bytes.utf8("payload")),
                10_000,
                keyPair.getPrivate());
        final byte[] differentReservation = Bytes.sha256(Bytes.utf8("different-reservation"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new CommitLargeScheduleBody(messageId, 20_000, differentReservation, proof));

        final byte[] nonCanonical = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, messageId.bytes());
            CanonicalProtobuf.uint32(output, 2, 3);
            CanonicalProtobuf.int64(output, 3, 20_000);
            CanonicalProtobuf.bytes(output, 11, proof.canonicalBytes());
            CanonicalProtobuf.bytes(output, 10, reservationId);
        });
        assertThrows(IllegalArgumentException.class, () -> CommitLargeScheduleBody.decode(nonCanonical));
    }
}
