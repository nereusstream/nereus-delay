package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.AuthorIdentity;
import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.CanonicalProtobuf;
import io.nereusstream.delay.protocol.CheckpointResourceV1;
import io.nereusstream.delay.protocol.KafkaSourcePosition;
import io.nereusstream.delay.protocol.ProfileKindV1;
import io.nereusstream.delay.protocol.ProfileRefV1;
import io.nereusstream.delay.protocol.ResourceDeleteConfirmedBody;
import io.nereusstream.delay.protocol.ResourceKind;
import io.nereusstream.delay.protocol.ResourceRetireIntentBody;
import io.nereusstream.delay.protocol.RouteIncarnation;
import io.nereusstream.delay.protocol.ShardId;
import io.nereusstream.delay.protocol.SourcePosition;
import io.nereusstream.delay.protocol.SystemMutation;
import io.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import io.nereusstream.delay.runtime.ResourceRetireIntentRecord;
import org.junit.jupiter.api.Test;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointDeleteConfirmationComposerTest {
    @Test
    void composesSignedDeletedConfirmationFromExactProviderReceipt() throws Exception {
        final Fixture fixture = fixture();
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final TrustedUtcIntervalEvidence observedAt = evidence(1_000, 1_010, "observed");
        final TrustedUtcIntervalEvidence confirmedAt = evidence(1_011, 1_012, "confirmed");

        final SystemMutation mutation = CheckpointDeleteConfirmationComposer.compose(fixture.intent(),
                new CheckpointDeleteResult(fixture.resource(), ResourceDeleteConfirmedBody.DeleteOutcome.DELETED,
                        id32(7), id32(8)), observedAt, confirmedAt, 9_000,
                serviceAuthor().canonicalBytes(), 1, keyPair.getPrivate());

        assertEquals(fixture.shard(), mutation.shardId());
        assertTrue(mutation.verifySignature(keyPair.getPublic()));
        final ResourceDeleteConfirmedBody body = ResourceDeleteConfirmedBody.decode(mutation.canonicalBody());
        assertEquals(ResourceDeleteConfirmedBody.DeleteOutcome.DELETED, body.outcome());
        assertArrayEquals(fixture.intent().mutationId(), body.intent().mutationId());
        assertArrayEquals(fixture.resource().immutableVersion(), body.evidence().observedImmutableVersion());
        assertArrayEquals(observedAt.canonicalBytes(), body.evidence().observedAt().canonicalBytes());
        assertArrayEquals(confirmedAt.canonicalBytes(), body.confirmedAt().canonicalBytes());

        final SystemMutation decoded = SystemMutation.decodeEnvelope(mutation.canonicalEnvelope());
        assertArrayEquals(mutation.systemMutationId(), decoded.systemMutationId());
        assertArrayEquals(fixture.intent().mutationId(), mutation.logicalOperationIdentity());
    }

    @Test
    void composesAlreadyAbsentWithoutInventingAnObservedVersion() throws Exception {
        final Fixture fixture = fixture();
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final TrustedUtcIntervalEvidence observedAt = evidence(2_000, 2_000, "absent-observed");
        final TrustedUtcIntervalEvidence confirmedAt = evidence(2_001, 2_001, "absent-confirmed");

        final SystemMutation mutation = CheckpointDeleteConfirmationComposer.compose(fixture.intent(),
                new CheckpointDeleteResult(fixture.resource(), ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT,
                        id32(9), id32(10)), observedAt, confirmedAt, 9_000,
                serviceAuthor().canonicalBytes(), 1, keyPair.getPrivate());

        final ResourceDeleteConfirmedBody body = ResourceDeleteConfirmedBody.decode(mutation.canonicalBody());
        assertEquals(ResourceDeleteConfirmedBody.DeleteOutcome.ALREADY_ABSENT, body.outcome());
        assertEquals(0, body.evidence().observedImmutableVersion().length);
        assertEquals(0, body.evidence().observedEtag().length);
    }

    @Test
    void rejectsProviderIdentityThatDiffersFromRetireIntent() throws Exception {
        final Fixture fixture = fixture();
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final CheckpointResourceV1 different = new CheckpointResourceV1(fixture.resource().recoveryLineageId(),
                fixture.resource().checkpointId(), fixture.resource().objectStoreProfile(),
                fixture.resource().container(), Bytes.utf8("checkpoint/other"), fixture.resource().immutableVersion(),
                fixture.resource().manifestLength(), fixture.resource().manifestSha256());

        assertThrows(IllegalArgumentException.class, () -> CheckpointDeleteConfirmationComposer.compose(
                fixture.intent(), new CheckpointDeleteResult(different,
                        ResourceDeleteConfirmedBody.DeleteOutcome.DELETED, id32(11), id32(12)),
                evidence(3_000, 3_000, "observed"), evidence(3_001, 3_001, "confirmed"), 9_000,
                serviceAuthor().canonicalBytes(), 1, keyPair.getPrivate()));
    }

    @Test
    void requiresConfirmationIntervalToFollowWholeObservationInterval() throws Exception {
        final Fixture fixture = fixture();
        final KeyPair keyPair = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();

        assertThrows(IllegalArgumentException.class, () -> CheckpointDeleteConfirmationComposer.compose(
                fixture.intent(), new CheckpointDeleteResult(fixture.resource(),
                        ResourceDeleteConfirmedBody.DeleteOutcome.DELETED, id32(13), id32(14)),
                evidence(4_000, 4_010, "observed"), evidence(4_005, 4_006, "confirmed"), 9_000,
                serviceAuthor().canonicalBytes(), 1, keyPair.getPrivate()));
    }

    private static Fixture fixture() {
        final ShardId shard = new ShardId(RouteIncarnation.random(), 7);
        final CheckpointResourceV1 resource = new CheckpointResourceV1(id16(1), id16(2),
                new ProfileRefV1(Bytes.utf8("checkpoint-profile"), 1, id32(3), ProfileKindV1.OBJECT_STORE),
                Bytes.utf8("bucket"), Bytes.utf8("checkpoint/manifest"), Bytes.utf8("object-version"), 17,
                id32(4));
        final SourcePosition source = new KafkaSourcePosition(shard, "cluster",
                UUID.fromString("00000000-0000-0000-0000-000000000001"), 12, null, 900);
        final byte[] identity = resource.exactResourceCanonicalBytes();
        final ResourceRetireIntentRecord intent = new ResourceRetireIntentRecord(id32(5), id32(6),
                ResourceKind.CHECKPOINT, identity,
                Bytes.sha256(Bytes.utf8("nereus-delay-resource-identity-v1\0"), identity), 4, 5,
                protectionSet(), source.canonicalBytes());
        return new Fixture(shard, resource, intent);
    }

    private static byte[] protectionSet() {
        final byte[] reference = CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.uint32(output, 1, 3);
            CanonicalProtobuf.bytes(output, 2, id32(20));
            CanonicalProtobuf.uint64Bits(output, 3, 1);
        });
        final byte[] references = CanonicalProtobuf.message(output -> CanonicalProtobuf.bytes(output, 1,
                reference));
        final byte[] digest = Bytes.sha256(Bytes.utf8("nereus-delay-protection-set-v1\0"), references);
        return CanonicalProtobuf.message(output -> {
            CanonicalProtobuf.bytes(output, 1, reference);
            CanonicalProtobuf.bytes(output, 2, digest);
        });
    }

    private static TrustedUtcIntervalEvidence evidence(final long earliest, final long latest,
                                                       final String sourceId) {
        return new TrustedUtcIntervalEvidence(earliest, latest,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK, Bytes.utf8(sourceId), 1, earliest, 1,
                id32((int) earliest), 0, null);
    }

    private static AuthorIdentity serviceAuthor() {
        return AuthorIdentity.service(Bytes.utf8("gc-service"), Bytes.utf8("gc-run"), 1);
    }

    private static byte[] id16(final int seed) {
        return java.util.Arrays.copyOf(id32(seed), 16);
    }

    private static byte[] id32(final int seed) {
        return Bytes.sha256(Bytes.utf8("checkpoint-confirmation-" + seed));
    }

    private record Fixture(ShardId shard, CheckpointResourceV1 resource, ResourceRetireIntentRecord intent) {
    }
}
