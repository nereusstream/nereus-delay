package com.nereusstream.delay.protocol;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.runtime.TargetCloseVerifier;
import com.nereusstream.delay.store.TargetValueEnvelope;
import com.nereusstream.delay.store.ValueEnvelope;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TargetCloseProtocolTest {
    private static final TargetPartitionId TARGET = new TargetPartitionId(bytes(32, 1));
    private static final byte[] INCARNATION = bytes(16, 2);
    private static final byte[] OPERATION = bytes(32, 3);

    @Test
    void bindsEveryFrozenShardAndRequiresPlatformAuthority() throws Exception {
        final var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var request = request(2);
        final var platform = actor(ControlRole.PLATFORM_OPERATOR);
        final var signed = sign(request, platform, keys);
        assertTrue(signed.prepared().verifySignature(keys.getPublic()));
        assertDoesNotThrow(() -> ControlOperationAuthorization.authorize(signed.prepared(), platform, p -> true));
        for (int i = 0; i < 2; i++) {
            final int index = i;
            final var mutation = signed.mutations().get(i);
            final var body = TargetCloseBody.decode(mutation.canonicalBody());
            assertEquals(i, body.controlRef().targetIndex());
            assertEquals(request, body.request());
            assertDoesNotThrow(() -> ControlTargetMutationBinding.validate(
                    signed.prepared(), signed.prepared().targets().get(index), mutation));
            assertThrows(IllegalArgumentException.class, () -> ApplyShardControlBody.decode(mutation.canonicalBody()));
        }
        assertThrows(
                IllegalArgumentException.class,
                () -> prepare(
                        request, platform, keys, signed.prepared().targets().subList(0, 1)));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlTargetMutationBinding.validate(
                        signed.prepared(),
                        signed.prepared().targets().get(0),
                        signed.mutations().get(1)));
        final var tenant = actor(ControlRole.TENANT_POLICY_ADMINISTRATOR);
        final var tenantSigned = sign(request, tenant, keys);
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationAuthorization.authorize(tenantSigned.prepared(), tenant, p -> true));
        assertThrows(
                IllegalArgumentException.class,
                () -> ControlOperationAuthorization.authorize(signed.prepared(), platform, p -> false));
    }

    @Test
    void boundsCompleteCoverageAndPreservesUnsignedVersions() {
        final var request = request(TargetCloseRequest.MAX_SHARDS);
        assertEquals(request, TargetCloseRequest.decode(request.canonicalBytes()));
        final int index = request.shards().size() - 1;
        final var body = new TargetCloseBody(
                request.shards().get(index).shard(),
                Long.MAX_VALUE,
                new ControlRef(
                        OPERATION,
                        PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                        index),
                request);
        assertArrayEquals(
                body.canonicalBytes(),
                TargetCloseBody.decode(body.canonicalBytes()).canonicalBytes());
        assertTrue(body.canonicalBytes().length <= TargetCloseBody.MAX_CANONICAL_BYTES);
        assertThrows(IllegalArgumentException.class, () -> request(TargetCloseRequest.MAX_SHARDS + 1));
        assertThrows(IllegalArgumentException.class, () -> new TargetCloseRequest(TARGET, List.of(), policy()));
        assertThrows(
                IllegalArgumentException.class,
                () -> new TargetCloseRequest(
                        TARGET,
                        List.of(request.shards().get(1), request.shards().get(0)),
                        policy()));
        assertThrows(
                IllegalArgumentException.class, () -> new TargetCloseRequest.ShardTarget(shard(0), INCARNATION, -1));
        assertThrows(
                IllegalArgumentException.class, () -> new TargetCloseRequest.ShardTarget(shard(0), INCARNATION, 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetCloseRequest.decode(Bytes.concat(request(1).canonicalBytes(), new byte[] {40, 1})));
    }

    @Test
    void markerRequiresSourceStampExactStoreIdentityAndTargetReader() {
        final var request = request(1);
        final var body = new TargetCloseBody(
                shard(0),
                500,
                new ControlRef(
                        OPERATION,
                        PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                        0),
                request);
        final var source = source(shard(0));
        final var marker =
                new TargetCloseRecord(body, -1, new TargetQuotaMutation(1, source, bytes(32, 4)), INCARNATION);
        final byte[] encoded = TargetValueEnvelope.encode(TargetCloseRecord.VALUE_TYPE, marker.canonicalBytes());
        assertArrayEquals(
                marker.canonicalBytes(),
                TargetCloseRecord.decodeForStore(
                                marker.key(),
                                TargetValueEnvelope.decode(encoded, TargetCloseRecord.VALUE_TYPE)
                                        .payload(),
                                shard(0),
                                INCARNATION)
                        .canonicalBytes());
        assertThrows(IllegalArgumentException.class, () -> ValueEnvelope.decodeAny(encoded));
        assertThrows(
                IllegalStateException.class,
                () -> TargetCloseRecord.decodeForStore(marker.key(), marker.canonicalBytes(), shard(1), INCARNATION));
        assertThrows(
                IllegalStateException.class,
                () -> TargetCloseRecord.decodeForStore(marker.key(), marker.canonicalBytes(), shard(0), bytes(16, 9)));
        final byte[] corrupt = marker.canonicalBytes();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetCloseRecord.decode(corrupt));
    }

    @Test
    void closeCursorBindsFirstMarkerAndMonotonicLocalProgress() {
        final var request = request(1);
        final var body = new TargetCloseBody(
                shard(0),
                500,
                new ControlRef(
                        OPERATION,
                        PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                        0),
                request);
        final var source = source(shard(0));
        final var marker =
                new TargetCloseRecord(body, -1, new TargetQuotaMutation(1, source, bytes(32, 4)), INCARNATION);
        final var initial = TargetCloseCursorRecord.initial(marker);
        assertTrue(initial.canonicalBytes().length <= TargetCloseCursorRecord.MAX_CANONICAL_BYTES);
        assertArrayEquals(
                initial.canonicalBytes(),
                TargetCloseCursorRecord.decodeForStore(initial.key(), initial.canonicalBytes(), marker, INCARNATION)
                        .canonicalBytes());
        final var message = DelayMessageId.random(shard(0));
        final var closureStamp = new TargetQuotaMutation(1, source, bytes(32, 5), 1, false, true);
        final var complete = initial.advance(message, true, closureStamp);
        assertTrue(complete.complete());
        assertArrayEquals(message.bytes(), complete.afterMessageId());
        assertThrows(IllegalStateException.class, () -> complete.advance(message, true, closureStamp));
        assertThrows(
                IllegalStateException.class,
                () -> TargetCloseCursorRecord.decodeForStore(
                        initial.key(), complete.canonicalBytes(), marker, bytes(16, 9)));
        final var other = new TargetCloseRecord(body, 0, marker.mutation(), INCARNATION);
        assertThrows(IllegalStateException.class, () -> complete.requireMarker(other));
        final var cursorStamp = new TargetQuotaMutation(1, source, bytes(32, 6), 1, false, false, true);
        final var empty = initial.completeEmpty(cursorStamp);
        assertTrue(empty.complete());
        assertEquals(2, empty.revision());
        assertThrows(IllegalStateException.class, () -> empty.completeEmpty(cursorStamp));
        final byte[] corrupt = empty.canonicalBytes();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetCloseCursorRecord.decode(corrupt));
    }

    @Test
    void externalCoverageFailurePropagatesAndUntrustedSignatureIsDenied() throws Exception {
        final var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var platform = actor(ControlRole.PLATFORM_OPERATOR);
        final var signed = sign(request(1), platform, keys);
        final var registrations = new InMemoryControlTargetRegistrationAuthority();
        registrations.register(signed.prepared());
        final var scope = new TargetQuotaScope(shard(0), bytes(32, 5), null);
        final var failure = new IllegalStateException("coverage snapshot unavailable");
        final var unavailable = new TargetCloseVerifier.Authority(
                registrations,
                (v, s) -> keys.getPublic(),
                (s, r, p, q) -> {
                    throw failure;
                },
                platform,
                p -> true);
        assertSame(
                failure,
                assertThrows(
                        IllegalStateException.class,
                        () -> TargetCloseVerifier.decideFirstApplication(
                                scope,
                                signed.prepared(),
                                signed.mutations().getFirst(),
                                source(shard(0)),
                                null,
                                unavailable)));
        final var otherKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var untrusted = new TargetCloseVerifier.Authority(
                registrations,
                (v, s) -> otherKeys.getPublic(),
                (s, r, p, q) -> fail("invalid signature must not reach coverage"),
                platform,
                p -> true);
        assertEquals(
                StableCode.UNAUTHORIZED_SYSTEM_MUTATION,
                TargetCloseVerifier.decideFirstApplication(
                                scope,
                                signed.prepared(),
                                signed.mutations().getFirst(),
                                source(shard(0)),
                                null,
                                untrusted)
                        .rejection());
    }

    private record Signed(PreparedControlOperation prepared, List<SystemMutation> mutations) {}

    private static Signed sign(TargetCloseRequest request, ControlAuthorizationContext actor, KeyPair keys) {
        final var mutations = new ArrayList<SystemMutation>();
        final var targets = new ArrayList<ControlTargetRef>();
        for (int i = 0; i < request.shards().size(); i++) {
            final var body = new TargetCloseBody(
                    request.shards().get(i).shard(),
                    500,
                    new ControlRef(
                            OPERATION,
                            PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                            i),
                    request);
            final var mutation = SystemMutation.signed(
                    body.shard(),
                    SystemMutationType.APPLY_SHARD_CONTROL,
                    500,
                    body.logicalIdentity(),
                    body.canonicalBytes(),
                    AuthorIdentity.control(
                                    actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash())
                            .canonicalBytes(),
                    1,
                    keys.getPrivate());
            mutations.add(mutation);
            targets.add(new ControlTargetRef(
                    i,
                    ControlTargetKind.SHARD,
                    new ShardSubject(body.shard()),
                    mutation.systemMutationId(),
                    mutation.mutationHash()));
        }
        return new Signed(prepare(request, actor, keys, targets), List.copyOf(mutations));
    }

    private static PreparedControlOperation prepare(
            TargetCloseRequest request,
            ControlAuthorizationContext actor,
            KeyPair keys,
            List<ControlTargetRef> targets) {
        return PreparedControlOperation.prepare(
                OPERATION,
                request.operationKind(),
                new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash()),
                request.operationRequest(),
                targets,
                1,
                400,
                1,
                keys.getPrivate());
    }

    private static TargetCloseRequest request(int count) {
        final var shards = new ArrayList<TargetCloseRequest.ShardTarget>();
        for (int i = 0; i < count; i++) {
            shards.add(new TargetCloseRequest.ShardTarget(shard(i), INCARNATION, Long.MIN_VALUE));
        }
        shards.sort((a, b) -> Arrays.compareUnsigned(
                new ShardSubject(a.shard()).canonicalBytes(), new ShardSubject(b.shard()).canonicalBytes()));
        return new TargetCloseRequest(TARGET, shards, policy());
    }

    private static CloseLaneRequest policy() {
        return new CloseLaneRequest(
                new ControlReason(ControlReasonKind.OPERATOR_REQUEST, null, null),
                ClosePolicy._FREEZE_UNADMITTED_AND_PRESERVE_ADMITTED,
                false,
                AcknowledgementSet.empty());
    }

    private static ShardId shard(int partition) {
        return new ShardId(new RouteIncarnation(bytes(16, 6)), partition);
    }

    private static KafkaSourcePosition source(ShardId shard) {
        return new KafkaSourcePosition(shard, "cluster", new UUID(1, 2), 1, 1, 100);
    }

    private static ControlAuthorizationContext actor(ControlRole role) {
        return new ControlAuthorizationContext(bytes(32, 7), ControlRoleSet.of(role), bytes(32, 8));
    }

    private static byte[] bytes(int size, int value) {
        final byte[] result = new byte[size];
        Arrays.fill(result, (byte) value);
        return result;
    }
}
