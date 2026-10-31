package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.ControlRoleSet;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetControlSnapshotInput;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;

/** Real signed SHARD bootstrap shared by Store tests; external capacity and Owner are explicit fixture authorities. */
final class TargetProtocolStoreTestFixture {
    final TargetQuotaGrantActivation template;
    final TargetQuotaGrantControlRequest request;
    final KafkaSourcePosition origin;
    final KafkaSourcePosition earlier;
    final byte[] lineage = bytes(16, 0xcc);
    final TargetQuotaScope scope;
    final TargetQuotaGrantControlRequest rootRequest;
    final KeyPair keys;
    final KeyPair snapshotKeys;
    final ControlAuthorizationContext actor;
    final InMemoryControlTargetRegistrationAuthority registrations = new InMemoryControlTargetRegistrationAuthority();
    final Signed operation;
    final Signed rootOperation;
    final TargetQuotaGrantControlVerifier.Authority rootAuthority;

    TargetProtocolStoreTestFixture() throws Exception {
        template = TargetQuotaGrantActivation.decode(raw("target.initial.activation"));
        request = template.request();
        origin = (KafkaSourcePosition) template.mutation().source();
        earlier = source(origin, origin.offset() - 1, origin.brokerLogAppendTimeEpochMs() - 1);
        scope = request.next().scope().shardScope();
        final var grant = request.next();
        final long[] resources = grant.limit().resources().amounts();
        Arrays.fill(resources, 0, 15, 1L << 30);
        Arrays.fill(resources, 50, 55, 1L << 30);
        rootRequest = new TargetQuotaGrantControlRequest(
                new TargetQuotaGrant(
                        scope,
                        bytes(32, 0x71),
                        1,
                        grant.accounting(),
                        new TargetQuotaUsage(new CapacityVector(resources), 64, 64, 64, 64),
                        grant.tenantPolicyVersion(),
                        grant.tenantPolicyHash()),
                null,
                null);
        keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        snapshotKeys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        actor = new ControlAuthorizationContext(
                bytes(32, 0xa1), ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR), bytes(32, 0xa2));
        operation = signed(request, bytes(32, 0x72), actor, keys);
        rootOperation = signed(rootRequest, bytes(32, 0x70), actor, keys);
        registrations.register(operation.control());
        registrations.register(rootOperation.control());
        rootAuthority = authority(registrations, keys, actor, earlier, rootRequest, (a, b, c, d) -> {});
    }

    TargetControlSnapshotInput snapshot() {
        return TargetControlSnapshotInput.signed(
                scope,
                lineage,
                rootRequest.next().digest(),
                earlier,
                Bytes.sha256(rootOperation.mutation().canonicalEnvelope()),
                List.of(ProtocolTuple.currentSystemMutation()),
                List.of(),
                1,
                snapshotKeys.getPrivate());
    }

    TargetStoreBootstrap.Initialized bootstrap(ShardStore store, TargetStoreBackend.WriteLimits limits) {
        return TargetStoreBootstrap.commit(
                TargetStoreBootstrap.prepareWithControlSnapshot(
                        store,
                        scope,
                        lineage,
                        limits,
                        budget(),
                        rootOperation.control(),
                        rootOperation.mutation(),
                        earlier,
                        rootAuthority,
                        (a, b, c) -> {},
                        snapshot(),
                        (input, control, mutation, at) -> snapshotKeys.getPublic()),
                (a, b, c) -> guard());
    }

    static TargetQuotaGrantControlVerifier.Authority authority(
            InMemoryControlTargetRegistrationAuthority registrations,
            KeyPair keys,
            ControlAuthorizationContext actor,
            KafkaSourcePosition source,
            TargetQuotaGrantControlRequest request,
            TargetQuotaGrantControlVerifier.CapacityAuthority capacity) {
        return new TargetQuotaGrantControlVerifier.Authority(
                registrations,
                (version, position) -> version == 1 ? keys.getPublic() : null,
                (scope, position) -> {
                    scope.requireRoute(source.shardId(), request.next().scope().tenantScope());
                    if (!source.sameSourceIdentity(position)) {
                        throw new IllegalStateException("foreign source");
                    }
                },
                capacity,
                actor,
                prepared -> true);
    }

    record Signed(PreparedControlOperation control, SystemMutation mutation) {}

    static Signed signed(
            TargetQuotaGrantControlRequest request, byte[] operation, ControlAuthorizationContext actor, KeyPair keys) {
        final var ref = new ControlRef(
                operation,
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final var body = new TargetQuotaGrantControlBody(request.next().scope().shard(), 500, ref, request);
        final var mutation = SystemMutation.signed(
                body.shard(),
                SystemMutationType.APPLY_SHARD_CONTROL,
                body.retryUntil(),
                body.logicalIdentity(),
                body.canonicalBytes(),
                AuthorIdentity.control(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash())
                        .canonicalBytes(),
                1,
                keys.getPrivate());
        final var target = new ControlTargetRef(
                0,
                ControlTargetKind.SHARD,
                new ShardSubject(body.shard()),
                mutation.systemMutationId(),
                mutation.mutationHash());
        return new Signed(
                PreparedControlOperation.prepare(
                        operation,
                        request.operationKind(),
                        new ControlAuthor(
                                actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash()),
                        request.operationRequest(),
                        List.of(target),
                        1,
                        400,
                        1,
                        keys.getPrivate()),
                mutation);
    }

    static KafkaSourcePosition source(KafkaSourcePosition from, long offset, long time) {
        return new KafkaSourcePosition(
                from.shardId(),
                from.authenticatedClusterId(),
                from.nativeTopicUuid(),
                offset,
                from.leaderEpoch(),
                time);
    }

    static BoundedReadBudget budget() {
        return new BoundedReadBudget(2048, 32L << 20, 60_000_000_000L, System::nanoTime);
    }

    static TargetStoreBackend.CommitGuard guard() {
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {}

            @Override
            public void close() {}
        };
    }

    static byte[] bytes(int size, int value) {
        final byte[] result = new byte[size];
        Arrays.fill(result, (byte) value);
        return result;
    }

    private static byte[] raw(String key) throws Exception {
        final var values = new Properties();
        try (var stream = TargetProtocolStoreTestFixture.class.getResourceAsStream(
                "/ndip3/target-quota-grant-vectors.properties")) {
            values.load(stream);
        }
        return HexFormat.of().parseHex(values.getProperty(key));
    }
}
