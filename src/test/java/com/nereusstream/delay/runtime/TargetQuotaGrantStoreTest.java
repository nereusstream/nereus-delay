package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.ownership.InMemoryControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.ControlRoleSet;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Signed source/Store development check; physical capacity and live CommitGuard remain test substitutes. */
class TargetQuotaGrantStoreTest {
    @TempDir
    Path root;

    @Test
    void signedFirstGrantCommitsAllocationResultAndPositionWhileExternalFailureMakesNoWrite() throws Exception {
        final var template = TargetQuotaGrantActivation.decode(raw("target.initial.activation"));
        final var request = template.request();
        final var origin = (KafkaSourcePosition) template.mutation().source();
        final var earlier = source(origin, origin.offset() - 1, origin.brokerLogAppendTimeEpochMs() - 1);
        final byte[] lineage = bytes(16, 0xcc);
        final var stamp = new TargetQuotaMutation(1, earlier, bytes(32, 0x33));
        final var shard = TargetQuotaIncarnation.allocate(
                request.next().scope().shardScope(), request.next().accounting(), lineage, stamp, (a, b) -> {});
        final var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var actor = new ControlAuthorizationContext(
                bytes(32, 0xa1), ControlRoleSet.of(ControlRole.PLATFORM_OPERATOR), bytes(32, 0xa2));
        final var operation = signed(request, bytes(32, 0x72), actor, keys);
        final var registrations = new InMemoryControlTargetRegistrationAuthority();
        registrations.register(operation.control());
        final var config = ShardStoreConfig.defaults(root);
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, shard.scope().shard(), resources)) {
            final var backend = new TargetStoreBackend(
                    store,
                    shard.scope(),
                    shard.identity().accountingIncarnation(),
                    lineage,
                    new TargetStoreBackend.WriteLimits(64, 2 << 20));
            final var bootstrap =
                    new TargetSourceAccounting(shard.scope(), lineage, earlier, stamp.mutationDigest(), 16, 1, 1);
            backend.commit(
                    backend.prepare(
                            budget(),
                            reader -> bootstrap.assemble(
                                    reader,
                                    List.of(reader.replace(
                                            ColumnFamily.META,
                                            shard.key(),
                                            TargetQuotaIncarnation.VALUE_TYPE,
                                            shard.canonicalBytes())))),
                    (a, b, c) -> guard());
            final var applier = new TargetQuotaGrantStore(backend, shard.scope(), lineage, 16, 1);
            final var external = new CommandResolutionException(
                    StableCode.UNAUTHORIZED_SYSTEM_MUTATION, "external capacity unavailable");
            final var unavailable = authority(registrations, keys, actor, origin, request, (a, b, c, d) -> {
                throw external;
            });
            assertSame(
                    external,
                    assertThrows(
                            CommandResolutionException.class,
                            () -> applier.prepareFirst(
                                    budget(), operation.control(), operation.mutation(), origin, unavailable)));
            assertNull(store.get(ColumnFamily.META, template.key()));
            assertNull(store.get(ColumnFamily.DEDUPE, systemKey(operation.mutation())));
            final var allowed = authority(registrations, keys, actor, origin, request, (a, b, c, d) -> {});
            final var result = applier.commit(
                    applier.prepare(budget(), operation.control(), operation.mutation(), origin, allowed),
                    (a, b, c) -> guard(),
                    (a, b) -> guard());
            assertEquals(ApplyStatus.APPLIED, result.applyStatus());
            assertEquals(StableCode.OK, result.stableCode());
            final var activation = TargetQuotaGrantActivation.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, template.key()), TargetQuotaGrantActivation.VALUE_TYPE)
                    .payload());
            assertEquals(2, activation.mutation().sequence());
            assertNotNull(store.get(ColumnFamily.META, activation.allocation().key()));
            final var first = TargetResultRecord.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.DEDUPE, systemKey(operation.mutation())),
                            TargetResultRecord.VALUE_TYPE)
                    .payload());
            assertEquals(result, SystemMutationResult.decode(first.typedPayload()));
            assertNotNull(first.allocation());
            final var position = TargetResultRecord.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.DEDUPE,
                                    Bytes.concat(
                                            new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1},
                                            origin.canonicalBytes())),
                            TargetResultRecord.VALUE_TYPE)
                    .payload());
            position.requireFirst(first);
            assertThrows(
                    IllegalStateException.class,
                    () -> applier.prepareFirst(
                            budget(),
                            operation.control(),
                            operation.mutation(),
                            source(origin, origin.offset() + 1, origin.brokerLogAppendTimeEpochMs() + 1),
                            allowed));
            final byte[] frozenFirst = store.get(ColumnFamily.DEDUPE, systemKey(operation.mutation()));
            final byte[] frozenActivation = store.get(ColumnFamily.META, template.key());
            final long nativeBeforeReplay = store.latestSequenceNumber();
            final var same = applier.prepare(budget(), operation.control(), operation.mutation(), origin, unavailable);
            assertEquals(
                    result,
                    applier.commit(
                            same,
                            (a, b, c) -> {
                                throw new AssertionError("same position wrote");
                            },
                            (a, b) -> guard()));
            assertEquals(nativeBeforeReplay, store.latestSequenceNumber());
            assertThrows(
                    IllegalStateException.class, () -> applier.commit(same, (a, b, c) -> guard(), (a, b) -> guard()));
            final var staleRead =
                    applier.prepare(budget(), operation.control(), operation.mutation(), origin, unavailable);
            final byte[] aggregateKey = TargetQuotaAggregate.genesis(
                            shard.identity().shard(), shard.identity().accountingIncarnation())
                    .key();
            final var beforeDuplicate = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, aggregateKey), TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            final var later = source(origin, origin.offset() + 1, origin.brokerLogAppendTimeEpochMs() + 1);
            assertEquals(
                    result,
                    applier.commit(
                            applier.prepare(budget(), operation.control(), operation.mutation(), later, unavailable),
                            (a, b, c) -> guard(),
                            (a, b) -> {
                                throw new AssertionError("later duplicate did not write");
                            }));
            assertEquals(3, store.shardMutationSequence());
            assertThrows(
                    IllegalStateException.class,
                    () -> applier.commit(staleRead, (a, b, c) -> guard(), (a, b) -> guard()));
            final var physical = TargetResultRecord.decode(TargetValueEnvelope.decode(
                            store.get(
                                    ColumnFamily.DEDUPE,
                                    Bytes.concat(
                                            new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, 1},
                                            later.canonicalBytes())),
                            TargetResultRecord.VALUE_TYPE)
                    .payload());
            physical.requireFirst(first);
            final var afterDuplicate = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, aggregateKey), TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            assertEquals(
                    beforeDuplicate.usage().resources().add(physical.recordCharge()),
                    afterDuplicate.usage().resources());
            final var expiredPosition = source(origin, origin.offset() + 2, 501);
            final var expiredDuplicate = applier.commit(
                    applier.prepare(budget(), operation.control(), operation.mutation(), expiredPosition, unavailable),
                    (a, b, c) -> guard(),
                    (a, b) -> guard());
            assertEquals(ApplyStatus.REJECTED, expiredDuplicate.applyStatus());
            assertEquals(StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED, expiredDuplicate.stableCode());
            assertArrayEquals(frozenFirst, store.get(ColumnFamily.DEDUPE, systemKey(operation.mutation())));
            assertArrayEquals(frozenActivation, store.get(ColumnFamily.META, template.key()));
            final long nativeBeforeExpiredReplay = store.latestSequenceNumber();
            assertEquals(
                    expiredDuplicate,
                    applier.commit(
                            applier.prepare(
                                    budget(), operation.control(), operation.mutation(), expiredPosition, unavailable),
                            (a, b, c) -> guard(),
                            (a, b) -> guard()));
            assertEquals(nativeBeforeExpiredReplay, store.latestSequenceNumber());
            final var originalMutation = operation.mutation();
            final var resigned = SystemMutation.signed(
                    originalMutation.shardId(),
                    originalMutation.type(),
                    originalMutation.retryUntilEpochMs(),
                    originalMutation.logicalOperationIdentity(),
                    originalMutation.canonicalBody(),
                    originalMutation.authorIdentity(),
                    2,
                    keys.getPrivate());
            assertThrows(
                    IllegalStateException.class,
                    () -> applier.prepare(budget(), operation.control(), resigned, expiredPosition, unavailable));
            final var expired = signed(request, bytes(32, 0x73), actor, keys);
            registrations.register(expired.control());
            final var rejection = applier.commit(
                    applier.prepareFirst(
                            budget(),
                            expired.control(),
                            expired.mutation(),
                            source(origin, origin.offset() + 3, 502),
                            allowed),
                    (a, b, c) -> guard());
            assertEquals(ApplyStatus.REJECTED, rejection.applyStatus());
            assertEquals(StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED, rejection.stableCode());
            final var unchanged = TargetQuotaGrantActivation.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, template.key()), TargetQuotaGrantActivation.VALUE_TYPE)
                    .payload());
            assertEquals(activation.mutation(), unchanged.mutation());
        }
    }

    private static TargetQuotaGrantControlVerifier.Authority authority(
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

    private record Signed(PreparedControlOperation control, SystemMutation mutation) {}

    private static Signed signed(
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
        final var control = PreparedControlOperation.prepare(
                operation,
                request.operationKind(),
                new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash()),
                request.operationRequest(),
                List.of(target),
                1,
                400,
                1,
                keys.getPrivate());
        return new Signed(control, mutation);
    }

    private static byte[] systemKey(SystemMutation mutation) {
        return Bytes.concat(new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, 1}, mutation.systemMutationId());
    }

    private static KafkaSourcePosition source(KafkaSourcePosition from, long offset, long time) {
        return new KafkaSourcePosition(
                from.shardId(),
                from.authenticatedClusterId(),
                from.nativeTopicUuid(),
                offset,
                from.leaderEpoch(),
                time);
    }

    private static BoundedReadBudget budget() {
        return new BoundedReadBudget(2048, 32L << 20, 60_000_000_000L, System::nanoTime);
    }

    private static TargetStoreBackend.CommitGuard guard() {
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {}

            @Override
            public void close() {}
        };
    }

    private static byte[] bytes(int size, int value) {
        final var data = new byte[size];
        Arrays.fill(data, (byte) value);
        return data;
    }

    private static byte[] raw(String key) throws Exception {
        final var values = new Properties();
        try (var stream =
                TargetQuotaGrantStoreTest.class.getResourceAsStream("/ndip3/target-quota-grant-vectors.properties")) {
            values.load(stream);
        }
        return HexFormat.of().parseHex(values.getProperty(key));
    }
}
