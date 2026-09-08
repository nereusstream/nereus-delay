package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Actual Store grant/read-view development check; labels, activation and CommitGuard are test inputs. */
class TargetQuotaStoreGateTest {
    @TempDir
    Path root;

    @Test
    void targetLimitRejectsBeforeWriteAndChangedGrantInvalidatesAnExistingWorkPlan() throws Exception {
        final var shard = TargetQuotaIncarnation.decode(raw("target-quota-incarnation", "shard.open"));
        final var target = TargetQuotaIncarnation.decode(raw("target-quota-incarnation", "target.open"));
        final var command = TargetResultRecord.decode(raw("target-result", "command"));
        final var grant = activation(target, null, target.ownContribution(), shard.allocation(), target);
        final var shardGrant = activation(shard, null, unlimited(), shard.allocation(), null);
        final var scope = shard.scope();
        final var config = ShardStoreConfig.defaults(root);
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, scope.shard(), resources)) {
            final var backend = new TargetStoreBackend(
                    store,
                    scope,
                    shard.identity().accountingIncarnation(),
                    shard.recoveryLineage(),
                    new TargetStoreBackend.WriteLimits(64, 1 << 20));
            final var bootstrap = accounting(shard, shard.allocation());
            backend.commit(
                    backend.prepare(
                            budget(),
                            reader -> bootstrap.assemble(
                                    reader,
                                    List.of(
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    shard.key(),
                                                    TargetQuotaIncarnation.VALUE_TYPE,
                                                    shard.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    target.key(),
                                                    TargetQuotaIncarnation.VALUE_TYPE,
                                                    target.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    grant.key(),
                                                    TargetQuotaGrantActivation.VALUE_TYPE,
                                                    grant.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.META,
                                                    shardGrant.key(),
                                                    TargetQuotaGrantActivation.VALUE_TYPE,
                                                    shardGrant.canonicalBytes())))),
                    (a, b, c) -> guard());
            final var gate = new TargetQuotaStoreGate(scope, shard.recoveryLineage(), 1);
            final var sourceAccounting = accounting(shard, command.mutation());
            final var denied = assertThrows(
                    TargetQuotaStoreGate.Rejected.class,
                    () -> backend.prepare(budget(), reader -> {
                        final var mutation = sourceAccounting.assemble(
                                reader,
                                List.of(reader.replace(
                                        ColumnFamily.DEDUPE,
                                        command.key(),
                                        TargetResultRecord.VALUE_TYPE,
                                        command.canonicalBytes())));
                        gate.check(
                                reader, mutation, TargetQuotaGrantGate.Operation.FIRST_SCHEDULE, target.accounting());
                        return mutation;
                    }));
            assertEquals(TargetQuotaGrantGate.Decision.TARGET_LIMIT, denied.decision());
            assertEquals(target.scope(), denied.scope());
            assertNull(store.get(ColumnFamily.DEDUPE, command.key()));
            // This only exercises the existing-work policy branch; it is not a real Outcome authorization.
            final var oldPlan = backend.prepare(budget(), reader -> {
                final var mutation = sourceAccounting.assemble(
                        reader,
                        List.of(reader.replace(
                                ColumnFamily.DEDUPE,
                                command.key(),
                                TargetResultRecord.VALUE_TYPE,
                                command.canonicalBytes())));
                assertEquals(
                        TargetQuotaGrantGate.Decision.EXISTING_WORK_DRAIN,
                        gate.check(reader, mutation, TargetQuotaGrantGate.Operation.OUTCOME, null)
                                .getFirst()
                                .decision());
                return mutation;
            });
            final var replacement = activation(target, grant.grant(), unlimited(), command.mutation(), target);
            backend.commit(
                    backend.prepare(
                            budget(),
                            reader -> sourceAccounting.assemble(
                                    reader,
                                    List.of(reader.replace(
                                            ColumnFamily.META,
                                            replacement.key(),
                                            TargetQuotaGrantActivation.VALUE_TYPE,
                                            replacement.canonicalBytes())))),
                    (a, b, c) -> guard());
            assertThrows(IllegalStateException.class, () -> backend.commit(oldPlan, (a, b, c) -> guard()));
            assertNull(store.get(ColumnFamily.DEDUPE, command.key()));
        }
    }

    private static TargetSourceAccounting accounting(TargetQuotaIncarnation shard, TargetQuotaMutation mutation) {
        return new TargetSourceAccounting(
                shard.scope(), shard.recoveryLineage(), mutation.source(), mutation.mutationDigest(), 16, 2, 1);
    }

    private static TargetQuotaGrantActivation activation(
            TargetQuotaIncarnation owner,
            TargetQuotaGrant prior,
            TargetQuotaUsage limit,
            TargetQuotaMutation stamp,
            TargetQuotaIncarnation origin) {
        final var grant = new TargetQuotaGrant(
                owner.scope(),
                bytes(32, 0x77),
                prior == null ? 1 : prior.version() + 1,
                owner.accounting(),
                limit,
                1,
                bytes(32, 0x88));
        final var request = new TargetQuotaGrantControlRequest(grant, prior, null);
        final var ref = new ControlRef(
                bytes(32, 0x99),
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final byte[] hash = bytes(32, 0xaa);
        return new TargetQuotaGrantActivation(
                request,
                ref,
                stamp,
                SystemMutation.computeSystemMutationId(
                        owner.identity().shard(),
                        SystemMutationType.APPLY_SHARD_CONTROL,
                        ref.logicalOperationIdentity(TargetQuotaGrantControlRequest.CONTROL_KIND),
                        hash),
                hash,
                origin);
    }

    private static TargetQuotaUsage unlimited() {
        final long[] amounts = new long[CapacityDimension.COUNT];
        amounts[CapacityDimension.LOGICAL_STATE_BYTES.wireValue() - 1] = Long.MAX_VALUE;
        amounts[CapacityDimension.RESULT_BYTES.wireValue() - 1] = Long.MAX_VALUE;
        amounts[CapacityDimension.RESULT_RECORDS.wireValue() - 1] = Long.MAX_VALUE;
        amounts[CapacityDimension.EVIDENCE_BYTES.wireValue() - 1] = Long.MAX_VALUE;
        amounts[CapacityDimension.EVIDENCE_RECORDS.wireValue() - 1] = Long.MAX_VALUE;
        return new TargetQuotaUsage(new CapacityVector(amounts), 1, 64, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    private static BoundedReadBudget budget() {
        return new BoundedReadBudget(2048, 16L << 20, 60_000_000_000L, System::nanoTime);
    }

    private static TargetStoreBackend.CommitGuard guard() {
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {}

            @Override
            public void close() {}
        };
    }

    private static byte[] bytes(int n, int value) {
        final byte[] bytes = new byte[n];
        Arrays.fill(bytes, (byte) value);
        return bytes;
    }

    private static byte[] raw(String file, String key) throws Exception {
        final var values = new Properties();
        try (var stream =
                TargetQuotaStoreGateTest.class.getResourceAsStream("/ndip3/" + file + "-vectors.properties")) {
            values.load(stream);
        }
        return HexFormat.of().parseHex(values.getProperty(key));
    }
}
