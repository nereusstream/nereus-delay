package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Minimal real Store accounting smoke; it does not certify signing, grants or retirement authority. */
class TargetSourceAccountingTest {
    @TempDir
    Path root;

    @Test
    void bootstrapInventoryAndResultRecordsProduceActualPrimaryMirrorsAndTotals() throws Exception {
        final var descriptors = properties("target-quota-incarnation");
        final var results = properties("target-result");
        final var shard = TargetQuotaIncarnation.decode(raw(descriptors, "shard.open"));
        final var target = TargetQuotaIncarnation.decode(raw(descriptors, "target.open"));
        final var command = TargetResultRecord.decode(raw(results, "command"));
        final var result = TargetResultRecord.decode(raw(results, "result"));
        final var position = TargetResultRecord.decode(raw(results, "position"));
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
            final var bootstrap = new TargetSourceAccounting(
                    scope,
                    shard.recoveryLineage(),
                    shard.allocation().source(),
                    shard.allocation().mutationDigest(),
                    16,
                    4,
                    1);
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
                                                    target.canonicalBytes())))),
                    (a, b, c) -> guard());
            final byte[] bookKey = TargetQuotaAggregate.genesis(
                            scope.shard(), shard.identity().accountingIncarnation())
                    .key();
            bookKey[0] = TargetKeyCodec.QUOTA_BOOKKEEPING_TAG;
            final byte[] rootBefore = store.get(ColumnFamily.META, bookKey);
            final var bookkeeping = TargetQuotaBookkeeping.decode(
                    TargetValueEnvelope.decode(rootBefore, TargetQuotaBookkeeping.VALUE_TYPE)
                            .payload());
            assertEquals(new TargetQuotaBookkeeping.Inventory(4, 1, 0), bookkeeping.inventory());
            final var next = new TargetSourceAccounting(
                    scope,
                    shard.recoveryLineage(),
                    command.mutation().source(),
                    command.mutation().mutationDigest(),
                    16,
                    4,
                    1);
            backend.commit(
                    backend.prepare(
                            budget(),
                            reader -> next.assemble(
                                    reader,
                                    List.of(
                                            reader.replace(
                                                    ColumnFamily.DEDUPE,
                                                    command.key(),
                                                    TargetResultRecord.VALUE_TYPE,
                                                    command.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.DEDUPE,
                                                    result.key(),
                                                    TargetResultRecord.VALUE_TYPE,
                                                    result.canonicalBytes()),
                                            reader.replace(
                                                    ColumnFamily.DEDUPE,
                                                    position.key(),
                                                    TargetResultRecord.VALUE_TYPE,
                                                    position.canonicalBytes())))),
                    (a, b, c) -> guard());
            final CapacityVector targetResources = target.ownContribution()
                    .resources()
                    .add(command.recordCharge())
                    .add(result.recordCharge());
            final CapacityVector shardResources = shard.ownContribution()
                    .resources()
                    .add(bookkeeping.charge())
                    .add(position.recordCharge());
            final var primary = counter(store, target.identity().key());
            final var mirror = counter(store, target.tenantIdentity().key());
            assertEquals(targetResources, primary.usage().resources());
            assertEquals(targetResources, mirror.usage().resources());
            assertEquals(1, primary.usage().accountingIncarnations());
            assertEquals(0, mirror.usage().accountingIncarnations());
            assertEquals(
                    shardResources,
                    counter(store, shard.identity().key()).usage().resources());
            final byte[] aggregateKey = TargetQuotaAggregate.genesis(
                            scope.shard(), shard.identity().accountingIncarnation())
                    .key();
            final var aggregate = TargetQuotaAggregate.decode(TargetValueEnvelope.decode(
                            store.get(ColumnFamily.META, aggregateKey), TargetQuotaAggregate.VALUE_TYPE)
                    .payload());
            assertEquals(targetResources.add(shardResources), aggregate.usage().resources());
            assertEquals(2, aggregate.usage().accountingIncarnations());
            assertArrayEquals(rootBefore, store.get(ColumnFamily.META, bookKey));
            final byte[] totalKey = com.nereusstream.delay.protocol.Bytes.concat(
                    new byte[] {TargetKeyCodec.QUOTA_TOTAL_TAG, 1},
                    target.scope().keySuffix());
            final var total = TargetQuotaTotal.decode(
                    TargetValueEnvelope.decode(store.get(ColumnFamily.META, totalKey), TargetQuotaTotal.VALUE_TYPE)
                            .payload());
            assertEquals(primary.usage(), total.usage());
            assertEquals(2, store.shardMutationSequence());
        }
    }

    private static TargetQuotaCounter counter(final ShardStore store, final byte[] key) {
        return TargetQuotaCounter.decode(
                TargetValueEnvelope.decode(store.get(ColumnFamily.META, key), TargetQuotaCounter.VALUE_TYPE)
                        .payload());
    }

    private static BoundedReadBudget budget() {
        return new BoundedReadBudget(500, 1 << 20, 10_000_000_000L, System::nanoTime);
    }

    private static Properties properties(final String name) throws Exception {
        final var result = new Properties();
        try (var input =
                TargetSourceAccountingTest.class.getResourceAsStream("/ndip3/" + name + "-vectors.properties")) {
            result.load(input);
        }
        return result;
    }

    private static byte[] raw(final Properties values, final String key) {
        return HexFormat.of().parseHex(values.getProperty(key));
    }

    private static TargetStoreBackend.CommitGuard guard() {
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {}

            @Override
            public void close() {}
        };
    }
}
