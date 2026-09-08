package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.TargetDomainState;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Development projection smoke; accounting and authorization are deliberate test doubles, not certification. */
class TargetMessageStoreTest {
    @TempDir
    Path root;

    @Test
    void initialNativeGraphAndTerminalRemovalCommitWithExactHeadsAndSource() throws Exception {
        final var vectors = new Properties();
        try (var stream = getClass().getResourceAsStream("/ndip3/target-identity-vectors.properties")) {
            vectors.load(stream);
        }
        final var message = TargetMessageRecord.decode(raw(vectors, "message.initial"));
        final var physical = CanonicalTargetPartition.decode(raw(vectors, "pulsar.canonical"));
        final var template = TargetQueueState.decode(raw(vectors, "queue.active"));
        final var domain = template.domains().getFirst();
        final var queue = new TargetQueueState(
                template.targetId(),
                1,
                template.controlVersion(),
                template.admissionState(),
                template.accountingIncarnation(),
                template.nativeIndexLeadCapMs(),
                List.of(new TargetDomainState(
                        domain.domain(),
                        domain.lifecycle(),
                        domain.dispatchCompatibilityRef(),
                        domain.controlScopeRef(),
                        domain.nativePolicyScopeRef(),
                        null,
                        null)));
        final var config = ShardStoreConfig.defaults(root);
        final var shard = message.scheduleSource().shardId();
        final var scope = new TargetQuotaScope(shard, bytes(32, 0x22), null);
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(config, shard, resources)) {
            final var backend = new TargetStoreBackend(
                    store, scope, bytes(16, 0x33), bytes(16, 0x44), new TargetStoreBackend.WriteLimits(64, 1 << 20));
            final var messages = new TargetMessageStore(backend, 2, 4, 1);
            final TargetMessageStore.AccountingPlanner accounting = (reader, edits) -> {
                final var base = (KafkaSourcePosition) message.scheduleSource();
                final long sequence = reader.sourceSequence();
                final var source = new KafkaSourcePosition(
                        shard,
                        base.authenticatedClusterId(),
                        base.nativeTopicUuid(),
                        base.offset() + sequence,
                        base.leaderEpoch(),
                        base.brokerLogAppendTimeEpochMs() + sequence);
                final var delta = TargetQuotaDelta.prepare(
                        reader.aggregate(),
                        sequence,
                        reader.source(),
                        source,
                        bytes(32, (int) sequence + 1),
                        List.of(),
                        4,
                        reader::counter);
                return TargetQuotaTotalsDelta.prepare(delta, scope, 2, reader::total);
            };
            messages.apply(
                    budget(),
                    reader -> new TargetMessageStore.Input(
                            List.of(new TargetMessageStore.Transition(null, message)),
                            List.of(),
                            List.of(
                                    reader.replace(
                                            ColumnFamily.META,
                                            TargetKeyCodec.identity(physical.id()),
                                            CanonicalTargetPartition.VALUE_TYPE,
                                            physical.canonicalBytes()),
                                    reader.replace(
                                            ColumnFamily.META,
                                            TargetKeyCodec.state(queue.targetId()),
                                            TargetQueueState.VALUE_TYPE,
                                            queue.canonicalBytes()))),
                    accounting,
                    (a, b, c) -> guard());
            final var persisted = readQueue(store, queue);
            assertArrayEquals(
                    message.runtime().timeline().ordinaryKey(),
                    persisted.domains().getFirst().ordinaryHead().key());
            assertArrayEquals(
                    message.runtime().timeline().nativeKey(),
                    persisted.domains().getFirst().nativeHead().key());
            final var runtime = new TargetGenerationRuntimeIndex(
                    message.locator().generation(),
                    GenerationAggregateState.CANCELED,
                    CurrentSendWorkKind.NONE,
                    null,
                    null,
                    null,
                    List.of(),
                    0,
                    0,
                    false,
                    message.runtime().runtimeRevision() + 1);
            final var terminal = new TargetMessageRecord(
                    message.locator(),
                    message.stateVersion() + 1,
                    message.deliverAtEpochMs(),
                    message.expireAtEpochMs(),
                    message.retryEligibilityAtEpochMs(),
                    message.nativeDeliveryPolicy(),
                    message.scheduleSource(),
                    message.inlinePayload(),
                    message.payloadReference(),
                    runtime);
            final var prepared = messages.prepare(
                    budget(),
                    reader -> new TargetMessageStore.Input(
                            List.of(new TargetMessageStore.Transition(message, terminal)), List.of(), List.of()),
                    accounting);
            backend.commit(prepared, (a, b, c) -> guard());
            assertThrows(IllegalStateException.class, () -> backend.commit(prepared, (a, b, c) -> guard()));
            assertNull(store.get(
                    ColumnFamily.TIMELINE, message.runtime().timeline().ordinaryKey()));
            assertNull(store.get(
                    ColumnFamily.TIMELINE, message.runtime().timeline().nativeKey()));
            assertNull(store.get(
                    ColumnFamily.TIMELINE,
                    new TargetExpiryRef(message.locator(), message.expireAtEpochMs()).encodedKey()));
            final var empty = readQueue(store, queue);
            assertEquals(2, empty.headRevision());
            assertNull(empty.domains().getFirst().ordinaryHead());
            assertNull(empty.domains().getFirst().nativeHead());
            assertEquals(2, store.shardMutationSequence());
            assertArrayEquals(
                    terminal.canonicalBytes(),
                    TargetValueEnvelope.decode(
                                    store.get(ColumnFamily.ID, message.encodedKey()), TargetMessageRecord.VALUE_TYPE)
                            .payload());
        }
    }

    private static TargetQueueState readQueue(final ShardStore store, final TargetQueueState queue) {
        return TargetQueueState.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.META, TargetKeyCodec.state(queue.targetId())),
                        TargetQueueState.VALUE_TYPE)
                .payload());
    }

    private static BoundedReadBudget budget() {
        return new BoundedReadBudget(200, 1 << 20, 10_000_000_000L, System::nanoTime);
    }

    private static TargetStoreBackend.CommitGuard guard() {
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {}

            @Override
            public void close() {}
        };
    }

    private static byte[] raw(final Properties values, final String key) {
        return HexFormat.of().parseHex(values.getProperty(key));
    }

    private static byte[] bytes(final int length, final int value) {
        final byte[] raw = new byte[length];
        Arrays.fill(raw, (byte) value);
        return Bytes.copy(raw);
    }
}
