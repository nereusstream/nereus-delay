package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetQuotaAttemptBudget;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.runtime.AttemptLedgerState;
import com.nereusstream.delay.runtime.AttemptObligationRef;
import com.nereusstream.delay.runtime.CurrentSendWorkKind;
import com.nereusstream.delay.runtime.GenerationAggregateState;
import com.nereusstream.delay.runtime.TargetGenerationRuntimeIndex;
import com.nereusstream.delay.runtime.TargetMessageRecord;
import com.nereusstream.delay.runtime.TargetOrderState;
import com.nereusstream.delay.runtime.TargetRecordAccounting;
import com.nereusstream.delay.runtime.TargetTerminalGenerationRecord;
import com.nereusstream.delay.runtime.TargetTimelineWorkRef;
import com.nereusstream.delay.runtime.TimelineWorkKind;
import com.nereusstream.delay.runtime.UncertainRetryAuthority;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetCheckpointRootVerifierTest {
    @TempDir
    Path tempDir;

    @Test
    void refusesEmptyTargetWrongShardAndFormatOneImages() {
        final var shard = new ShardId(RouteIncarnation.random(), 2);
        final var another = new ShardId(RouteIncarnation.random(), 3);
        final var targetConfig = ShardStoreConfig.defaults(tempDir.resolve("target"));
        final Path targetDb;
        try (var resources = new SharedRocksDbResources(targetConfig);
                var store = ShardStore.openTarget(targetConfig, shard, resources)) {
            targetDb = store.dbPath();
        }
        final var limits = new CheckpointManifestLimits(100, 64L << 20, 64L << 20, 1024, 1 << 20, 100, 1024);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetCheckpointRootVerifier.validate(targetDb, shard, CheckpointManifestLimits.unbounded()));
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.validate(
                                targetDb,
                                shard,
                                new CheckpointManifestLimits(1, 64L << 20, 64L << 20, 1024, 1 << 20, 100, 1024)))
                .getMessage()
                .contains("file count"));
        assertTrue(assertThrows(
                        IllegalArgumentException.class,
                        () -> TargetCheckpointRootVerifier.validate(targetDb, shard, limits))
                .getMessage()
                .contains("missing source position"));
        assertThrows(
                IllegalArgumentException.class, () -> TargetCheckpointRootVerifier.validate(targetDb, another, limits));

        final var laneConfig = ShardStoreConfig.defaults(tempDir.resolve("lane"));
        final Path laneDb;
        try (var resources = new SharedRocksDbResources(laneConfig);
                var store = ShardStore.open(laneConfig, shard, resources)) {
            laneDb = store.dbPath();
        }
        assertThrows(
                IllegalArgumentException.class, () -> TargetCheckpointRootVerifier.validate(laneDb, shard, limits));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetCheckpointRootVerifier.validate(tempDir.resolve("absent"), shard, limits));
    }

    @Test
    void strictOrderStateRequiresItsStoredHeadAndBarrierMessage() throws Exception {
        final var state = TargetOrderState.decode(vector("order.head"));
        final var message = TargetMessageRecord.decode(vector("order.message.initial"));
        final var work = TargetTimelineWorkRef.decode(vector("work.fifo.initial"));
        final var barrier = TargetOrderState.decode(vector("order.claimed"));
        final var claimed = TargetMessageRecord.decode(vector("order.message.claimed"));
        final var config = ShardStoreConfig.defaults(tempDir.resolve("strict"));
        try (var resources = new SharedRocksDbResources(config);
                var store = ShardStore.openTarget(
                        config, message.locator().messageId().routingId().shardId(), resources)) {
            final TargetRecordAccounting.View view = new TargetRecordAccounting.View() {
                @Override
                public ShardId shardId() {
                    return store.shardId();
                }

                @Override
                public byte[] projected(
                        final ColumnFamily family,
                        final byte[] key,
                        final List<TargetStoreBackend.Edit> overlay) {
                    if (!overlay.isEmpty()) {
                        throw new IllegalArgumentException("test audit cannot use an overlay");
                    }
                    return store.get(family, key);
                }
            };
            store.write(batch -> {
                batch.put(ColumnFamily.META, state.encodedKey(),
                        TargetValueEnvelope.encode(TargetOrderState.VALUE_TYPE, state.canonicalBytes()));
                batch.put(ColumnFamily.ID, message.encodedKey(),
                        TargetValueEnvelope.encode(TargetMessageRecord.VALUE_TYPE, message.canonicalBytes()));
                batch.put(ColumnFamily.TIMELINE, state.serviceableHead().key(),
                        TargetValueEnvelope.encode(TargetTimelineWorkRef.VALUE_TYPE, work.canonicalBytes()));
            });
            TargetCheckpointLedgerAudit.auditOrderStateDependencies(state, view);

            store.write(batch -> batch.delete(ColumnFamily.TIMELINE, state.serviceableHead().key()));
            assertTrue(assertThrows(IllegalStateException.class,
                    () -> TargetCheckpointLedgerAudit.auditOrderStateDependencies(state, view))
                    .getMessage().contains("lacks its serviceable head"));
            store.write(batch -> batch.put(ColumnFamily.TIMELINE, state.serviceableHead().key(),
                    TargetValueEnvelope.encode(TargetTimelineWorkRef.VALUE_TYPE, work.canonicalBytes())));
            TargetCheckpointLedgerAudit.auditOrderStateDependencies(state, view);
            store.write(batch -> batch.put(ColumnFamily.TIMELINE, state.serviceableHead().key(),
                    TargetValueEnvelope.encode(TargetTimelineWorkRef.VALUE_TYPE,
                            work.withRuntimeRevision(work.runtimeRevision() + 1).canonicalBytes())));
            assertThrows(IllegalArgumentException.class,
                    () -> TargetCheckpointLedgerAudit.auditOrderStateDependencies(state, view));
            store.write(batch -> batch.put(ColumnFamily.TIMELINE, state.serviceableHead().key(),
                    TargetValueEnvelope.encode(TargetTimelineWorkRef.VALUE_TYPE, work.canonicalBytes())));

            store.write(batch -> {
                batch.put(ColumnFamily.META, barrier.encodedKey(),
                        TargetValueEnvelope.encode(TargetOrderState.VALUE_TYPE, barrier.canonicalBytes()));
                batch.put(ColumnFamily.ID, claimed.encodedKey(),
                        TargetValueEnvelope.encode(TargetMessageRecord.VALUE_TYPE, claimed.canonicalBytes()));
            });
            TargetCheckpointLedgerAudit.auditOrderStateDependencies(barrier, view);
            store.write(batch -> batch.put(ColumnFamily.ID, claimed.encodedKey(),
                    TargetValueEnvelope.encode(TargetMessageRecord.VALUE_TYPE, message.canonicalBytes())));
            assertThrows(IllegalArgumentException.class,
                    () -> TargetCheckpointLedgerAudit.auditOrderStateDependencies(barrier, view));
            store.write(batch -> batch.put(ColumnFamily.ID, claimed.encodedKey(),
                    TargetValueEnvelope.encode(TargetMessageRecord.VALUE_TYPE, claimed.canonicalBytes())));
            store.write(batch -> batch.delete(ColumnFamily.ID, claimed.encodedKey()));
            assertTrue(assertThrows(IllegalStateException.class,
                    () -> TargetCheckpointLedgerAudit.auditOrderStateDependencies(barrier, view))
                    .getMessage().contains("lacks its referenced Message"));
            final var terminalMessage = TargetMessageRecord.decode(vector("order.message.terminal"));
            final var terminalState = TargetOrderState.decode(vector("order.terminal"));
            final var old = terminalMessage.locator();
            final var nextLocator = new TargetMessageLocator(old.messageId(), old.generation() + 1,
                    old.target(), old.domain(), old.accountingIncarnation(), old.orderingMode(),
                    old.orderingDomain(), old.scheduleBindingDigest());
            final var nextWork = new TargetTimelineWorkRef(nextLocator, TimelineWorkKind.INITIAL_SCHEDULE,
                    message.deliverAtEpochMs(), message.deliverAtEpochMs(), message.scheduleSource().sourceOrderToken(),
                    1, 1, UncertainRetryAuthority.NONE, null, null, false);
            final var nextMessage = new TargetMessageRecord(nextLocator, terminalMessage.stateVersion() + 1,
                    message.deliverAtEpochMs(), message.expireAtEpochMs(), message.retryEligibilityAtEpochMs(),
                    message.nativeDeliveryPolicy(), message.scheduleSource(), message.inlinePayload(),
                    message.payloadReference(), new TargetGenerationRuntimeIndex(nextLocator.generation(),
                            GenerationAggregateState.SCHEDULED, CurrentSendWorkKind.TIMELINE, nextWork,
                            null, null, List.of(), 0, 0, false, 1));
            final var terminal = new TargetTerminalGenerationRecord(old, terminalMessage.stateVersion(),
                    StableCode.ALREADY_EXPIRED, terminalMessage.runtime(),
                    new TargetQuotaMutation(1, message.scheduleSource(), terminalMessage.runtime().runtimeDigest()),
                    HexFormat.of().parseHex("01".repeat(16)));
            store.write(batch -> {
                batch.put(ColumnFamily.ID, nextMessage.encodedKey(),
                        TargetValueEnvelope.encode(TargetMessageRecord.VALUE_TYPE, nextMessage.canonicalBytes()));
                batch.put(ColumnFamily.TERMINAL, terminal.key(),
                        TargetValueEnvelope.encode(
                                TargetTerminalGenerationRecord.VALUE_TYPE, terminal.canonicalBytes()));
            });
            TargetCheckpointLedgerAudit.auditOrderStateDependencies(terminalState, view);
            store.write(batch -> batch.delete(ColumnFamily.TERMINAL, terminal.key()));
            assertTrue(assertThrows(IllegalStateException.class,
                    () -> TargetCheckpointLedgerAudit.auditOrderStateDependencies(terminalState, view))
                    .getMessage().contains("historical barrier lacks its terminal summary"));
            final var oldRuntime = terminal.runtime();
            final var wrongRuntime = new TargetGenerationRuntimeIndex(oldRuntime.generation(),
                    oldRuntime.aggregateState(), oldRuntime.currentWorkKind(), null, null, null,
                    oldRuntime.attemptObligations(), oldRuntime.admissionsUsed(),
                    oldRuntime.uncertainRetryAdmissionsUsed(),
                    oldRuntime.possibleDestinationDuplicate(), oldRuntime.runtimeRevision() + 1);
            final var wrong = new TargetTerminalGenerationRecord(old, terminal.stateVersion(), terminal.terminalCode(),
                    wrongRuntime, terminal.mutation(), terminal.recoveryLineage());
            store.write(batch -> batch.put(ColumnFamily.TERMINAL, wrong.key(),
                    TargetValueEnvelope.encode(TargetTerminalGenerationRecord.VALUE_TYPE, wrong.canonicalBytes())));
            assertThrows(IllegalArgumentException.class,
                    () -> TargetCheckpointLedgerAudit.auditOrderStateDependencies(terminalState, view));
        }
    }

    @Test
    void unresolvedAttemptBudgetsMustMatchOpenMessageObligations() throws Exception {
        final var admitted = TargetQuotaAttemptBudget.decode(quotaVector("admitted.budget"));
        final var unknown = TargetQuotaAttemptBudget.decode(quotaVector("unknown.budget"));
        final var resolved = TargetQuotaAttemptBudget.decode(quotaVector("resolved.budget"));
        final var publishing = attemptProjection(admitted, AttemptLedgerState.PUBLISHING);
        final var changedPublishing = attemptProjection(admitted, AttemptLedgerState.PUBLISHING, 3);
        final var uncertain = attemptProjection(unknown, AttemptLedgerState.UNCERTAIN);

        assertDoesNotThrow(() -> TargetCheckpointLedgerAudit.auditAttemptBudgetReferences(
                List.of(admitted), List.of(publishing, publishing)));
        assertDoesNotThrow(() -> TargetCheckpointLedgerAudit.auditAttemptBudgetReferences(
                List.of(unknown), List.of(uncertain)));
        assertDoesNotThrow(() -> TargetCheckpointLedgerAudit.auditAttemptBudgetReferences(
                List.of(resolved), List.of()));

        assertTrue(assertThrows(
                        IllegalStateException.class,
                        () -> TargetCheckpointLedgerAudit.auditAttemptBudgetReferences(List.of(), List.of(publishing)))
                .getMessage()
                .contains("lacks its quota budget"));
        assertTrue(assertThrows(
                        IllegalStateException.class,
                        () -> TargetCheckpointLedgerAudit.auditAttemptBudgetReferences(List.of(admitted), List.of()))
                .getMessage()
                .contains("lacks its attempt reference"));
        assertTrue(assertThrows(
                        IllegalStateException.class,
                        () -> TargetCheckpointLedgerAudit.auditAttemptBudgetReferences(
                                List.of(admitted), List.of(uncertain)))
                .getMessage()
                .contains("contradicts its quota budget"));
        assertTrue(assertThrows(
                        IllegalStateException.class,
                        () -> TargetCheckpointLedgerAudit.auditAttemptBudgetReferences(
                                List.of(resolved), List.of(publishing)))
                .getMessage()
                .contains("contradicts its quota budget"));
        assertTrue(assertThrows(
                        IllegalStateException.class,
                        () -> TargetCheckpointLedgerAudit.auditAttemptBudgetReferences(
                                List.of(admitted), List.of(publishing, changedPublishing)))
                .getMessage()
                .contains("Message and Terminal runtime disagree"));
    }

    private static TargetCheckpointLedgerAudit.AttemptProjection attemptProjection(
            final TargetQuotaAttemptBudget budget, final AttemptLedgerState state) {
        return attemptProjection(budget, state, 2);
    }

    private static TargetCheckpointLedgerAudit.AttemptProjection attemptProjection(
            final TargetQuotaAttemptBudget budget, final AttemptLedgerState state, final long runtimeRevision) {
        final byte[] attemptId = budget.publishAttemptId();
        final byte[] key = KeyCodec.inflight((byte) (state == AttemptLedgerState.PUBLISHING ? 2 : 3), 1, attemptId);
        final var reference = new AttemptObligationRef(attemptId, budget.locator().generation(), state, key);
        final boolean publishing = state == AttemptLedgerState.PUBLISHING;
        final var runtime = new TargetGenerationRuntimeIndex(
                budget.locator().generation(),
                publishing ? GenerationAggregateState.PUBLISHING : GenerationAggregateState.UNCERTAIN,
                publishing ? CurrentSendWorkKind.PUBLISHING : CurrentSendWorkKind.NONE,
                null,
                null,
                publishing ? attemptId : null,
                List.of(reference),
                1,
                0,
                false,
                runtimeRevision);
        return new TargetCheckpointLedgerAudit.AttemptProjection(budget.locator(), runtime);
    }

    private static byte[] vector(final String key) throws Exception {
        final var properties = new Properties();
        final var resource = TargetCheckpointRootVerifierTest.class
                .getResourceAsStream("/ndip3/target-identity-vectors.properties");
        try (var stream = Objects.requireNonNull(resource)) {
            properties.load(stream);
        }
        return HexFormat.of().parseHex(Objects.requireNonNull(properties.getProperty(key)));
    }

    private static byte[] quotaVector(final String key) throws Exception {
        final var properties = new Properties();
        final var resource = TargetCheckpointRootVerifierTest.class
                .getResourceAsStream("/ndip3/target-quota-accounting-vectors.properties");
        try (var stream = Objects.requireNonNull(resource)) {
            properties.load(stream);
        }
        return HexFormat.of().parseHex(Objects.requireNonNull(properties.getProperty(key)));
    }
}
