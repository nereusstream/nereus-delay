package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.DelayMessageId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.OrderingMode;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetMessageLocator;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.KeyCodec;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.ShardStoreConfig;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
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
            applyRejectedTargetAdmission(backend, store, shard, target);
        }
    }

    private static void applyRejectedTargetAdmission(
            TargetStoreBackend backend, ShardStore store, TargetQuotaIncarnation shard, TargetQuotaIncarnation target)
            throws java.security.GeneralSecurityException {
        final var previous = (KafkaSourcePosition) store.appliedShardLogPosition();
        final var source = new KafkaSourcePosition(
                previous.shardId(),
                previous.authenticatedClusterId(),
                previous.nativeTopicUuid(),
                previous.offset() + 1,
                previous.leaderEpoch(),
                previous.brokerLogAppendTimeEpochMs() + 1);
        final var owner = new OwnerIdentity(
                Bytes.utf8("target-deployment"), Bytes.utf8("target-worker"), 7, bytes(32, 0x42));
        final byte[] claimId = bytes(32, 0x43);
        final var messageId = DelayMessageId.random(shard.identity().shard());
        final var locator = new TargetMessageLocator(
                messageId,
                0,
                target.identity().target(),
                new TargetKeyCodec.Domain(0, 1),
                target.identity().accountingIncarnation(),
                OrderingMode.BEST_EFFORT,
                null,
                bytes(32, 0x44));
        final byte[] attemptId = SystemMutation.computePublishAttemptLogicalIdentity(claimId, messageId, 0, 1);
        final var obligation = new AttemptObligationRef(
                attemptId,
                0,
                AttemptLedgerState.PUBLISHING,
                KeyCodec.inflight((byte) 2, owner.ownerEpoch(), attemptId));
        final long[] reserve = new long[CapacityDimension.COUNT];
        reserve[CapacityDimension.RESULT_BYTES.wireValue() - 1] = 128;
        final var proof = new TrustedUtcIntervalEvidence(
                source.brokerPersistenceTimeEpochMs(),
                source.brokerPersistenceTimeEpochMs() + 1,
                TrustedUtcIntervalEvidence.Source.CERTIFIED_HOST_CLOCK,
                Bytes.utf8("store-gate-test-clock"),
                1,
                1,
                1,
                Bytes.sha256(Bytes.utf8("store-gate-test-proof")),
                0,
                null);
        final long retryUntil = source.brokerPersistenceTimeEpochMs() + 10_000;
        final var body = new TargetPublishAdmissionBody(
                shard.identity().shard(),
                retryUntil,
                owner,
                bytes(16, 0x45),
                claimId,
                locator,
                1,
                attemptId,
                obligation,
                8,
                new CapacityVector(reserve),
                CapacityVector.empty(),
                proof);
        final var keys = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        final var mutation = SystemMutation.signed(
                shard.identity().shard(),
                SystemMutationType.TARGET_PUBLISH_ADMISSION,
                retryUntil,
                body.publishAttemptId(),
                body.canonicalBytes(),
                AuthorIdentity.owner(
                                owner.deploymentId(),
                                owner.workerRunId(),
                                owner.ownerEpoch(),
                                owner.leaseFencingDigest())
                        .canonicalBytes(),
                1,
                keys.getPrivate());
        final var admissionStore = new TargetPublishAdmissionStore(
                backend, shard.scope(), shard.recoveryLineage(), 16, 1, 1);
        final long beforeSequence = store.shardMutationSequence();
        final long beforeVersion = store.latestSequenceNumber();
        final var result = admissionStore.commit(
                admissionStore.prepareFirst(
                        budget(),
                        mutation,
                        source,
                        (actualScope, writer, entry, position) -> new TargetPublishAdmissionVerifier.Authorization(
                                keys.getPublic(),
                                ProtocolTuple.currentSystemMutation(),
                                10,
                                10,
                                100,
                                (a, b, c, evidence) -> true)),
                (a, b, c) -> guard());
        assertEquals(ApplyStatus.REJECTED, result.applyStatus());
        assertEquals(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, result.stableCode());
        assertEquals(beforeSequence + 1, store.shardMutationSequence());
        assertTrue(store.latestSequenceNumber() > beforeVersion);
        assertEquals(source, store.appliedShardLogPosition());
        final byte[] systemKey = Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                mutation.systemMutationId());
        final var first = TargetResultRecord.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.DEDUPE, systemKey), TargetResultRecord.VALUE_TYPE)
                .payload());
        assertArrayEquals(result.encode(), SystemMutationResult.decode(first.typedPayload()).encode());
        final byte[] positionKey = Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                source.canonicalBytes());
        final var positionRecord = TargetResultRecord.decode(TargetValueEnvelope.decode(
                        store.get(ColumnFamily.DEDUPE, positionKey), TargetResultRecord.VALUE_TYPE)
                .payload());
        positionRecord.requireFirst(first);
        assertNull(store.get(ColumnFamily.ID, TargetKeyCodec.message(messageId)));
        assertNull(store.get(ColumnFamily.INFLIGHT, TargetClaimRecord.key(claimId)));
        assertNull(store.get(
                ColumnFamily.META,
                Bytes.concat(
                        new byte[] {
                            (byte) TargetKeyCodec.QUOTA_ATTEMPT_BUDGET_TAG,
                            TargetKeyCodec.KEY_FORMAT
                        },
                        attemptId)));
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
