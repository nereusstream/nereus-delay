package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.store.ValueEnvelope;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TargetResultRecordTest {
    private final Properties vectors = properties("target-result");
    private final Properties incarnations = properties("target-quota-incarnation");
    private final TargetQuotaIncarnation target = TargetQuotaIncarnation.decode(raw(incarnations, "target.open"));
    private final TargetQuotaIncarnation shard = TargetQuotaIncarnation.decode(raw(incarnations, "shard.open"));
    private final CommandId commandId = new CommandId(raw(vectors, "command.id"));
    private final CommandResult outcome = CommandResult.decode(raw(vectors, "result.payload"));
    private final SystemMutationResult systemOutcome = SystemMutationResult.decode(raw(vectors, "system.payload"));
    private final TargetResultRecord.CreationAuthority allow = (record, first) -> {};

    @Test
    void everyRecordKindAndAllocationAttachmentMatchIndependentBytes() {
        final var command = command();
        final var result = TargetResultRecord.result(command, allow);
        final var system = system();
        final var records = List.of(
                command,
                result,
                system,
                TargetResultRecord.position(shard, command, stamp(2), allow),
                TargetResultRecord.position(shard, command, stamp(3), allow),
                TargetResultRecord.position(shard, system, stamp(2), allow));
        final var names = List.of("command", "result", "system", "position", "duplicate", "systemPosition");
        for (int i = 0; i < records.size(); i++) {
            final var record = records.get(i);
            final var name = names.get(i);
            assertArrayEquals(raw(vectors, name), record.canonicalBytes());
            assertArrayEquals(raw(vectors, name + ".key"), record.key());
            assertEquals(CapacityVector.decode(raw(vectors, name + ".charge")), record.recordCharge());
            assertArrayEquals(
                    record.canonicalBytes(),
                    TargetResultRecord.decode(record.canonicalBytes()).canonicalBytes());
            assertTrue(record.canonicalBytes().length <= TargetResultRecord.MAX_CANONICAL_BYTES);
        }
        assertArrayEquals(raw(vectors, "allocation"), system.allocation().canonicalBytes());
    }

    @Test
    void physicalRetriesKeepFirstOutcomeAndFrozenOwnerAcrossFiftyPositions() {
        final var first = command();
        final var before = first.canonicalBytes();
        for (int i = 3; i < 53; i++) {
            final var audit = TargetResultRecord.position(shard, first, stamp(i), allow);
            audit.requireFirst(first);
            assertArrayEquals(first.digest(), audit.firstDigest());
            assertEquals(shard.identity(), audit.primaryIdentity());
            assertEquals(1, audit.recordCharge().amount(CapacityDimension.EVIDENCE_RECORDS));
            assertEquals(0, audit.recordCharge().amount(CapacityDimension.RESULT_RECORDS));
            assertNotEquals(
                    HexFormat.of().formatHex(audit.key()), HexFormat.of().formatHex(first.key()));
        }
        assertArrayEquals(before, first.canonicalBytes());
        assertArrayEquals(
                outcome.encode(),
                CommandDedupeRecord.decode(first.typedPayload()).result().encode());
    }

    @Test
    void queryProjectionRetainsExactCommandOutcomeAndFullFirstReference() {
        final var first = command();
        final var result = TargetResultRecord.result(first, allow);
        result.requireFirst(first);
        assertEquals(first.primaryIdentity(), result.primaryIdentity());
        final var changed = new CommandResult(
                ApplyStatus.REJECTED,
                StableCode.NOT_FOUND,
                -1,
                0,
                null,
                source(2).canonicalBytes());
        final var other = copy(result, changed.encode(), result.mutation(), result.firstDigest(), null);
        assertThrows(IllegalStateException.class, () -> other.requireFirst(first));
        assertThrows(IllegalStateException.class, () -> result.requireFirst(system()));
        assertThrows(IllegalArgumentException.class, () -> TargetResultRecord.result(result, allow));
    }

    @Test
    void positionCannotReferenceQueryCopyOrAnotherLogicalFirstRecord() {
        final var first = command();
        final var audit = TargetResultRecord.position(shard, first, stamp(3), allow);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetResultRecord.position(shard, TargetResultRecord.result(first, allow), stamp(3), allow));
        final var different = TargetResultRecord.command(
                target, commandId, ProtocolTuple.currentClientCommand(), bytes(32, 0x98), outcome, stamp(2), allow);
        assertThrows(IllegalStateException.class, () -> audit.requireFirst(different));
        assertThrows(IllegalStateException.class, () -> audit.requireFirst(null));
    }

    @Test
    void physicalAuditRequiresShardOwnerAndSameTenantLineage() {
        assertThrows(
                IllegalArgumentException.class, () -> TargetResultRecord.position(target, command(), stamp(3), allow));
        final var tenantOwner = TargetQuotaIncarnation.allocate(
                new TargetQuotaScope(shard.identity().shard(), bytes(32, 0x45), null),
                shard.accounting(),
                shard.recoveryLineage(),
                stamp(1),
                (p, n) -> {});
        assertThrows(
                IllegalStateException.class,
                () -> TargetResultRecord.position(tenantOwner, command(), stamp(3), allow));
        final var lineageOwner = TargetQuotaIncarnation.allocate(
                shard.scope(), shard.accounting(), bytes(16, 0xcd), stamp(1), (p, n) -> {});
        assertThrows(
                IllegalStateException.class,
                () -> TargetResultRecord.position(lineageOwner, command(), stamp(3), allow));
    }

    @Test
    void sourceAndLocalOrdinalContradictionsFailBeforeAuthority() {
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetResultRecord.command(
                        target,
                        commandId,
                        ProtocolTuple.currentClientCommand(),
                        bytes(32, 0x99),
                        outcome,
                        stamp(3),
                        allow));
        final var local = new TargetQuotaMutation(2, source(2), bytes(32, 0x66), 1);
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetResultRecord.command(
                        target,
                        commandId,
                        ProtocolTuple.currentClientCommand(),
                        bytes(32, 0x99),
                        outcome,
                        local,
                        allow));
        assertThrows(IllegalStateException.class, () -> TargetResultRecord.position(shard, command(), stamp(1), allow));
        final var conflicting = new TargetQuotaMutation(2, source(2), bytes(32, 0x67));
        assertThrows(
                IllegalStateException.class, () -> TargetResultRecord.position(shard, command(), conflicting, allow));
    }

    @Test
    void creationRequiresCurrentDescriptorButHistoricalReadsAllowLaterDrain() {
        final var first = command();
        final var draining = target.drain(stamp(3), (p, n) -> {});
        first.requireOwner(draining);
        assertThrows(
                IllegalStateException.class,
                () -> TargetResultRecord.command(
                        draining,
                        commandId,
                        ProtocolTuple.currentClientCommand(),
                        bytes(32, 0x99),
                        outcome,
                        stamp(2),
                        allow));
        assertThrows(IllegalStateException.class, () -> first.requireOwner(shard));
    }

    @Test
    void mandatoryAuthorityFailuresAndFatalErrorsNeverReturnAuthorizedRecords() {
        assertThrows(NullPointerException.class, () -> TargetResultRecord.result(command(), null));
        assertThrows(
                IllegalStateException.class,
                () -> TargetResultRecord.result(command(), (r, f) -> {
                    throw new IllegalStateException("denied");
                }));
        assertThrows(
                AssertionError.class,
                () -> TargetResultRecord.position(shard, command(), stamp(3), (r, f) -> {
                    throw new AssertionError("fatal");
                }));
        final var calls = new AtomicInteger();
        TargetResultRecord.result(command(), (r, f) -> {
            calls.incrementAndGet();
            r.requireFirst(f);
        });
        assertEquals(1, calls.get());
    }

    @Test
    void successfulAllocationResultRetainsExactOpenOriginAndRejectsOtherOperations() {
        final var allocated = allocation();
        assertEquals(stamp(2), system().allocation().allocation());
        final var rejected = new SystemMutationResult(
                systemOutcome.mutationId(),
                systemOutcome.mutationHash(),
                systemOutcome.mutationType(),
                1000,
                systemOutcome.authorIdentity(),
                ApplyStatus.REJECTED,
                StableCode.NOT_FOUND,
                source(2).canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetResultRecord.system(shard, rejected, stamp(2), allocated, allow));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetResultRecord.system(shard, systemOutcome, stamp(2), target, allow));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetResultRecord.system(
                        shard, systemOutcome, stamp(2), allocated.drain(stamp(3), (p, n) -> {}), allow));
        assertThrows(
                IllegalArgumentException.class,
                () -> copy(command(), command().typedPayload(), stamp(2), null, allocated));
        final var wrongType = new SystemMutationResult(
                systemOutcome.mutationId(),
                systemOutcome.mutationHash(),
                SystemMutationType.RESOLVE_UNCERTAIN,
                1000,
                systemOutcome.authorIdentity(),
                ApplyStatus.APPLIED,
                StableCode.OK,
                source(2).canonicalBytes());
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetResultRecord.system(shard, wrongType, stamp(2), allocated, allow));
    }

    @Test
    void deletionRequiresProtectedFloorAndActualReferenceAuthority() {
        final var record = command();
        final var original = record.canonicalBytes();
        final var calls = new AtomicInteger();
        record.requireDeletion(floor(2, shard.recoveryLineage()), stamp(3), (r, f, d) -> calls.incrementAndGet());
        assertEquals(1, calls.get());
        assertThrows(
                IllegalStateException.class,
                () -> record.requireDeletion(floor(1, shard.recoveryLineage()), stamp(3), (r, f, d) -> {}));
        assertThrows(
                IllegalStateException.class,
                () -> record.requireDeletion(floor(2, bytes(16, 0xdd)), stamp(3), (r, f, d) -> {}));
        assertThrows(
                IllegalStateException.class,
                () -> record.requireDeletion(floor(3, shard.recoveryLineage()), stamp(3), (r, f, d) -> {}));
        assertThrows(
                IllegalStateException.class,
                () -> record.requireDeletion(floor(2, shard.recoveryLineage()), stamp(3), (r, f, d) -> {
                    throw new IllegalStateException("live POSITION/query reference");
                }));
        assertThrows(
                NullPointerException.class,
                () -> record.requireDeletion(floor(2, shard.recoveryLineage()), stamp(3), null));
        assertThrows(
                AssertionError.class,
                () -> record.requireDeletion(floor(2, shard.recoveryLineage()), stamp(3), (r, f, d) -> {
                    throw new AssertionError("fatal");
                }));
        assertArrayEquals(original, record.canonicalBytes());
    }

    @Test
    void exactStoredRecordAndDefensiveCopiesProtectReadSets() {
        final var record = system();
        final var before = record.canonicalBytes();
        record.requireStored(record.key(), 35, before);
        assertThrows(IllegalStateException.class, () -> record.requireStored(record.key(), 4, before));
        assertThrows(
                IllegalStateException.class,
                () -> record.requireStored(command().key(), 35, before));
        assertThrows(IllegalStateException.class, () -> record.requireStored(record.key(), 35, null));
        for (byte[] raw : List.of(
                record.logicalId(),
                record.tenantScope(),
                record.recoveryLineage(),
                record.typedPayload(),
                record.digest(),
                record.key())) {
            raw[0] ^= 1;
        }
        assertArrayEquals(before, record.canonicalBytes());
        assertNull(record.firstDigest());
    }

    @Test
    void commandEvidenceAndResultsUseOneDistinctRecordClassEach() {
        final var command = command();
        final var result = TargetResultRecord.result(command, allow);
        final var system = system();
        assertEquals(1, command.recordCharge().amount(CapacityDimension.EVIDENCE_RECORDS));
        assertEquals(1, result.recordCharge().amount(CapacityDimension.RESULT_RECORDS));
        assertEquals(1, system.recordCharge().amount(CapacityDimension.RESULT_RECORDS));
        for (var value : List.of(command, result, system)) {
            assertEquals(0, value.recordCharge().amount(CapacityDimension.LOGICAL_STATE_BYTES));
            assertEquals(0, value.recordCharge().amount(CapacityDimension.SYSTEM_MUTATION_RECORDS));
            assertEquals(0, value.recordCharge().amount(CapacityDimension.INFLIGHT_MESSAGES));
            assertEquals(value.primaryIdentity(), value.tenantIdentity().primary());
        }
    }

    @Test
    void malformedCanonicalFieldsAndLegacyEnvelopeAreRejected() {
        final byte[] raw = command().canonicalBytes();
        final var corrupt = raw.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> TargetResultRecord.decode(corrupt));
        assertThrows(
                IllegalArgumentException.class, () -> TargetResultRecord.decode(Arrays.copyOf(raw, raw.length - 1)));
        assertThrows(
                IllegalArgumentException.class, () -> TargetResultRecord.decode(Bytes.concat(raw, new byte[] {8, 1})));
        final byte[] unknown = raw.clone();
        unknown[1] = 2;
        assertThrows(IllegalArgumentException.class, () -> TargetResultRecord.decode(unknown));
        final byte[] missingFirst = raw.clone();
        missingFirst[3] = 2;
        assertThrows(IllegalArgumentException.class, () -> TargetResultRecord.decode(missingFirst));
        final byte[] framed = Bytes.concat(new byte[] {'N', 'V', 35, 1}, Bytes.u32be(raw.length), raw);
        assertThrows(
                IllegalArgumentException.class,
                () -> ValueEnvelope.decodeAny(Bytes.concat(framed, Bytes.crc32cbe(framed))));
    }

    @Test
    void authorIdentityAndNestedPayloadBoundsFailClosed() {
        final var wrongAuthor =
                AuthorIdentity.owner(bytes(2, 1), bytes(2, 2), 1, bytes(32, 3)).canonicalBytes();
        final var wrong = new SystemMutationResult(
                systemOutcome.mutationId(),
                systemOutcome.mutationHash(),
                SystemMutationType.APPLY_SHARD_CONTROL,
                1000,
                wrongAuthor,
                ApplyStatus.APPLIED,
                StableCode.OK,
                source(2).canonicalBytes());
        assertThrows(
                IllegalArgumentException.class, () -> TargetResultRecord.system(shard, wrong, stamp(2), null, allow));
        final var huge = new SystemMutationResult(
                systemOutcome.mutationId(),
                systemOutcome.mutationHash(),
                SystemMutationType.APPLY_SHARD_CONTROL,
                1000,
                new byte[TargetResultRecord.MAX_AUTHOR_BYTES + 1],
                ApplyStatus.APPLIED,
                StableCode.OK,
                source(2).canonicalBytes());
        assertThrows(
                IllegalArgumentException.class, () -> TargetResultRecord.system(shard, huge, stamp(2), null, allow));
        assertThrows(
                IllegalArgumentException.class,
                () -> copy(command(), new byte[TargetResultRecord.MAX_PAYLOAD_BYTES + 1], stamp(2), null, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetResultRecord.decode(new byte[TargetResultRecord.MAX_CANONICAL_BYTES + 1]));
    }

    @Test
    void targetCommandEvidenceRejectsLegacyPayloadAndSystemTuple() {
        final byte[] legacy =
                Bytes.concat(Bytes.u32be(1), bytes(32, 0x99), Bytes.u32be(outcome.encode().length), outcome.encode());
        assertThrows(IllegalArgumentException.class, () -> copy(command(), legacy, stamp(2), null, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetResultRecord.command(
                        target,
                        commandId,
                        ProtocolTuple.currentSystemMutation(),
                        bytes(32, 0x99),
                        outcome,
                        stamp(2),
                        allow));
        assertThrows(
                IllegalArgumentException.class,
                () -> TargetResultRecord.command(
                        target,
                        commandId,
                        ProtocolTuple.currentClientCommand(),
                        new byte[32],
                        outcome,
                        stamp(2),
                        allow));
    }

    private TargetResultRecord command() {
        return TargetResultRecord.command(
                target, commandId, ProtocolTuple.currentClientCommand(), bytes(32, 0x99), outcome, stamp(2), allow);
    }

    private TargetResultRecord system() {
        return TargetResultRecord.system(shard, systemOutcome, stamp(2), allocation(), allow);
    }

    private TargetQuotaIncarnation allocation() {
        return TargetQuotaIncarnation.allocate(
                target.scope(), target.accounting(), target.recoveryLineage(), stamp(2), (p, n) -> {});
    }

    private TargetResultRecord copy(
            TargetResultRecord r,
            byte[] payload,
            TargetQuotaMutation stamp,
            byte[] first,
            TargetQuotaIncarnation allocation) {
        return new TargetResultRecord(
                r.kind(),
                r.logicalId(),
                r.primaryIdentity(),
                r.tenantScope(),
                r.accounting(),
                r.recoveryLineage(),
                stamp,
                payload,
                first,
                allocation);
    }

    private TargetQuotaMutation stamp(long n) {
        return new TargetQuotaMutation(n, source(n), bytes(32, 0x66));
    }

    private KafkaSourcePosition source(long n) {
        final var s = (KafkaSourcePosition) target.allocation().source();
        return new KafkaSourcePosition(s.shardId(), s.authenticatedClusterId(), s.nativeTopicUuid(), 9 + n, null, 100);
    }

    private RecoveryFloorRef floor(long n, byte[] lineage) {
        return new RecoveryFloorRef(lineage, bytes(16, 0xee), bytes(32, 0xff), n, source(n), n, List.of());
    }

    private static byte[] bytes(int size, int value) {
        final var raw = new byte[size];
        Arrays.fill(raw, (byte) value);
        return raw;
    }

    private static byte[] raw(Properties p, String key) {
        return HexFormat.of().parseHex(p.getProperty(key));
    }

    private static Properties properties(String name) {
        final var p = new Properties();
        try (var in = TargetResultRecordTest.class.getResourceAsStream("/ndip3/" + name + "-vectors.properties")) {
            p.load(in);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return p;
    }
}
