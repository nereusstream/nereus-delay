package com.nereusstream.delay.runtime;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TargetResultLedgerAuditTest {
    private final Properties vectors = properties("target-result");
    private final Properties descriptors = properties("target-quota-incarnation");
    private final TargetQuotaIncarnation target = TargetQuotaIncarnation.decode(raw(descriptors, "target.open"));
    private final TargetQuotaIncarnation shard = TargetQuotaIncarnation.decode(raw(descriptors, "shard.open"));
    private final TargetResultLedgerAudit.Limits limits = new TargetResultLedgerAudit.Limits(16, 4, 1 << 20);
    private final TargetResultLedgerAudit.CompletenessAuthority allow = summary -> {};

    @Test
    void independentRecordBytesRebuildPrimaryAndMirrorsWithoutReadingCounters() {
        final var reads = new AtomicInteger();
        final var summary = audit(
                rows(),
                limits,
                key -> {
                    reads.incrementAndGet();
                    return lookup(key);
                },
                allow);
        assertEquals(4, summary.resultRecords());
        assertEquals(2, summary.ownerRecords());
        assertEquals(2, reads.get());
        assertEquals(Long.parseLong(vectors.getProperty("audit.encodedBytes")), summary.encodedBytes());
        assertEquals(CapacityVector.decode(raw(vectors, "audit.primary")), summary.primaryTotal());
        assertEquals(
                CapacityVector.decode(raw(vectors, "audit.target")),
                summary.contributions().get(target.identity()));
        assertEquals(
                summary.contributions().get(target.identity()),
                summary.contributions().get(target.tenantIdentity()));
        assertEquals(
                CapacityVector.decode(raw(vectors, "audit.shard")),
                summary.contributions().get(shard.identity()));
        assertEquals(
                summary.contributions().get(shard.identity()),
                summary.contributions().get(shard.tenantIdentity()));
    }

    @Test
    void shuffledNamespaceTraversalStillResolvesExactFirstReferences() {
        final var before = audit(rows(), limits, this::lookup, allow);
        final var shuffled = new ArrayList<>(rows());
        Collections.reverse(shuffled);
        final var after = audit(shuffled, limits, this::lookup, allow);
        assertEquals(before.contributions(), after.contributions());
        assertEquals(before.primaryTotal(), after.primaryTotal());
        assertEquals(before.encodedBytes(), after.encodedBytes());
    }

    @Test
    void missingOrChangedFirstRecordCannotPassRecovery() {
        assertThrows(IllegalStateException.class, () -> audit(rows().subList(1, 4), limits, this::lookup, allow));
        final var first = TargetResultRecord.decode(raw(vectors, "command"));
        final var outcome = CommandDedupeRecord.decode(first.typedPayload());
        final var changed = TargetResultRecord.command(
                target,
                new com.nereusstream.delay.protocol.CommandId(first.logicalId()),
                outcome.protocolTuple(),
                bytes(32, 0x98),
                outcome.result(),
                first.mutation(),
                (r, f) -> {});
        final var replaced = new ArrayList<>(rows());
        replaced.set(0, stored(changed));
        assertThrows(IllegalStateException.class, () -> audit(replaced, limits, this::lookup, allow));
    }

    @Test
    void duplicatePhysicalKeysAndConflictingSourceEventsFailClosed() {
        final var duplicate = new ArrayList<>(rows());
        duplicate.add(rows().getFirst());
        assertThrows(IllegalStateException.class, () -> audit(duplicate, limits, this::lookup, allow));
        final var conflicting = new ArrayList<>(rows());
        conflicting.add(row("system"));
        assertThrows(IllegalStateException.class, () -> audit(conflicting, limits, this::lookup, allow));
        final var doublePosition = new ArrayList<>(rows());
        doublePosition.add(row("systemPosition"));
        assertThrows(IllegalStateException.class, () -> audit(doublePosition, limits, this::lookup, allow));
    }

    @Test
    void missingWrongKeyWrongTypeAndForeignDescriptorRejectBeforeTotalsPublish() {
        assertThrows(IllegalStateException.class, () -> audit(rows(), limits, key -> null, allow));
        assertThrows(
                IllegalStateException.class,
                () -> audit(
                        rows(),
                        limits,
                        key -> new TargetResultLedgerAudit.Stored(
                                key, 34, lookup(key).payload()),
                        allow));
        assertThrows(
                IllegalStateException.class,
                () -> audit(
                        rows(),
                        limits,
                        key -> new TargetResultLedgerAudit.Stored(
                                new byte[] {1}, 33, lookup(key).payload()),
                        allow));
        assertThrows(
                IllegalStateException.class,
                () -> audit(
                        rows(),
                        limits,
                        key -> new TargetResultLedgerAudit.Stored(key, 33, shard.canonicalBytes()),
                        allow));
    }

    @Test
    void descriptorDrainMustAgreeWithLedgerSourceHistoryAndFrontier() {
        final var later = target.drain(stamp(3, 0x66), (p, n) -> {});
        audit(rows(), limits, key -> Arrays.equals(key, target.key()) ? stored(later) : lookup(key), allow);
        final var conflicting = target.drain(stamp(3, 0x67), (p, n) -> {});
        assertThrows(
                IllegalStateException.class,
                () -> audit(
                        rows(),
                        limits,
                        key -> Arrays.equals(key, target.key()) ? stored(conflicting) : lookup(key),
                        allow));
        final var ahead = target.drain(stamp(5, 0x66), (p, n) -> {});
        assertThrows(
                IllegalStateException.class,
                () -> audit(
                        rows(), limits, key -> Arrays.equals(key, target.key()) ? stored(ahead) : lookup(key), allow));
    }

    @Test
    void sourceSequenceOrderAndCompleteMetadataCannotDisagree() {
        final var first = TargetResultRecord.decode(raw(vectors, "command"));
        final var contradictory = new TargetQuotaMutation(3, source(2), bytes(32, 0x66));
        final var record = new TargetResultRecord(
                first.kind(),
                first.logicalId(),
                first.primaryIdentity(),
                first.tenantScope(),
                first.accounting(),
                first.recoveryLineage(),
                contradictory,
                first.typedPayload(),
                null,
                null);
        // A single internally consistent record can still contradict the descriptor's source order.
        final var ownerAtSamePosition = target.drain(stamp(2, 0x66), (p, n) -> {});
        assertThrows(
                IllegalStateException.class,
                () -> audit(List.of(stored(record)), limits, key -> stored(ownerAtSamePosition), allow));
        assertThrows(
                IllegalStateException.class,
                () -> TargetResultLedgerAudit.audit(
                        shard.scope(), shard.recoveryLineage(), 3, source(2), limits, rows(), this::lookup, allow));
    }

    @Test
    void finiteRecordOwnerAndByteLimitsStopBeforeCompletenessApproval() {
        final var calls = new AtomicInteger();
        final TargetResultLedgerAudit.CompletenessAuthority approved = s -> calls.incrementAndGet();
        assertThrows(
                IllegalStateException.class,
                () -> audit(rows(), new TargetResultLedgerAudit.Limits(3, 4, 1 << 20), this::lookup, approved));
        assertThrows(
                IllegalStateException.class,
                () -> audit(rows(), new TargetResultLedgerAudit.Limits(16, 1, 1 << 20), this::lookup, approved));
        final long exact = Long.parseLong(vectors.getProperty("audit.encodedBytes"));
        assertThrows(
                IllegalStateException.class,
                () -> audit(rows(), new TargetResultLedgerAudit.Limits(16, 4, exact - 1), this::lookup, approved));
        assertEquals(0, calls.get());
        audit(rows(), new TargetResultLedgerAudit.Limits(4, 2, exact), this::lookup, approved);
        assertEquals(1, calls.get());
        assertThrows(IllegalArgumentException.class, () -> new TargetResultLedgerAudit.Limits(0, 1, 1));
    }

    @Test
    void traversalLookupAndCompletenessFailuresPropagate() {
        assertThrows(NullPointerException.class, () -> audit(rows(), limits, this::lookup, null));
        assertThrows(
                IllegalStateException.class,
                () -> audit(rows(), limits, this::lookup, s -> {
                    throw new IllegalStateException("incomplete Store range");
                }));
        assertThrows(
                AssertionError.class,
                () -> audit(
                        rows(),
                        limits,
                        key -> {
                            throw new AssertionError("read failure");
                        },
                        allow));
        final Iterable<TargetResultLedgerAudit.Stored> broken = () -> new Iterator<>() {
            @Override
            public boolean hasNext() {
                throw new IllegalStateException("scan failed");
            }

            @Override
            public TargetResultLedgerAudit.Stored next() {
                throw new AssertionError("unreachable");
            }
        };
        assertThrows(IllegalStateException.class, () -> audit(broken, limits, this::lookup, allow));
        assertThrows(
                AssertionError.class,
                () -> audit(rows(), limits, this::lookup, s -> {
                    throw new AssertionError("lost Store");
                }));
    }

    @Test
    void snapshotsAndReturnedContributionsAreImmutable() {
        final var rows = rows();
        final var first = rows.getFirst();
        final byte[] before = first.payload();
        first.key()[0] ^= 1;
        first.payload()[0] ^= 1;
        assertArrayEquals(before, first.payload());
        final byte[] lineage = shard.recoveryLineage();
        final var summary = TargetResultLedgerAudit.audit(
                shard.scope(),
                lineage,
                4,
                source(4),
                limits,
                rows,
                key -> {
                    lineage[0] ^= 1;
                    return lookup(key);
                },
                allow);
        summary.lineage()[0] ^= 1;
        assertArrayEquals(shard.recoveryLineage(), summary.lineage());
        assertThrows(UnsupportedOperationException.class, () -> summary.contributions()
                .clear());
    }

    @Test
    void wrongScopeLineageAndEnvelopeCannotBecomeAnEmptyOrPartialPass() {
        final var wrongScope = new TargetQuotaScope(shard.identity().shard(), bytes(32, 0x45), null);
        assertThrows(
                IllegalStateException.class,
                () -> TargetResultLedgerAudit.audit(
                        wrongScope, shard.recoveryLineage(), 4, source(4), limits, rows(), this::lookup, allow));
        assertThrows(
                IllegalStateException.class,
                () -> TargetResultLedgerAudit.audit(
                        shard.scope(), bytes(16, 0xcd), 4, source(4), limits, rows(), this::lookup, allow));
        final var invalid = new ArrayList<>(rows());
        final var first = invalid.getFirst();
        invalid.set(0, new TargetResultLedgerAudit.Stored(first.key(), 4, first.payload()));
        assertThrows(IllegalStateException.class, () -> audit(invalid, limits, this::lookup, allow));
        assertThrows(
                IllegalStateException.class,
                () -> audit(List.of(), limits, this::lookup, s -> {
                    throw new IllegalStateException("actual namespaces were not empty");
                }));
    }

    private TargetResultLedgerAudit.Summary audit(
            Iterable<TargetResultLedgerAudit.Stored> rows,
            TargetResultLedgerAudit.Limits l,
            TargetResultLedgerAudit.DescriptorLookup lookup,
            TargetResultLedgerAudit.CompletenessAuthority authority) {
        return TargetResultLedgerAudit.audit(
                shard.scope(), shard.recoveryLineage(), 4, source(4), l, rows, lookup, authority);
    }

    private List<TargetResultLedgerAudit.Stored> rows() {
        return List.of(row("command"), row("result"), row("position"), row("duplicate"));
    }

    private TargetResultLedgerAudit.Stored row(String name) {
        return new TargetResultLedgerAudit.Stored(raw(vectors, name + ".key"), 35, raw(vectors, name));
    }

    private TargetResultLedgerAudit.Stored stored(TargetResultRecord r) {
        return new TargetResultLedgerAudit.Stored(r.key(), 35, r.canonicalBytes());
    }

    private TargetResultLedgerAudit.Stored stored(TargetQuotaIncarnation r) {
        return new TargetResultLedgerAudit.Stored(r.key(), 33, r.canonicalBytes());
    }

    private TargetResultLedgerAudit.Stored lookup(byte[] key) {
        if (Arrays.equals(key, target.key())) {
            return stored(target);
        }
        if (Arrays.equals(key, shard.key())) {
            return stored(shard);
        }
        throw new IllegalStateException("unexpected META lookup");
    }

    private TargetQuotaMutation stamp(long n, int digest) {
        return new TargetQuotaMutation(n, source(n), bytes(32, digest));
    }

    private KafkaSourcePosition source(long n) {
        final var s = (KafkaSourcePosition) target.allocation().source();
        return new KafkaSourcePosition(s.shardId(), s.authenticatedClusterId(), s.nativeTopicUuid(), 9 + n, null, 100);
    }

    private static byte[] bytes(int n, int v) {
        final var b = new byte[n];
        Arrays.fill(b, (byte) v);
        return b;
    }

    private static byte[] raw(Properties p, String key) {
        return HexFormat.of().parseHex(p.getProperty(key));
    }

    private static Properties properties(String name) {
        final var p = new Properties();
        try (var in = TargetResultLedgerAuditTest.class.getResourceAsStream("/ndip3/" + name + "-vectors.properties")) {
            p.load(in);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
        return p;
    }
}
