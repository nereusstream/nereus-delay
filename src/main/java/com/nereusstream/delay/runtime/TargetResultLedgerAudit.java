package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Independent, finite recovery fold of the complete Target result namespaces; never a hot mutation scan. */
public final class TargetResultLedgerAudit {
    private TargetResultLedgerAudit() {}

    public record Limits(int maximumResults, int maximumOwners, long maximumEncodedBytes) {
        public Limits {
            if (maximumResults <= 0 || maximumOwners <= 0 || maximumEncodedBytes <= 0) {
                throw new IllegalArgumentException("result audit requires positive finite limits");
            }
        }
    }

    /** One decoded NV envelope from the indicated CF; payload excludes its twelve framing bytes. */
    public record Stored(byte[] key, int type, byte[] payload) {
        public Stored {
            key = Bytes.copy(Objects.requireNonNull(key, "key"));
            payload = Bytes.copy(Objects.requireNonNull(payload, "payload"));
        }

        @Override
        public byte[] key() {
            return Bytes.copy(key);
        }

        @Override
        public byte[] payload() {
            return Bytes.copy(payload);
        }
    }

    @FunctionalInterface
    public interface DescriptorLookup {
        /** Exact META point read in the same snapshot as the DEDUPE traversal. */
        Stored get(byte[] key);
    }

    @FunctionalInterface
    public interface CompletenessAuthority {
        /**
         * Proves all DEDUPE 06–09 rows and actual META dependencies were traversed in one complete Store view,
         * bound to the supplied Route tenant, lineage, source frontier, Owner and Store incarnation. A caller
         * collection, matching subtotal or callback invocation alone does not prove that the snapshot is complete.
         */
        void requireComplete(Summary summary);
    }

    public record Summary(
            TargetQuotaScope scope,
            byte[] lineage,
            long sourceSequence,
            SourcePosition frontier,
            int resultRecords,
            int ownerRecords,
            long encodedBytes,
            Map<TargetQuotaIdentity, CapacityVector> contributions,
            CapacityVector primaryTotal) {
        public Summary {
            lineage = Bytes.copy(lineage);
            contributions = Map.copyOf(contributions);
        }

        @Override
        public byte[] lineage() {
            return Bytes.copy(lineage);
        }
    }

    public static Summary audit(
            final TargetQuotaScope scope,
            final byte[] lineage,
            final long sourceSequence,
            final SourcePosition frontier,
            final Limits limits,
            final Iterable<Stored> resultRows,
            final DescriptorLookup lookup,
            final CompletenessAuthority authority) {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(resultRows, "resultRows");
        Objects.requireNonNull(lookup, "descriptorLookup");
        Objects.requireNonNull(authority, "completenessAuthority");
        Bytes.requireLength(lineage, 16, "lineage");
        final byte[] expectedLineage = Bytes.copy(lineage);
        if (scope.target() != null
                || sourceSequence == 0
                || Arrays.equals(expectedLineage, new byte[16])
                || !scope.shard()
                        .equals(TargetSourcePosition.requireBounded(frontier).shardId())) {
            throw new IllegalArgumentException("invalid result recovery scope/lineage/frontier");
        }
        final var records = new LinkedHashMap<String, TargetResultRecord>();
        final var owners = new LinkedHashMap<TargetQuotaIdentity, TargetQuotaIncarnation>();
        final var stamps = new TreeMap<Long, TargetQuotaMutation>(Long::compareUnsigned);
        final var events = new LinkedHashMap<Long, String>();
        final var contributions = new LinkedHashMap<TargetQuotaIdentity, CapacityVector>();
        CapacityVector total = CapacityVector.empty();
        long bytes = 0;
        for (Stored stored : resultRows) {
            if (records.size() == limits.maximumResults()) {
                throw new IllegalStateException("result recovery record budget exhausted");
            }
            Objects.requireNonNull(stored, "resultRow");
            bytes = consume(bytes, stored, limits);
            final var record = TargetResultRecord.decode(stored.payload);
            record.requireStored(stored.key, stored.type, stored.payload);
            if (!scope.shard().equals(record.primaryIdentity().shard())
                    || !Arrays.equals(scope.tenantScope(), record.tenantScope())
                    || !Arrays.equals(expectedLineage, record.recoveryLineage())) {
                throw new IllegalStateException("result recovery crossed tenant/Shard/lineage");
            }
            record.mutation().requireAtOrBefore(sourceSequence, frontier);
            final var priorStamp = stamps.putIfAbsent(record.mutation().sequence(), record.mutation());
            if (priorStamp != null && !priorStamp.equals(record.mutation())) {
                throw new IllegalStateException("one source sequence has conflicting result stamps");
            }
            final String key = HexFormat.of().formatHex(stored.key);
            if (records.putIfAbsent(key, record) != null) {
                throw new IllegalStateException("duplicate physical result key in recovery snapshot");
            }
            final String event = (record.kind() == TargetResultRecord.Kind.SYSTEM
                                    || record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM
                            ? "S:"
                            : "C:")
                    + HexFormat.of().formatHex(record.logicalId());
            final var priorEvent = events.putIfAbsent(record.mutation().sequence(), event);
            if (priorEvent != null && !priorEvent.equals(event)) {
                throw new IllegalStateException("source position belongs to more than one logical event");
            }
            var descriptor = owners.get(record.primaryIdentity());
            if (descriptor == null) {
                if (owners.size() == limits.maximumOwners()) {
                    throw new IllegalStateException("result recovery owner budget exhausted");
                }
                final byte[] descriptorKey = descriptorKey(record.primaryIdentity());
                final var storedOwner = lookup.get(Bytes.copy(descriptorKey));
                if (storedOwner == null) {
                    throw new IllegalStateException("missing result owner");
                }
                bytes = consume(bytes, storedOwner, limits);
                if (storedOwner.type != TargetQuotaIncarnation.VALUE_TYPE
                        || !Arrays.equals(storedOwner.key, descriptorKey)) {
                    throw new IllegalStateException("result descriptor point read returned another key/type");
                }
                descriptor = TargetQuotaIncarnation.decode(storedOwner.payload);
                if (!Arrays.equals(descriptor.key(), descriptorKey)) {
                    throw new IllegalStateException("result owner key and canonical descriptor disagree");
                }
                descriptor.latestMutation().requireAtOrBefore(sourceSequence, frontier);
                for (var ownerStamp : java.util.List.of(descriptor.allocation(), descriptor.latestMutation())) {
                    final var same = stamps.putIfAbsent(ownerStamp.sequence(), ownerStamp);
                    if (same != null && !same.equals(ownerStamp)) {
                        throw new IllegalStateException("result owner and ledger contradict the same source mutation");
                    }
                }
                owners.put(record.primaryIdentity(), descriptor);
            }
            record.requireOwner(descriptor);
            final var charge = record.recordCharge();
            contributions.merge(record.primaryIdentity(), charge, CapacityVector::add);
            contributions.merge(record.tenantIdentity(), charge, CapacityVector::add);
            total = total.add(charge);
        }
        TargetQuotaMutation earlier = null;
        for (var stamp : stamps.values()) {
            if (earlier != null) {
                earlier.requireAtOrBefore(stamp);
            }
            earlier = stamp;
        }
        for (var record : records.values()) {
            if (record.kind() == TargetResultRecord.Kind.COMMAND || record.kind() == TargetResultRecord.Kind.SYSTEM) {
                continue;
            }
            final byte[] firstKey = Bytes.concat(
                    new byte[] {
                        (byte)
                                (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM
                                        ? TargetKeyCodec.RESULT_SYSTEM_TAG
                                        : TargetKeyCodec.RESULT_COMMAND_TAG),
                        TargetKeyCodec.KEY_FORMAT
                    },
                    record.logicalId());
            record.requireFirst(records.get(HexFormat.of().formatHex(firstKey)));
        }
        final var summary = new Summary(
                scope,
                expectedLineage,
                sourceSequence,
                frontier,
                records.size(),
                owners.size(),
                bytes,
                contributions,
                total);
        authority.requireComplete(summary);
        return summary;
    }

    private static long consume(final long used, final Stored stored, final Limits limits) {
        final long next =
                Math.addExact(used, Math.addExact(12L, Math.addExact((long) stored.key.length, stored.payload.length)));
        if (next > limits.maximumEncodedBytes()) {
            throw new IllegalStateException("result recovery encoded-byte budget exhausted");
        }
        return next;
    }

    private static byte[] descriptorKey(final TargetQuotaIdentity identity) {
        final byte[] counterKey = identity.key();
        return Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_INCARNATION_TAG, TargetKeyCodec.KEY_FORMAT},
                Arrays.copyOfRange(counterKey, 2, counterKey.length));
    }
}
