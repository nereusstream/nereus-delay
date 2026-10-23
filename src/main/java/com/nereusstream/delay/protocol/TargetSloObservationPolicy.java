package com.nereusstream.delay.protocol;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Frozen Source-scoped certification declaration. Decoding it grants no activation or custody authority. */
public record TargetSloObservationPolicy(TargetQuotaScope scope, byte[] lineage, SourcePosition sourceAnchor,
        List<Declaration> declarations, Limits limits, AuthorIdentity service, int keyVersion,
        byte[] collectorContractDigest) {
    public static final int MAX_DECLARATIONS = 3;
    public static final int MAX_SERVICE_BYTES = 1024;
    public static final int MAX_CANONICAL_BYTES = TargetQuotaScope.MAX_CANONICAL_BYTES
            + TargetSourcePosition.MAX_CANONICAL_BYTES + MAX_DECLARATIONS * Declaration.MAX_CANONICAL_BYTES
            + MAX_SERVICE_BYTES + 256;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-slo-observation-policy\0");

    /** Only the actual Command and managed Admission producers are declared in this schema. */
    public record Declaration(SloObjective objective, SloPath path) {
        public static final int MAX_OBJECTIVE_BYTES = 1024;
        public static final int MAX_CANONICAL_BYTES = MAX_OBJECTIVE_BYTES + 32;

        public Declaration {
            Objects.requireNonNull(objective); Objects.requireNonNull(path);
            if (objective.canonicalBytes().length > MAX_OBJECTIVE_BYTES
                    || objective.population() != SloPopulation.ALL_ACCEPTED
                    || objective.name() == SloObjectiveName.COMMAND_APPLIED_LATENCY && path != SloPath.NOT_APPLICABLE
                    || objective.name() == SloObjectiveName.DUE_ADMISSION_LAG
                        && path != SloPath.ORDINARY_MANAGED && path != SloPath.MANAGED_PULSAR_HANDOFF
                    || objective.name() != SloObjectiveName.COMMAND_APPLIED_LATENCY
                        && objective.name() != SloObjectiveName.DUE_ADMISSION_LAG) {
                throw new IllegalArgumentException("unsupported Target certification objective/population/path declaration");
            }
        }

        private int slot() { return objective.name() == SloObjectiveName.COMMAND_APPLIED_LATENCY ? 1 : path.wireValue(); }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1);
                CanonicalProtobuf.bytes(out, 2, objective.canonicalBytes());
                CanonicalProtobuf.uint32(out, 3, path.wireValue());
            });
        }

        public static Declaration decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 3, false, "Target SLO declaration");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3}, "Target SLO declaration");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown Target SLO declaration schema");
            final var objective = QueryCodecSupport.bytes(f.get(1), 2);
            if (objective.length > MAX_OBJECTIVE_BYTES) throw new IllegalArgumentException("Target SLO objective exceeds bound");
            final var result = new Declaration(SloObjective.decode(objective), SloPath.fromWire(QueryCodecSupport.uint32(f.get(2), 3)));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target SLO declaration");
            return result;
        }
    }

    /** Actual deployment must independently qualify all these finite reservations and retention duties. */
    public record Limits(int maximumConcurrentGenerations, int maximumOutboxRecords, long maximumOutboxBytes,
            int maximumRecordBytes, long retentionMs) {
        public static final int MAX_CANONICAL_BYTES = 96;

        public Limits {
            if (maximumConcurrentGenerations < 1 || maximumConcurrentGenerations == Integer.MAX_VALUE
                    || maximumOutboxRecords < 1 || maximumOutboxRecords == Integer.MAX_VALUE
                    || maximumOutboxBytes < 1 || maximumOutboxBytes == Long.MAX_VALUE
                    || maximumRecordBytes < 1 || maximumRecordBytes == Integer.MAX_VALUE
                    || maximumRecordBytes > maximumOutboxBytes || retentionMs < 1 || retentionMs == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Target SLO certification requires finite outbox/generation/retention bounds");
            }
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, 1);
                CanonicalProtobuf.uint32(out, 2, maximumConcurrentGenerations);
                CanonicalProtobuf.uint32(out, 3, maximumOutboxRecords);
                CanonicalProtobuf.uint64Bits(out, 4, maximumOutboxBytes);
                CanonicalProtobuf.uint32(out, 5, maximumRecordBytes);
                CanonicalProtobuf.uint64Bits(out, 6, retentionMs);
            });
        }

        public static Limits decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "Target SLO limits");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5, 6}, "Target SLO limits");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown Target SLO limits schema");
            final var result = new Limits(QueryCodecSupport.uint32(f.get(1), 2), QueryCodecSupport.uint32(f.get(2), 3),
                    QueryCodecSupport.uint64Bits(f.get(3), 4), QueryCodecSupport.uint32(f.get(4), 5), QueryCodecSupport.uint64Bits(f.get(5), 6));
            QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target SLO limits");
            return result;
        }
    }

    public TargetSloObservationPolicy {
        Objects.requireNonNull(scope); Objects.requireNonNull(limits); Objects.requireNonNull(service);
        lineage = TargetCompatibilityCodec.assigned(lineage, 16, "Target SLO recovery lineage");
        collectorContractDigest = TargetCompatibilityCodec.assigned(collectorContractDigest, 32, "Target SLO collector contract");
        sourceAnchor = TargetSourcePosition.requireBounded(sourceAnchor);
        if (scope.target() != null || !scope.shard().equals(sourceAnchor.shardId()) || keyVersion == 0
                || service.kind() != AuthorIdentity.Kind.SERVICE || service.canonicalBytes().length > MAX_SERVICE_BYTES) {
            throw new IllegalArgumentException("Target SLO policy changed original Shard/source/service scope");
        }
        final var sorted = new ArrayList<>(Objects.requireNonNull(declarations));
        if (sorted.isEmpty() || sorted.size() > MAX_DECLARATIONS) throw new IllegalArgumentException("Target SLO declaration set exceeds supported producers");
        sorted.forEach(Objects::requireNonNull);
        sorted.sort(Comparator.comparingInt(Declaration::slot));
        for (int i = 1; i < sorted.size(); i++) {
            if (sorted.get(i - 1).slot() == sorted.get(i).slot()) throw new IllegalArgumentException("duplicate Target SLO producer declaration");
        }
        declarations = List.copyOf(sorted);
    }

    @Override public byte[] lineage() { return Bytes.copy(lineage); }
    @Override public byte[] collectorContractDigest() { return Bytes.copy(collectorContractDigest); }

    public void requireSource(SourcePosition actual) {
        TargetSourcePosition.requireBounded(actual);
        if (!scope.shard().equals(actual.shardId()) || !sourceAnchor.sameSourceIdentity(actual)) {
            throw new IllegalArgumentException("Target SLO certification changed physical Source");
        }
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, lineage);
            CanonicalProtobuf.bytes(out, 4, sourceAnchor.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, CanonicalProtobuf.message(group -> {
                for (var declaration : declarations) CanonicalProtobuf.bytes(group, 1, declaration.canonicalBytes());
            }));
            CanonicalProtobuf.bytes(out, 6, limits.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, service.canonicalBytes());
            CanonicalProtobuf.uint32Bits(out, 8, keyVersion);
            CanonicalProtobuf.bytes(out, 9, collectorContractDigest);
        });
    }

    public byte[] digest() { return Bytes.sha256(DOMAIN, fields()); }

    public byte[] canonicalBytes() {
        final var encoded = Bytes.concat(fields(), CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 10, digest())));
        if (encoded.length > MAX_CANONICAL_BYTES) throw new IllegalArgumentException("Target SLO policy exceeds canonical bound");
        return encoded;
    }

    public static TargetSloObservationPolicy decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 10, false, "Target SLO policy");
        QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, "Target SLO policy");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != 1) throw new IllegalArgumentException("unknown Target SLO policy schema");
        final var group = TargetCompatibilityCodec.read(QueryCodecSupport.bytes(f.get(4), 5), MAX_DECLARATIONS * (Declaration.MAX_CANONICAL_BYTES + 8), MAX_DECLARATIONS, true, "Target SLO declarations");
        final var declarations = new ArrayList<Declaration>();
        for (var field : group) declarations.add(Declaration.decode(QueryCodecSupport.bytes(field, 1)));
        final var service = QueryCodecSupport.bytes(f.get(6), 7);
        if (service.length > MAX_SERVICE_BYTES) throw new IllegalArgumentException("Target SLO service identity exceeds bound");
        final var result = new TargetSloObservationPolicy(TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.fixed(f.get(2), 3, 16), TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                declarations, Limits.decode(QueryCodecSupport.bytes(f.get(5), 6)), AuthorIdentity.decode(service),
                QueryCodecSupport.uint32(f.get(7), 8), QueryCodecSupport.fixed(f.get(8), 9, 32));
        if (!Arrays.equals(result.digest(), QueryCodecSupport.fixed(f.get(9), 10, 32))) throw new IllegalArgumentException("Target SLO policy digest mismatch");
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target SLO policy");
        return result;
    }

    @Override public boolean equals(Object other) { return other instanceof TargetSloObservationPolicy that && Arrays.equals(canonicalBytes(), that.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest()); }
}
