package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.CommandId;
import com.nereusstream.delay.protocol.ProtocolTuple;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaAccounting;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/** Immutable first logical results and separate physical-position audits, with frozen record ownership. */
public final class TargetResultRecord {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 35;
    /** Target schema ceiling; activated resource budgets may impose a smaller complete-operation limit. */
    public static final int MAX_AUTHOR_BYTES = 1 << 20;

    public static final int MAX_PAYLOAD_BYTES = MAX_AUTHOR_BYTES + TargetSourcePosition.MAX_CANONICAL_BYTES + 512;
    public static final int MAX_CANONICAL_BYTES = 2
            + 2
            + 43
            + 3
            + TargetQuotaIdentity.MAX_CANONICAL_BYTES
            + 34
            + 2
            + TargetQuotaAccounting.MAX_CANONICAL_BYTES
            + 18
            + 4
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES
            + 4
            + MAX_PAYLOAD_BYTES
            + 34
            + 4
            + TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES
            + 34;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-result-record\0");

    public enum Kind {
        COMMAND(1),
        RESULT(2),
        SYSTEM(3),
        POSITION_COMMAND(4),
        POSITION_SYSTEM(5);
        private final int wire;

        Kind(int wire) {
            this.wire = wire;
        }

        public int wireValue() {
            return wire;
        }

        public boolean position() {
            return this == POSITION_COMMAND || this == POSITION_SYSTEM;
        }

        private boolean referencesFirst() {
            return this == RESULT || position();
        }

        private boolean commandIdentity() {
            return this == COMMAND || this == RESULT || this == POSITION_COMMAND;
        }
    }
    /**
     * Validates actual accepted source/tuple/hash/signature/routing, immutable owner selection, exact first
     * outcome, allocation attachment and complete before/absence read sets. References may be part of the
     * same atomic proposal. Duplicate source processing must retain the first logical record. No default.
     */
    @FunctionalInterface
    public interface CreationAuthority {
        void requireAuthorized(TargetResultRecord record, TargetResultRecord first);
    }
    /** Requires source retry/query/replay windows, closed fences, catalog/pins, all references and actual deletion. */
    @FunctionalInterface
    public interface DeletionAuthority {
        void requireAuthorized(TargetResultRecord record, RecoveryFloorRef floor, TargetQuotaMutation deletion);
    }

    private final Kind kind;
    private final byte[] logicalId;
    private final TargetQuotaIdentity identity;
    private final byte[] tenant;
    private final TargetQuotaAccounting accounting;
    private final byte[] lineage;
    private final TargetQuotaMutation mutation;
    private final byte[] payload;
    private final byte[] firstDigest;
    private final TargetQuotaIncarnation allocation;
    private final byte[] digest;

    public TargetResultRecord(
            final Kind kind,
            final byte[] logicalId,
            final TargetQuotaIdentity identity,
            final byte[] tenant,
            final TargetQuotaAccounting accounting,
            final byte[] lineage,
            final TargetQuotaMutation mutation,
            final byte[] payload,
            final byte[] firstDigest,
            final TargetQuotaIncarnation allocation) {
        this.kind = Objects.requireNonNull(kind, "kind");
        this.logicalId = assigned(logicalId, kind.commandIdentity() ? CommandId.LENGTH : 32, "logicalId");
        this.identity = Objects.requireNonNull(identity, "identity");
        this.tenant = assigned(tenant, 32, "tenantRoutingScope");
        this.accounting = Objects.requireNonNull(accounting, "accounting");
        this.lineage = assigned(lineage, 16, "recoveryLineage");
        this.mutation = Objects.requireNonNull(mutation, "mutation");
        mutation.requireSourceApplied();
        if (payload == null
                || payload.length == 0
                || payload.length > MAX_PAYLOAD_BYTES
                || identity.kind().isMirror()
                || !identity.shard().equals(mutation.source().shardId())
                || (kind.position() && identity.kind() != TargetQuotaIdentity.Kind.SHARD)
                || kind.referencesFirst() != (firstDigest != null)) {
            throw new IllegalArgumentException("invalid Target result owner/source/payload/reference");
        }
        if (kind.commandIdentity()
                && !new CommandId(logicalId).routingId().shardId().equals(identity.shard())) {
            throw new IllegalArgumentException("Target result Command belongs to another Shard");
        }
        this.payload = Bytes.copy(payload);
        this.firstDigest = firstDigest == null ? null : assigned(firstDigest, 32, "firstLogicalRecordDigest");
        this.allocation = allocation;
        validatePayload();
        validateAllocation();
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    private void validatePayload() {
        byte[] source = null;
        if (kind == Kind.COMMAND) {
            if (payload.length < 8
                    || Bytes.readU32be(payload, 0) != 2
                    || Bytes.readU32be(payload, 4) > 30
                    || payload.length > 100 + TargetSourcePosition.MAX_CANONICAL_BYTES) {
                throw new IllegalArgumentException("Target command tuple/result exceeds its structural bound");
            }
            final var command = CommandDedupeRecord.decode(payload);
            assigned(command.commandHash(), 32, "commandHash");
            if (!Arrays.equals(payload, command.encode())) {
                throw new IllegalArgumentException("Target command evidence requires canonical payload version 2");
            }
            source = command.result().appliedSourcePosition();
        } else if (kind == Kind.RESULT) {
            final var result = CommandResult.decode(payload);
            if (!Arrays.equals(payload, result.encode())) {
                throw new IllegalArgumentException("noncanonical Target result");
            }
            source = result.appliedSourcePosition();
        } else if (kind == Kind.SYSTEM) {
            final var result = SystemMutationResult.decode(payload);
            if (!Arrays.equals(logicalId, result.mutationId()) || result.authorIdentity().length > MAX_AUTHOR_BYTES) {
                throw new IllegalArgumentException("Target System result ID/author bound mismatch");
            }
            assigned(result.mutationHash(), 32, "mutationHash");
            final var authorReader = new CanonicalProtobuf.Reader(result.authorIdentity(), false);
            if (!authorReader.hasRemaining()) {
                throw new IllegalArgumentException("missing result author");
            }
            final var branch = authorReader.next();
            if (authorReader.hasRemaining()) {
                throw new IllegalArgumentException("multiple result author branches");
            }
            final var bodyReader =
                    new CanonicalProtobuf.Reader(QueryCodecSupport.bytes(branch, branch.number()), false);
            int fields = 0;
            while (bodyReader.hasRemaining()) {
                if (++fields > 4) {
                    throw new IllegalArgumentException("excess result author fields");
                }
                bodyReader.next();
            }
            AuthorIdentity.decode(result.authorIdentity()).requireFor(result.mutationType());
            source = result.appliedSourcePosition();
        } else if (!Arrays.equals(logicalId, payload)) {
            throw new IllegalArgumentException("physical audit payload differs from its logical identity");
        }
        if (source != null
                && !Arrays.equals(
                        mutation.source().canonicalBytes(),
                        TargetSourcePosition.decode(source).canonicalBytes())) {
            throw new IllegalArgumentException("Target result source differs from its original accounting mutation");
        }
    }

    private void validateAllocation() {
        if (allocation == null) {
            return;
        }
        if (kind != Kind.SYSTEM) {
            throw new IllegalArgumentException("allocation result belongs to a System result");
        }
        final var result = SystemMutationResult.decode(payload);
        if (result.mutationType() != SystemMutationType.APPLY_SHARD_CONTROL
                || result.applyStatus() != ApplyStatus.APPLIED
                || result.stableCode() != StableCode.OK
                || allocation.draining()
                || !allocation.allocation().equals(mutation)
                || !allocation.identity().shard().equals(identity.shard())
                || !Arrays.equals(allocation.tenantScope(), tenant)
                || !Arrays.equals(allocation.recoveryLineage(), lineage)) {
            throw new IllegalArgumentException(
                    "allocation attachment is not this successful source's exact OPEN origin");
        }
    }

    public static TargetResultRecord command(
            final TargetQuotaIncarnation owner,
            final CommandId id,
            final ProtocolTuple tuple,
            final byte[] commandHash,
            final CommandResult result,
            final TargetQuotaMutation mutation,
            final CreationAuthority authority) {
        return create(
                Kind.COMMAND,
                id.bytes(),
                owner,
                mutation,
                new CommandDedupeRecord(tuple, commandHash, result).encode(),
                null,
                null,
                null,
                authority);
    }

    public static TargetResultRecord result(final TargetResultRecord command, final CreationAuthority authority) {
        Objects.requireNonNull(authority, "creationAuthority");
        if (command.kind != Kind.COMMAND) {
            throw new IllegalArgumentException("query result requires original Command evidence");
        }
        final var result = new TargetResultRecord(
                Kind.RESULT,
                command.logicalId,
                command.identity,
                command.tenant,
                command.accounting,
                command.lineage,
                command.mutation,
                CommandDedupeRecord.decode(command.payload).result().encode(),
                command.digest,
                null);
        result.requireFirst(command);
        authority.requireAuthorized(result, command);
        return result;
    }

    public static TargetResultRecord system(
            final TargetQuotaIncarnation owner,
            final SystemMutationResult result,
            final TargetQuotaMutation mutation,
            final TargetQuotaIncarnation allocation,
            final CreationAuthority authority) {
        return create(
                Kind.SYSTEM, result.mutationId(), owner, mutation, result.encode(), null, allocation, null, authority);
    }

    public static TargetResultRecord position(
            final TargetQuotaIncarnation shardOwner,
            final TargetResultRecord first,
            final TargetQuotaMutation physicalMutation,
            final CreationAuthority authority) {
        if (first.kind != Kind.COMMAND && first.kind != Kind.SYSTEM) {
            throw new IllegalArgumentException("POSITION requires first logical evidence");
        }
        return create(
                first.kind == Kind.COMMAND ? Kind.POSITION_COMMAND : Kind.POSITION_SYSTEM,
                first.logicalId,
                shardOwner,
                physicalMutation,
                first.logicalId,
                first.digest,
                null,
                first,
                authority);
    }

    private static TargetResultRecord create(
            final Kind kind,
            final byte[] id,
            final TargetQuotaIncarnation owner,
            final TargetQuotaMutation mutation,
            final byte[] payload,
            final byte[] firstDigest,
            final TargetQuotaIncarnation allocation,
            final TargetResultRecord first,
            final CreationAuthority authority) {
        Objects.requireNonNull(authority, "creationAuthority");
        final var result = new TargetResultRecord(
                kind,
                id,
                owner.identity(),
                owner.tenantScope(),
                owner.accounting(),
                owner.recoveryLineage(),
                mutation,
                payload,
                firstDigest,
                allocation);
        result.requireOwner(owner);
        owner.latestMutation().requireAtOrBefore(mutation);
        if (first != null) {
            result.requireFirst(first);
        }
        authority.requireAuthorized(result, first);
        return result;
    }

    public void requireOwner(final TargetQuotaIncarnation owner) {
        if (!identity.equals(owner.identity())
                || !Arrays.equals(tenant, owner.tenantScope())
                || !accounting.equals(owner.accounting())
                || !Arrays.equals(lineage, owner.recoveryLineage())) {
            throw new IllegalStateException("result attribution changed its frozen owner");
        }
        owner.allocation().requireAtOrBefore(mutation);
    }

    public void requireFirst(final TargetResultRecord first) {
        if (!kind.referencesFirst()
                || first == null
                || first.kind != (kind == Kind.POSITION_SYSTEM ? Kind.SYSTEM : Kind.COMMAND)
                || !Arrays.equals(firstDigest, first.digest)
                || !Arrays.equals(logicalId, first.logicalId)
                || !Arrays.equals(tenant, first.tenant)
                || !Arrays.equals(lineage, first.lineage)
                || !identity.shard().equals(first.identity.shard())) {
            throw new IllegalStateException("result or POSITION lost its exact first logical record");
        }
        first.mutation.requireAtOrBefore(mutation);
        if (kind == Kind.RESULT
                && (!identity.equals(first.identity)
                        || !accounting.equals(first.accounting)
                        || !mutation.equals(first.mutation)
                        || !Arrays.equals(
                                payload,
                                CommandDedupeRecord.decode(first.payload)
                                        .result()
                                        .encode()))) {
            throw new IllegalStateException("query result changed first outcome/source/owner");
        }
    }

    public void requireStored(final byte[] key, final int type, final byte[] actualPayload) {
        if (type != VALUE_TYPE || !Arrays.equals(key, key()) || !Arrays.equals(actualPayload, canonicalBytes())) {
            throw new IllegalStateException("actual Target result key/type/full value changed");
        }
    }

    public void requireDeletion(
            final RecoveryFloorRef floor, final TargetQuotaMutation deletion, final DeletionAuthority authority) {
        Objects.requireNonNull(authority, "deletionAuthority");
        Objects.requireNonNull(floor, "floor");
        deletion.requireAfter(mutation);
        mutation.requireCoveredByFloor(floor);
        if (!Arrays.equals(lineage, floor.recoveryLineageId())
                || floor.appliedSourcePosition().compareTo(deletion.source()) >= 0
                || Long.compareUnsigned(floor.includedMutationSequence(), deletion.sequence()) >= 0) {
            throw new IllegalStateException("result deletion Floor/source/lineage mismatch");
        }
        authority.requireAuthorized(this, floor, deletion);
    }

    public Kind kind() {
        return kind;
    }

    public byte[] logicalId() {
        return Bytes.copy(logicalId);
    }

    public TargetQuotaIdentity primaryIdentity() {
        return identity;
    }

    public TargetQuotaIdentity tenantIdentity() {
        return new TargetQuotaIdentity(
                identity.kind() == TargetQuotaIdentity.Kind.TARGET
                        ? TargetQuotaIdentity.Kind.TENANT_TARGET
                        : TargetQuotaIdentity.Kind.TENANT_SHARD,
                identity.shard(),
                identity.accountingIncarnation(),
                identity.target(),
                tenant);
    }

    public TargetQuotaAccounting accounting() {
        return accounting;
    }

    public byte[] tenantScope() {
        return Bytes.copy(tenant);
    }

    public byte[] recoveryLineage() {
        return Bytes.copy(lineage);
    }

    public TargetQuotaMutation mutation() {
        return mutation;
    }

    public byte[] typedPayload() {
        return Bytes.copy(payload);
    }

    public byte[] firstDigest() {
        return firstDigest == null ? null : Bytes.copy(firstDigest);
    }

    public TargetQuotaIncarnation allocation() {
        return allocation;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] key() {
        final int tag =
                switch (kind) {
                    case COMMAND -> TargetKeyCodec.RESULT_COMMAND_TAG;
                    case RESULT -> TargetKeyCodec.RESULT_QUERY_TAG;
                    case SYSTEM -> TargetKeyCodec.RESULT_SYSTEM_TAG;
                    case POSITION_COMMAND, POSITION_SYSTEM -> TargetKeyCodec.RESULT_POSITION_TAG;
                };
        return Bytes.concat(
                new byte[] {(byte) tag, TargetKeyCodec.KEY_FORMAT},
                kind.position() ? mutation.source().canonicalBytes() : logicalId);
    }

    public CapacityVector recordCharge() {
        final var recordClass = (kind == Kind.RESULT || kind == Kind.SYSTEM)
                ? TargetQuotaAccounting.RecordClass.RESULT
                : TargetQuotaAccounting.RecordClass.EVIDENCE;
        return accounting.recordCharge(recordClass, key().length, canonicalBytes().length);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.uint32(out, 2, kind.wire);
            CanonicalProtobuf.bytes(out, 3, logicalId);
            CanonicalProtobuf.bytes(out, 4, identity.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, tenant);
            CanonicalProtobuf.bytes(out, 6, accounting.canonicalBytes());
            CanonicalProtobuf.bytes(out, 7, lineage);
            CanonicalProtobuf.bytes(out, 8, mutation.canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, payload);
            if (firstDigest != null) {
                CanonicalProtobuf.bytes(out, 10, firstDigest);
            }
            if (allocation != null) {
                CanonicalProtobuf.bytes(out, 11, allocation.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 12, digest);
        });
    }

    public static TargetResultRecord decode(final byte[] encoded) {
        if (encoded == null || encoded.length > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("Target result exceeds byte bound");
        }
        final var reader = new CanonicalProtobuf.Reader(encoded, false);
        final var f = new ArrayList<CanonicalProtobuf.Reader.Field>();
        while (reader.hasRemaining()) {
            if (f.size() == 12) {
                throw new IllegalArgumentException("excess Target result fields");
            }
            f.add(reader.next());
        }
        if (f.size() < 10) {
            throw new IllegalArgumentException("missing Target result fields");
        }
        final int n = QueryCodecSupport.uint32(f.get(1), 2);
        if (n < 1 || n > Kind.values().length || QueryCodecSupport.uint32(f.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target result schema/kind");
        }
        final Kind kind = Kind.values()[n - 1];
        final var numbers = new ArrayList<Integer>();
        for (int field = 1; field <= 9; field++) {
            numbers.add(field);
        }
        if (kind.referencesFirst()) {
            numbers.add(10);
        }
        if (f.stream().anyMatch(field -> field.number() == 11)) {
            numbers.add(11);
        }
        numbers.add(12);
        QueryCodecSupport.requireNumbers(
                f, numbers.stream().mapToInt(Integer::intValue).toArray(), "TargetResultRecord");
        int index = 9;
        byte[] first = null;
        TargetQuotaIncarnation allocation = null;
        if (kind.referencesFirst()) {
            first = QueryCodecSupport.fixed(f.get(index++), 10, 32);
        }
        if (index < f.size() && f.get(index).number() == 11) {
            allocation = TargetQuotaIncarnation.decode(QueryCodecSupport.bytes(f.get(index++), 11));
        }
        final byte[] expected = QueryCodecSupport.fixed(f.get(index++), 12, 32);
        if (index != f.size()) {
            throw new IllegalArgumentException("excess Target result fields");
        }
        final var result = new TargetResultRecord(
                kind,
                QueryCodecSupport.bytes(f.get(2), 3),
                TargetQuotaIdentity.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                QueryCodecSupport.fixed(f.get(4), 5, 32),
                TargetQuotaAccounting.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                QueryCodecSupport.fixed(f.get(6), 7, 16),
                TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(7), 8)),
                QueryCodecSupport.bytes(f.get(8), 9),
                first,
                allocation);
        if (!Bytes.constantTimeEquals(result.digest, expected)) {
            throw new IllegalArgumentException("Target result digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetResultRecord");
        return result;
    }

    private static byte[] assigned(final byte[] raw, final int length, final String name) {
        Bytes.requireLength(raw, length, name);
        if (Arrays.equals(raw, new byte[length])) {
            throw new IllegalArgumentException(name + " is unassigned");
        }
        return Bytes.copy(raw);
    }
}
