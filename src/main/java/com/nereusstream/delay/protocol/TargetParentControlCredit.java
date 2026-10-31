package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Source-created paid SHARD Control credit. Pure projections never authorize funding or child allocation. */
public final class TargetParentControlCredit {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 63;
    public static final int KEY_TAG = 57;
    public static final int MAX_ALLOCATIONS = 64;
    public static final int MAX_CANONICAL_BYTES = 64
            + 6
            + Control.MAX_CANONICAL_BYTES
            + 6
            + TargetQuotaGrantActivation.MAX_CANONICAL_BYTES
            + MAX_ALLOCATIONS * (6 + Allocation.MAX_CANONICAL_BYTES);
    private static final byte[] ID_DOMAIN = Bytes.utf8("nereus-delay-target-parent-control-credit-id\0");
    private static final byte[] DEFINITION_DOMAIN =
            Bytes.utf8("nereus-delay-target-parent-control-credit-definition\0");
    private static final byte[] RECORD_DOMAIN = Bytes.utf8("nereus-delay-target-parent-control-credit\0");
    private static final byte[] SOURCE_DOMAIN =
            Bytes.utf8("nereus-delay-target-parent-control-credit-physical-source\0");

    /** Immutable definition, independent of this class's Control/Body bounds to avoid a recursive codec limit. */
    public static final class Definition {
        public static final int MAX_FUNDING_GRANT_BYTES = TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES + 128;
        public static final int MAX_CANONICAL_BYTES = 512
                + TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES
                + TargetQuotaGrant.MAX_CANONICAL_BYTES
                + MAX_FUNDING_GRANT_BYTES
                + TargetSourcePosition.MAX_CANONICAL_BYTES
                + TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES;
        private final TargetQuotaIncarnation originalRoot;
        private final TargetQuotaGrant expectedPayerGrant;
        private final CapacityGrant fundingGrant;
        private final byte[] payerAllocationBinding;
        private final SourcePosition sourceAnchor;
        private final CapacityVector funded;
        private final int maximumAllocations;
        private final byte[] creditId;
        private final byte[] digest;

        public Definition(
                TargetQuotaIncarnation originalRoot,
                TargetQuotaGrant expectedPayerGrant,
                CapacityGrant fundingGrant,
                byte[] payerAllocationBinding,
                SourcePosition sourceAnchor,
                CapacityVector funded,
                int maximumAllocations) {
            this.originalRoot = Objects.requireNonNull(originalRoot);
            this.expectedPayerGrant = Objects.requireNonNull(expectedPayerGrant);
            this.fundingGrant = Objects.requireNonNull(fundingGrant);
            this.payerAllocationBinding = TargetCompatibilityCodec.assigned(
                    payerAllocationBinding, 32, "original funding allocation binding");
            this.sourceAnchor = TargetSourcePosition.requireBounded(sourceAnchor);
            this.funded = requireNativeVector(funded);
            if (originalRoot.identity().kind() != TargetQuotaIdentity.Kind.SHARD
                    || originalRoot.draining()
                    || !originalRoot.scope().equals(expectedPayerGrant.scope())
                    || !Arrays.equals(
                            originalRoot.accounting().canonicalBytes(),
                            expectedPayerGrant.accounting().canonicalBytes())
                    || !originalRoot.scope().shard().equals(sourceAnchor.shardId())
                    || !originalRoot.allocation().source().sameSourceIdentity(sourceAnchor)
                    || fundingGrant.kind() != CapacityGrantKind.NON_OUTCOME_CONTROL
                    || !expectedPayerGrant.limit().resources().covers(funded)
                    || maximumAllocations < 1
                    || maximumAllocations > MAX_ALLOCATIONS) {
                throw new IllegalArgumentException(
                        "parent Control credit changed its original SHARD payer/funding/Native envelope");
            }
            if (originalRoot.canonicalBytes().length > TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES
                    || fundingGrant.canonicalBytes().length > MAX_FUNDING_GRANT_BYTES) {
                throw new IllegalArgumentException(
                        "parent Control credit definition exceeds its original closed proof bounds");
            }
            this.maximumAllocations = maximumAllocations;
            // Do not include grant digest, active Native grant, Root incarnation, Owner, alias or Source offset in the
            // key.
            // Same funding identity with changed bytes must collide and be rejected, never create fresh credit.
            creditId = Bytes.sha256(ID_DOMAIN, CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, fundingGrant.kind().wireValue());
                CanonicalProtobuf.bytes(out, 2, fundingGrant.grantId());
                CanonicalProtobuf.uint64Bits(out, 3, fundingGrant.reserveSourceVersion());
                CanonicalProtobuf.bytes(out, 4, this.payerAllocationBinding);
                CanonicalProtobuf.bytes(out, 5, physicalSourceDigest(sourceAnchor));
            }));
            digest = Bytes.sha256(DEFINITION_DOMAIN, fields());
            if (canonicalBytes().length > MAX_CANONICAL_BYTES) {
                throw new IllegalArgumentException("parent credit definition exceeds schema bound");
            }
        }

        public TargetQuotaIncarnation originalRoot() {
            return originalRoot;
        }

        public TargetQuotaGrant expectedPayerGrant() {
            return expectedPayerGrant;
        }

        public TargetQuotaScope scope() {
            return originalRoot.scope();
        }

        public byte[] lineage() {
            return originalRoot.recoveryLineage();
        }

        public TargetQuotaAccounting accounting() {
            return originalRoot.accounting();
        }

        public CapacityGrant fundingGrant() {
            return fundingGrant;
        }

        public byte[] payerAllocationBinding() {
            return Bytes.copy(payerAllocationBinding);
        }

        public SourcePosition sourceAnchor() {
            return sourceAnchor;
        }

        public CapacityVector funded() {
            return funded;
        }

        public int maximumAllocations() {
            return maximumAllocations;
        }

        public byte[] creditId() {
            return Bytes.copy(creditId);
        }

        public byte[] digest() {
            return Bytes.copy(digest);
        }

        public Reference reference() {
            return new Reference(creditId, digest);
        }

        public byte[] key() {
            return reference().key();
        }

        public void requireSource(SourcePosition source) {
            TargetSourcePosition.requireBounded(source);
            if (!scope().shard().equals(source.shardId()) || !sourceAnchor.sameSourceIdentity(source)) {
                throw new IllegalArgumentException("parent Control credit changed its original physical Source");
            }
        }
        /** Current drain/latest mutations do not replace the original immutable payer allocation descriptor. */
        public void requireRoot(TargetQuotaIncarnation current) {
            if (current == null
                    || !originalRoot.identity().equals(current.identity())
                    || !Arrays.equals(originalRoot.tenantScope(), current.tenantScope())
                    || !Arrays.equals(
                            originalRoot.accounting().canonicalBytes(),
                            current.accounting().canonicalBytes())
                    || !Arrays.equals(originalRoot.recoveryLineage(), current.recoveryLineage())
                    || !originalRoot.allocation().equals(current.allocation())) {
                throw new IllegalArgumentException(
                        "parent Control credit changed its frozen Root/tenant/accounting/lineage/origin");
            }
        }

        private byte[] fields() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.bytes(out, 2, originalRoot.canonicalBytes());
                CanonicalProtobuf.bytes(out, 3, expectedPayerGrant.canonicalBytes());
                CanonicalProtobuf.bytes(out, 4, fundingGrant.canonicalBytes());
                CanonicalProtobuf.bytes(out, 5, payerAllocationBinding);
                CanonicalProtobuf.bytes(out, 6, sourceAnchor.canonicalBytes());
                CanonicalProtobuf.bytes(out, 7, funded.canonicalBytes());
                CanonicalProtobuf.uint32(out, 8, maximumAllocations);
                CanonicalProtobuf.bytes(out, 9, creditId);
            });
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                out.writeBytes(fields());
                CanonicalProtobuf.bytes(out, 10, digest);
            });
        }

        public static Definition decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(
                    encoded, MAX_CANONICAL_BYTES, 10, false, "parent Control credit definition");
            QueryCodecSupport.requireNumbers(
                    f, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, "parent Control credit definition");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) {
                throw new IllegalArgumentException("unknown parent credit definition schema");
            }
            final var root = QueryCodecSupport.bytes(f.get(1), 2);
            final var funding = QueryCodecSupport.bytes(f.get(3), 4);
            final var vector = QueryCodecSupport.bytes(f.get(6), 7);
            if (root.length > TargetQuotaIncarnation.MAX_ALLOCATION_CANONICAL_BYTES
                    || funding.length > MAX_FUNDING_GRANT_BYTES
                    || vector.length > TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES) {
                throw new IllegalArgumentException("parent definition nested bound exceeded");
            }
            final var value = new Definition(
                    TargetQuotaIncarnation.decode(root),
                    TargetQuotaGrant.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                    CapacityGrant.decode(funding),
                    QueryCodecSupport.fixed(f.get(4), 5, 32),
                    TargetSourcePosition.decode(QueryCodecSupport.bytes(f.get(5), 6)),
                    CapacityVector.decode(vector),
                    QueryCodecSupport.uint32(f.get(7), 8));
            if (!Arrays.equals(value.creditId, QueryCodecSupport.fixed(f.get(8), 9, 32))
                    || !Arrays.equals(value.digest, QueryCodecSupport.fixed(f.get(9), 10, 32))) {
                throw new IllegalArgumentException("parent credit definition identity/digest mismatch");
            }
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "parent Control credit definition");
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Definition that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(digest);
        }
    }

    /** Stable original funding reference, unchanged by later immutable child allocations. */
    public record Reference(byte[] creditId, byte[] definitionDigest) {
        public static final int MAX_CANONICAL_BYTES = 128;

        public Reference {
            creditId = TargetCompatibilityCodec.assigned(creditId, 32, "original parent credit ID");
            definitionDigest =
                    TargetCompatibilityCodec.assigned(definitionDigest, 32, "parent credit original definition");
        }

        @Override
        public byte[] creditId() {
            return Bytes.copy(creditId);
        }

        @Override
        public byte[] definitionDigest() {
            return Bytes.copy(definitionDigest);
        }

        public byte[] key() {
            return Bytes.concat(new byte[] {KEY_TAG, TargetKeyCodec.KEY_FORMAT}, creditId);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.bytes(out, 2, creditId);
                CanonicalProtobuf.bytes(out, 3, definitionDigest);
            });
        }

        public static Reference decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(
                    encoded, MAX_CANONICAL_BYTES, 3, false, "parent Control credit reference");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3}, "parent Control credit reference");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) {
                throw new IllegalArgumentException("unknown parent credit reference schema");
            }
            final var value =
                    new Reference(QueryCodecSupport.fixed(f.get(1), 2, 32), QueryCodecSupport.fixed(f.get(2), 3, 32));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "parent Control credit reference");
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Reference that
                    && Arrays.equals(creditId, that.creditId)
                    && Arrays.equals(definitionDigest, that.definitionDigest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(Arrays.hashCode(creditId), Arrays.hashCode(definitionDigest));
        }
    }

    /**
     * Full original signed funding Control and Source stamp; signature, first/POS and slice authority stay independent.
     */
    public record Control(SystemMutation input, TargetQuotaMutation appliedAt) {
        public static final int MAX_INPUT_BYTES = TargetParentControlCreditBody.MAX_INPUT_BYTES;
        public static final int MAX_CANONICAL_BYTES =
                16 + MAX_INPUT_BYTES + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;

        public Control {
            Objects.requireNonNull(input);
            Objects.requireNonNull(appliedAt).requireSourceApplied();
            if (input.type() != SystemMutationType.APPLY_SHARD_CONTROL
                    || input.canonicalEnvelope().length > MAX_INPUT_BYTES) {
                throw new IllegalArgumentException("parent credit requires its complete bounded original Control");
            }
            final var body = TargetParentControlCreditBody.decode(input.canonicalBody());
            final var definition = body.request().definition();
            definition.requireSource(appliedAt.source());
            if (!Arrays.equals(input.logicalOperationIdentity(), body.logicalIdentity())
                    || !input.shardId().equals(body.shard())
                    || !input.shardId().equals(appliedAt.source().shardId())
                    || input.retryUntilEpochMs() != body.retryUntil()
                    || appliedAt.source().brokerPersistenceTimeEpochMs() > input.retryUntilEpochMs()
                    || !Arrays.equals(appliedAt.mutationDigest(), Bytes.sha256(input.canonicalEnvelope()))
                    || AuthorIdentity.decode(input.authorIdentity()).kind() != AuthorIdentity.Kind.CONTROL
                    || appliedAt.source().compareTo(definition.sourceAnchor()) <= 0) {
                throw new IllegalArgumentException(
                        "parent credit changed its exact original Control/Source/anchor/retry binding");
            }
            appliedAt.requireAfter(definition.originalRoot().allocation());
        }

        public TargetParentControlCreditBody body() {
            return TargetParentControlCreditBody.decode(input.canonicalBody());
        }

        public Definition definition() {
            return body().request().definition();
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.bytes(out, 2, input.canonicalEnvelope());
                CanonicalProtobuf.bytes(out, 3, appliedAt.canonicalBytes());
            });
        }

        public static Control decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(
                    encoded, MAX_CANONICAL_BYTES, 3, false, "parent credit original applied Control");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3}, "parent credit original applied Control");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) {
                throw new IllegalArgumentException("unknown parent credit Control schema");
            }
            final var input = QueryCodecSupport.bytes(f.get(1), 2);
            if (input.length > MAX_INPUT_BYTES) {
                throw new IllegalArgumentException("original parent Control exceeds input bound");
            }
            final var value = new Control(
                    SystemMutation.decodeTargetEnvelope(input, SystemMutationType.APPLY_SHARD_CONTROL),
                    TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)));
            QueryCodecSupport.requireCanonical(
                    encoded, value.canonicalBytes(), "parent credit original applied Control");
            return value;
        }
    }

    /**
     * Irreversible original child commitment; a complete paid Scope row and Source proof must independently join it.
     */
    public record Allocation(
            TargetSloControlScopeHeader.Reference scope,
            byte[] allocationGrantDigest,
            CapacityVector commitment,
            TargetQuotaMutation allocatedAt) {
        public static final int MAX_CANONICAL_BYTES = 64
                + TargetSloControlScopeHeader.Reference.MAX_CANONICAL_BYTES
                + TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES
                + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;

        public Allocation {
            Objects.requireNonNull(scope);
            allocationGrantDigest =
                    TargetCompatibilityCodec.assigned(allocationGrantDigest, 32, "immutable child allocation grant");
            commitment = requireNativeVector(commitment);
            Objects.requireNonNull(allocatedAt).requireSourceApplied();
        }

        @Override
        public byte[] allocationGrantDigest() {
            return Bytes.copy(allocationGrantDigest);
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
                CanonicalProtobuf.bytes(out, 3, allocationGrantDigest);
                CanonicalProtobuf.bytes(out, 4, commitment.canonicalBytes());
                CanonicalProtobuf.bytes(out, 5, allocatedAt.canonicalBytes());
            });
        }

        private static long maximumCanonicalBytes(SourcePosition source) {
            return MAX_CANONICAL_BYTES
                    - (long) TargetSourcePosition.MAX_CANONICAL_BYTES
                    + TargetQuotaBookkeeping.maximumSourceBytes(source);
        }

        public static Allocation decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(
                    encoded, MAX_CANONICAL_BYTES, 5, false, "parent credit child allocation");
            QueryCodecSupport.requireNumbers(f, new int[] {1, 2, 3, 4, 5}, "parent credit child allocation");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) {
                throw new IllegalArgumentException("unknown parent child allocation schema");
            }
            final var vector = QueryCodecSupport.bytes(f.get(3), 4);
            if (vector.length > TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES) {
                throw new IllegalArgumentException("child allocation vector exceeds bound");
            }
            final var value = new Allocation(
                    TargetSloControlScopeHeader.Reference.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                    QueryCodecSupport.fixed(f.get(2), 3, 32),
                    CapacityVector.decode(vector),
                    TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(4), 5)));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "parent credit child allocation");
            return value;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof Allocation that
                    && scope.equals(that.scope)
                    && Arrays.equals(allocationGrantDigest, that.allocationGrantDigest)
                    && commitment.equals(that.commitment)
                    && allocatedAt.equals(that.allocatedAt);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(canonicalBytes());
        }
    }

    private final Control funding;
    private final Definition definition;
    private final TargetQuotaGrantActivation actualPayerGrant;
    private final List<Allocation> allocations;
    private final CapacityVector allocated;
    private final CapacityVector remaining;
    private final CapacityVector actualStateCharge;
    private final CapacityVector growthReserve;
    private final byte[] digest;

    private TargetParentControlCredit(
            Control funding, TargetQuotaGrantActivation actualPayerGrant, List<Allocation> allocations) {
        this.funding = Objects.requireNonNull(funding);
        this.actualPayerGrant = Objects.requireNonNull(actualPayerGrant);
        definition = funding.definition();
        if (!Arrays.equals(
                        actualPayerGrant.grant().canonicalBytes(),
                        definition.expectedPayerGrant().canonicalBytes())
                || actualPayerGrant.grant().scope().target() != null) {
            throw new IllegalArgumentException(
                    "parent credit changed its actual original Native SHARD grant activation");
        }
        definition.requireSource(actualPayerGrant.mutation().source());
        actualPayerGrant.mutation().requireAtOrBefore(funding.appliedAt());
        if (allocations == null || allocations.size() > definition.maximumAllocations()) {
            throw new IllegalArgumentException("parent child allocation count exceeds its frozen envelope");
        }
        this.allocations = List.copyOf(allocations);
        final var scopes = new HashSet<String>();
        final var grants = new HashSet<String>();
        CapacityVector committed = CapacityVector.empty();
        TargetQuotaMutation prior = funding.appliedAt();
        for (var child : this.allocations) {
            Objects.requireNonNull(child);
            definition.requireSource(child.allocatedAt().source());
            child.allocatedAt().requireAfter(prior);
            if (!scopes.add(Bytes.hex(child.scope().scopeId()))
                    || !grants.add(Bytes.hex(child.allocationGrantDigest()))) {
                throw new IllegalArgumentException(
                        "parent credit cannot allocate the same Scope ID or immutable grant twice");
            }
            committed = committed.add(child.commitment());
            prior = child.allocatedAt();
        }
        allocated = committed;
        remaining = definition.funded().subtract(allocated);
        digest = Bytes.sha256(RECORD_DOMAIN, fields());
        final int actualBytes = canonicalBytes().length;
        if (actualBytes > MAX_CANONICAL_BYTES) {
            throw new IllegalArgumentException("parent credit exceeds its closed record bound");
        }
        actualStateCharge = definition
                .accounting()
                .recordCharge(TargetQuotaAccounting.RecordClass.STATE, key().length, actualBytes);
        final long maximumBytes = maximumCanonicalBytes();
        if (actualBytes > maximumBytes) {
            throw new IllegalArgumentException("parent credit exceeds its frozen allocation growth envelope");
        }
        growthReserve = definition
                .accounting()
                .recordCharge(TargetQuotaAccounting.RecordClass.STATE, key().length, maximumBytes)
                .subtract(actualStateCharge);
    }

    public static TargetParentControlCredit fund(
            Definition definition,
            SystemMutation originalControl,
            TargetQuotaGrantActivation actualPayerGrant,
            TargetQuotaMutation fundedAt) {
        final var input = new Control(originalControl, fundedAt);
        if (!Objects.requireNonNull(definition).equals(input.definition())) {
            throw new IllegalArgumentException("funding Control changed its original full definition");
        }
        return new TargetParentControlCredit(input, actualPayerGrant, List.of());
    }

    public Definition definition() {
        return definition;
    }

    public Control funding() {
        return funding;
    }

    public TargetQuotaGrantActivation actualPayerGrant() {
        return actualPayerGrant;
    }

    public TargetQuotaMutation fundedAt() {
        return funding.appliedAt();
    }

    public TargetQuotaMutation mutation() {
        return allocations.isEmpty() ? fundedAt() : allocations.getLast().allocatedAt();
    }

    public long revision() {
        return 1L + allocations.size();
    }

    public Reference reference() {
        return definition().reference();
    }

    public TargetQuotaIdentity primaryIdentity() {
        return definition().originalRoot().identity();
    }

    public TargetQuotaIdentity tenantIdentity() {
        return definition().originalRoot().tenantIdentity();
    }

    public TargetQuotaScope scope() {
        return definition().scope();
    }

    public byte[] lineage() {
        return definition().lineage();
    }

    public TargetQuotaAccounting accounting() {
        return definition().accounting();
    }

    public List<Allocation> allocations() {
        return allocations;
    }

    public CapacityVector allocated() {
        return allocated;
    }

    public CapacityVector remaining() {
        return remaining;
    }

    public CapacityVector actualStateCharge() {
        return actualStateCharge;
    }

    public CapacityVector growthReserve() {
        return growthReserve;
    }

    public CapacityVector futureCharge() {
        return remaining.add(growthReserve);
    }

    public TargetQuotaUsage ownContribution() {
        return new TargetQuotaUsage(actualStateCharge.add(futureCharge()), 0, 0, 0, 0);
    }

    public TargetQuotaUsage tenantContribution() {
        return new TargetQuotaUsage(ownContribution().resources(), 0, 0, 0, 0);
    }

    public byte[] key() {
        return reference().key();
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public void requireRoot(TargetQuotaIncarnation root) {
        definition().requireRoot(root);
    }

    public void requireReference(Reference reference) {
        if (!reference().equals(reference)) {
            throw new IllegalArgumentException("parent credit reference changed its immutable funding definition");
        }
    }

    /**
     * Projection only. Source commit/cold fold must reject allocations without the actual matching funded child Scope
     * proof.
     */
    public TargetParentControlCredit allocate(Allocation child) {
        Objects.requireNonNull(child);
        for (var existing : allocations) {
            if (Arrays.equals(existing.scope().scopeId(), child.scope().scopeId())) {
                if (existing.equals(child)) {
                    return this;
                }
                throw new IllegalStateException(
                        "original child Scope allocation cannot be rewritten, renewed or removed");
            }
        }
        if (allocations.size() == definition().maximumAllocations() || !remaining.covers(child.commitment())) {
            throw new IllegalStateException("parent Control credit has no finite remaining child slot/vector");
        }
        child.allocatedAt().requireAfter(mutation());
        final var next = new ArrayList<>(allocations);
        next.add(child);
        return new TargetParentControlCredit(funding, actualPayerGrant, next);
    }

    private long maximumCanonicalBytes() {
        final long fixed = Math.addExact(
                36L,
                Math.addExact(6L + funding.canonicalBytes().length, 6L + actualPayerGrant.canonicalBytes().length));
        return Math.addExact(
                fixed,
                Math.multiplyExact(
                        definition().maximumAllocations(),
                        6L + Allocation.maximumCanonicalBytes(definition().sourceAnchor())));
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, funding.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, actualPayerGrant.canonicalBytes());
            for (var child : allocations) {
                CanonicalProtobuf.bytes(out, 4, child.canonicalBytes());
            }
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 5, digest);
        });
    }

    public static TargetParentControlCredit decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(
                encoded, MAX_CANONICAL_BYTES, MAX_ALLOCATIONS + 4, true, "parent Control credit");
        if (f.size() < 4
                || f.getFirst().number() != 1
                || f.get(1).number() != 2
                || f.get(2).number() != 3
                || QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) {
            throw new IllegalArgumentException("unknown or incomplete parent credit schema");
        }
        final var funding = Control.decode(QueryCodecSupport.bytes(f.get(1), 2));
        int at = 3;
        final var children = new ArrayList<Allocation>();
        while (at < f.size() && f.get(at).number() == 4) {
            if (children.size() == funding.definition().maximumAllocations()) {
                throw new IllegalArgumentException("parent credit child allocation count exceeds original bound");
            }
            children.add(Allocation.decode(QueryCodecSupport.bytes(f.get(at++), 4)));
        }
        if (at + 1 != f.size()) {
            throw new IllegalArgumentException("invalid parent credit suffix");
        }
        final var value = new TargetParentControlCredit(
                funding, TargetQuotaGrantActivation.decode(QueryCodecSupport.bytes(f.get(2), 3)), children);
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(f.get(at), 5, 32))) {
            throw new IllegalArgumentException("parent credit digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "parent Control credit");
        return value;
    }

    public static TargetParentControlCredit decodeForStore(byte[] key, byte[] encoded, ShardId shard, byte[] tenant) {
        final var value = decode(encoded);
        if (!Arrays.equals(key, value.key())
                || !value.scope().shard().equals(shard)
                || !Arrays.equals(value.scope().tenantScope(), tenant)) {
            throw new IllegalArgumentException("parent credit changed its original Store key/Shard/tenant");
        }
        return value;
    }

    public static TargetParentControlCredit decodeForStore(
            byte[] key, byte[] encoded, TargetQuotaScope scope, byte[] lineage) {
        final var value = decodeForStore(key, encoded, scope.shard(), scope.tenantScope());
        if (!value.scope().equals(scope) || !Arrays.equals(value.lineage(), lineage)) {
            throw new IllegalArgumentException("parent credit changed its original Store scope/lineage");
        }
        return value;
    }

    private static CapacityVector requireNativeVector(CapacityVector vector) {
        Objects.requireNonNull(vector);
        new TargetQuotaUsage(vector, 0, 0, 0, 0);
        if (vector.isZero()) {
            throw new IllegalArgumentException("paid parent/child credit must contain a nonzero Native vector");
        }
        for (var dimension : CapacityDimension.values()) {
            if (vector.amount(dimension) == Long.MAX_VALUE) {
                throw new IllegalArgumentException("parent/child Native credit must have finite bounded amounts");
            }
        }
        return vector;
    }

    private static byte[] physicalSourceDigest(SourcePosition source) {
        TargetSourcePosition.requireBounded(source);
        return Bytes.sha256(SOURCE_DOMAIN, CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, source.kind().wireValue());
            CanonicalProtobuf.uint32Bits(out, 2, source.shardId().partition());
            if (source instanceof KafkaSourcePosition kafka) {
                CanonicalProtobuf.bytes(out, 3, Bytes.utf8(kafka.authenticatedClusterId()));
                CanonicalProtobuf.bytes(
                        out,
                        4,
                        Bytes.concat(
                                Bytes.u64beBits(kafka.nativeTopicUuid().getMostSignificantBits()),
                                Bytes.u64beBits(kafka.nativeTopicUuid().getLeastSignificantBits())));
            } else if (source instanceof PulsarSourcePosition pulsar) {
                CanonicalProtobuf.bytes(out, 5, pulsar.brokerResourceIncarnation());
                CanonicalProtobuf.bytes(out, 6, Bytes.utf8(pulsar.physicalTopic()));
            }
        }));
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof TargetParentControlCredit that
                && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
