package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.TargetResultRecord;
import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Immutable Source-created child commitment; retained DTOs alone do not authorize parent funding or birth. */
public final class TargetScopeControlObligation {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 64;
    public static final int KEY_TAG = 58;
    public static final int MAX_CANONICAL_BYTES = 64 + 6 + Control.MAX_CANONICAL_BYTES
            + 6 + TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES + 6 + Usage.MAX_CANONICAL_BYTES;
    private static final byte[] DEFINITION_DOMAIN = Bytes.utf8("nereus-delay-target-scope-control-obligation-definition\0");
    private static final byte[] RECORD_DOMAIN = Bytes.utf8("nereus-delay-target-scope-control-obligation\0");

    /** Full header/parent/plan definition. The plan has no header digest, so these codec bounds are acyclic. */
    public record Definition(TargetParentControlCredit.Reference parentReference, TargetSloControlScopeHeader header,
            TargetSloScopeNativePlan plan) {
        public static final int MAX_CANONICAL_BYTES = 64 + 6 + TargetParentControlCredit.Reference.MAX_CANONICAL_BYTES
                + 6 + TargetSloControlScopeHeader.MAX_CANONICAL_BYTES + 6 + TargetSloScopeNativePlan.MAX_CANONICAL_BYTES;
        public Definition {
            Objects.requireNonNull(parentReference); Objects.requireNonNull(header); Objects.requireNonNull(plan);
            plan.requireHeader(header);
            // Validate every checked derived amount at definition time, before an input can be signed.
            plan.futureNative(header);
        }
        public TargetSloControlScopeHeader.Reference reference() { return header.reference(); }
        public TargetSloControlScopeHeader.Reference headerReference() { return header.reference(); }
        public TargetQuotaScope scope() { return plan.scope(); }
        public byte[] lineage() { return plan.lineage(); }
        public TargetQuotaIncarnation originalRoot() { return plan.originalRoot(); }
        public TargetQuotaAccounting accounting() { return plan.accounting(); }
        public SourcePosition sourceAnchor() { return plan.sourceAnchor(); }
        public byte[] digest() { return Bytes.sha256(DEFINITION_DOMAIN, fields()); }
        public byte[] allocationGrantDigest() { return digest(); }
        public byte[] key() { return Bytes.concat(new byte[]{KEY_TAG, TargetKeyCodec.KEY_FORMAT}, header.scopeId()); }
        public void requireRoot(TargetQuotaIncarnation root) { plan.requireRoot(root); }
        public void requireSource(SourcePosition source) { plan.requireSource(source); }
        public CapacityVector futureNative() { return plan.futureNative(header); }
        private byte[] fields() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION); CanonicalProtobuf.bytes(out, 2, parentReference.canonicalBytes());
                CanonicalProtobuf.bytes(out, 3, header.canonicalBytes()); CanonicalProtobuf.bytes(out, 4, plan.canonicalBytes());
            });
        }
        public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, 5, digest()); }); }
        public static Definition decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 5, false, "Scope obligation definition");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5}, "Scope obligation definition");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) throw new IllegalArgumentException("unknown Scope obligation definition schema");
            final var value = new Definition(TargetParentControlCredit.Reference.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                    TargetSloControlScopeHeader.decode(QueryCodecSupport.bytes(f.get(2), 3)), TargetSloScopeNativePlan.decode(QueryCodecSupport.bytes(f.get(3), 4)));
            if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.get(4), 5, 32))) throw new IllegalArgumentException("Scope obligation definition digest mismatch");
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Scope obligation definition"); return value;
        }
        @Override public boolean equals(Object other) { return other instanceof Definition that && Arrays.equals(canonicalBytes(), that.canonicalBytes()); }
        @Override public int hashCode() { return Arrays.hashCode(digest()); }
    }

    /** Complete original signed Control and actual Source stamp. Independent first/POS/parent joins remain required. */
    public record Control(SystemMutation input, TargetQuotaMutation appliedAt) {
        public static final int MAX_INPUT_BYTES = TargetScopeControlObligationBody.MAX_INPUT_BYTES;
        public static final int MAX_CANONICAL_BYTES = 16 + MAX_INPUT_BYTES + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;
        public Control {
            Objects.requireNonNull(input); Objects.requireNonNull(appliedAt).requireSourceApplied();
            if (input.type() != SystemMutationType.APPLY_SHARD_CONTROL || input.canonicalEnvelope().length > MAX_INPUT_BYTES)
                throw new IllegalArgumentException("Scope obligation requires its complete bounded original Control");
            final var body = TargetScopeControlObligationBody.decode(input.canonicalBody());
            final var definition = body.request().definition(); definition.requireSource(appliedAt.source());
            if (!Arrays.equals(input.logicalOperationIdentity(), body.logicalIdentity()) || !input.shardId().equals(body.shard())
                    || !input.shardId().equals(appliedAt.source().shardId()) || input.retryUntilEpochMs() != body.retryUntil()
                    || appliedAt.source().brokerPersistenceTimeEpochMs() > input.retryUntilEpochMs()
                    || !Arrays.equals(appliedAt.mutationDigest(), Bytes.sha256(input.canonicalEnvelope()))
                    || AuthorIdentity.decode(input.authorIdentity()).kind() != AuthorIdentity.Kind.CONTROL
                    || appliedAt.source().compareTo(definition.sourceAnchor()) <= 0)
                throw new IllegalArgumentException("Scope obligation changed its original Control/Source/anchor/retry binding");
            appliedAt.requireAfter(definition.originalRoot().allocation());
        }
        public TargetScopeControlObligationBody body() { return TargetScopeControlObligationBody.decode(input.canonicalBody()); }
        public Definition definition() { return body().request().definition(); }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION); CanonicalProtobuf.bytes(out, 2, input.canonicalEnvelope());
                CanonicalProtobuf.bytes(out, 3, appliedAt.canonicalBytes());
            });
        }
        public static Control decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 3, false, "Scope obligation original Control");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3}, "Scope obligation original Control");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) throw new IllegalArgumentException("unknown Scope obligation Control schema");
            final var input = QueryCodecSupport.bytes(f.get(1), 2);
            if (input.length > MAX_INPUT_BYTES) throw new IllegalArgumentException("Scope obligation original Control exceeds bound");
            final var value = new Control(SystemMutation.decodeTargetEnvelope(input, SystemMutationType.APPLY_SHARD_CONTROL),
                    TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Scope obligation original Control"); return value;
        }
    }

    /** Reserved shape for later Source-verified settlement. This package accepts only the original empty usage. */
    public record Usage(CapacityVector consumedNative, long sourceOccurrences, long samples, long writerEpochs,
            long generations, TargetQuotaMutation latestApplied) {
        public static final int MAX_CANONICAL_BYTES = 2 + 6 + TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES
                + 4 * 11 + 6 + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;
        public Usage {
            TargetSloScopeNativePlan.requireFiniteNative(consumedNative); Objects.requireNonNull(latestApplied).requireSourceApplied();
            if (!consumedNative.isZero() || sourceOccurrences != 0 || samples != 0 || writerEpochs != 0 || generations != 0)
                throw new IllegalArgumentException("Scope obligation Source settlement is not enabled; usage must remain empty");
        }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION); CanonicalProtobuf.bytes(out, 2, consumedNative.canonicalBytes());
                CanonicalProtobuf.uint64(out, 3, sourceOccurrences); CanonicalProtobuf.uint64(out, 4, samples);
                CanonicalProtobuf.uint64(out, 5, writerEpochs); CanonicalProtobuf.uint64(out, 6, generations);
                CanonicalProtobuf.bytes(out, 7, latestApplied.canonicalBytes());
            });
        }
        public static long maximumCanonicalBytes(SourcePosition source) {
            return Math.addExact(MAX_CANONICAL_BYTES - (long) TargetSourcePosition.MAX_CANONICAL_BYTES,
                    TargetQuotaBookkeeping.maximumSourceBytes(source));
        }
        public static Usage decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "Scope obligation usage");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5, 6, 7}, "Scope obligation usage");
            if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) throw new IllegalArgumentException("unknown Scope obligation usage schema");
            final var vector = QueryCodecSupport.bytes(f.get(1), 2);
            if (vector.length > TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES) throw new IllegalArgumentException("Scope usage vector exceeds bound");
            final var value = new Usage(CapacityVector.decode(vector), QueryCodecSupport.uint64Bits(f.get(2), 3),
                    QueryCodecSupport.uint64Bits(f.get(3), 4), QueryCodecSupport.uint64Bits(f.get(4), 5),
                    QueryCodecSupport.uint64Bits(f.get(5), 6), TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(6), 7)));
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Scope obligation usage"); return value;
        }
    }

    private final Control funding;
    private final Definition definition;
    private final CapacityVector bootstrapResultsCharge;
    private final Usage usage;
    private final CapacityVector futureNative;
    private final CapacityVector actualStateCharge;
    private final CapacityVector growthReserve;
    private final byte[] digest;

    private TargetScopeControlObligation(Control funding, CapacityVector bootstrapResultsCharge, Usage usage) {
        this.funding = Objects.requireNonNull(funding); definition = funding.definition();
        this.bootstrapResultsCharge = requireBootstrapResultsCharge(bootstrapResultsCharge, definition);
        this.usage = Objects.requireNonNull(usage);
        if (!usage.latestApplied().equals(funding.appliedAt()))
            throw new IllegalArgumentException("unsettled Scope usage must retain its exact original funded Source stamp");
        futureNative = definition.futureNative();
        digest = Bytes.sha256(RECORD_DOMAIN, fields());
        final long actualBytes = canonicalBytes().length;
        if (actualBytes > MAX_CANONICAL_BYTES) throw new IllegalArgumentException("Scope obligation exceeds closed codec bound");
        actualStateCharge = accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE, key().length, actualBytes);
        final long futureUsageBytes = Usage.maximumCanonicalBytes(definition.sourceAnchor());
        final long growthBytes = Math.subtractExact(lengthDelimitedBytes(futureUsageBytes),
                lengthDelimitedBytes(usage.canonicalBytes().length));
        if (growthBytes < 0) throw new IllegalArgumentException("Scope usage exceeds its frozen physical Source growth bound");
        // Only additional typed Usage bytes: no second key, envelope or record overhead is charged.
        growthReserve = accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE, key().length,
                Math.addExact(actualBytes, growthBytes)).subtract(actualStateCharge);
        TargetSloScopeNativePlan.requireFiniteNative(parentCommitment());
    }

    public static TargetScopeControlObligation fund(Definition definition, SystemMutation originalControl,
            TargetQuotaMutation fundedAt, CapacityVector bootstrapResultsCharge) {
        final var control = new Control(originalControl, fundedAt);
        if (!Objects.requireNonNull(definition).equals(control.definition()))
            throw new IllegalArgumentException("Scope funding Control changed its complete original definition");
        return new TargetScopeControlObligation(control, bootstrapResultsCharge,
                new Usage(CapacityVector.empty(), 0, 0, 0, 0, fundedAt));
    }
    public Definition definition() { return definition; }
    public Control funding() { return funding; }
    public TargetQuotaMutation fundedAt() { return funding.appliedAt(); }
    public TargetQuotaMutation mutation() { return usage.latestApplied(); }
    public TargetSloControlScopeHeader.Reference reference() { return definition.reference(); }
    public TargetSloControlScopeHeader.Reference headerReference() { return definition.headerReference(); }
    public TargetParentControlCredit.Reference parentReference() { return definition.parentReference(); }
    public byte[] allocationGrantDigest() { return definition.digest(); }
    public TargetQuotaScope scope() { return definition.scope(); }
    public byte[] lineage() { return definition.lineage(); }
    public TargetQuotaAccounting accounting() { return definition.accounting(); }
    public Usage usage() { return usage; }
    public CapacityVector bootstrapResultsCharge() { return bootstrapResultsCharge; }
    public CapacityVector futureNative() { return futureNative; }
    public CapacityVector actualStateCharge() { return actualStateCharge; }
    public CapacityVector growthReserve() { return growthReserve; }
    public CapacityVector metadataGrowthReserve() { return growthReserve; }
    public CapacityVector futureCharge() { return futureNative.add(growthReserve); }
    public TargetQuotaUsage ownContribution() { return new TargetQuotaUsage(actualStateCharge.add(futureCharge()), 0, 0, 0, 0); }
    public TargetQuotaUsage tenantContribution() { return ownContribution(); }
    /** The allocation Control's own actual first and POS are additional to future N. */
    public CapacityVector parentCommitment() { return ownContribution().resources().add(bootstrapResultsCharge); }
    public byte[] key() { return definition.key(); }
    public byte[] digest() { return Bytes.copy(digest); }
    public void requireRoot(TargetQuotaIncarnation root) { definition.requireRoot(root); }
    public void requireSource(SourcePosition source) { definition.requireSource(source); }
    public void requireReference(TargetSloControlScopeHeader.Reference reference) {
        if (!reference().equals(reference)) throw new IllegalArgumentException("Scope obligation changed its immutable header reference");
    }
    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION); CanonicalProtobuf.bytes(out, 2, funding.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, bootstrapResultsCharge.canonicalBytes()); CanonicalProtobuf.bytes(out, 4, usage.canonicalBytes());
        });
    }
    public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, 5, digest); }); }
    public static TargetScopeControlObligation decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 5, false, "Scope Control obligation");
        QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5}, "Scope Control obligation");
        if (QueryCodecSupport.uint32(f.getFirst(), 1) != VERSION) throw new IllegalArgumentException("unknown Scope Control obligation schema");
        final var bootstrap = QueryCodecSupport.bytes(f.get(2), 3);
        if (bootstrap.length > TargetQuotaUsage.MAX_CAPACITY_VECTOR_BYTES) throw new IllegalArgumentException("Scope bootstrap vector exceeds bound");
        final var value = new TargetScopeControlObligation(Control.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                CapacityVector.decode(bootstrap), Usage.decode(QueryCodecSupport.bytes(f.get(3), 4)));
        if (!Arrays.equals(value.digest, QueryCodecSupport.fixed(f.get(4), 5, 32))) throw new IllegalArgumentException("Scope obligation digest mismatch");
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Scope Control obligation"); return value;
    }
    public static TargetScopeControlObligation decodeForStore(byte[] key, byte[] encoded, ShardId shard, byte[] tenant) {
        final var value = decode(encoded);
        if (!Arrays.equals(key, value.key()) || !value.scope().shard().equals(shard) || !Arrays.equals(value.scope().tenantScope(), tenant))
            throw new IllegalArgumentException("Scope obligation changed its original Store key/Shard/tenant");
        return value;
    }
    public static TargetScopeControlObligation decodeForStore(byte[] key, byte[] encoded, TargetQuotaScope scope, byte[] lineage) {
        final var value = decodeForStore(key, encoded, scope.shard(), scope.tenantScope());
        if (!value.scope().equals(scope) || !Arrays.equals(value.lineage(), lineage))
            throw new IllegalArgumentException("Scope obligation changed its original Store scope/lineage");
        return value;
    }
    private static CapacityVector requireBootstrapResultsCharge(CapacityVector charge, Definition definition) {
        TargetSloScopeNativePlan.requireFiniteNative(charge);
        for (var dimension : CapacityDimension.values()) {
            if (dimension != CapacityDimension.RESULT_BYTES && dimension != CapacityDimension.RESULT_RECORDS
                    && dimension != CapacityDimension.EVIDENCE_BYTES && dimension != CapacityDimension.EVIDENCE_RECORDS
                    && charge.amount(dimension) != 0)
                throw new IllegalArgumentException("Scope bootstrap charge includes a non-result obligation");
        }
        final var ceiling = definition.accounting().recordCharge(TargetQuotaAccounting.RecordClass.RESULT, 34, TargetResultRecord.MAX_CANONICAL_BYTES)
                .add(definition.accounting().recordCharge(TargetQuotaAccounting.RecordClass.EVIDENCE,
                        Math.addExact(2L, TargetQuotaBookkeeping.maximumSourceBytes(definition.sourceAnchor())), TargetResultRecord.MAX_CANONICAL_BYTES));
        if (charge.amount(CapacityDimension.RESULT_RECORDS) != 1 || charge.amount(CapacityDimension.EVIDENCE_RECORDS) != 1
                || charge.amount(CapacityDimension.RESULT_BYTES) == 0 || charge.amount(CapacityDimension.EVIDENCE_BYTES) == 0
                || !ceiling.covers(charge))
            throw new IllegalArgumentException("Scope bootstrap charge must retain exactly its bounded actual first and POS");
        return charge;
    }
    private static long lengthDelimitedBytes(long payloadBytes) {
        if (payloadBytes < 0) throw new IllegalArgumentException("negative Scope usage encoding length");
        long remaining = payloadBytes; int lengthBytes = 1;
        while ((remaining >>>= 7) != 0) lengthBytes++;
        return Math.addExact(Math.addExact(1L, lengthBytes), payloadBytes);
    }
    @Override public boolean equals(Object other) { return other instanceof TargetScopeControlObligation that && Arrays.equals(canonicalBytes(), that.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest); }
}
