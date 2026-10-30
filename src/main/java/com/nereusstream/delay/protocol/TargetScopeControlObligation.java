package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Immutable Source-created child commitment; retained DTOs alone do not authorize parent funding or birth. */
public final class TargetScopeControlObligation {
    public static final int VERSION = 1;
    public static final int SETTLEMENT_VERSION = 2;
    public static final int EXPOSURE_VERSION = 3;
    public static final int V1_MAX_CANONICAL_BYTES = 5_286_012;
    public static final int V2_MAX_CANONICAL_BYTES = 5_288_071;
    public static final int VALUE_TYPE = 64;
    public static final int KEY_TAG = 58;
    public static final int MAX_CANONICAL_BYTES = 64 + 6 + Control.MAX_CANONICAL_BYTES
            + 3 * (6 + TargetSloScopeNativePlan.V1_VECTOR_BYTES) + 6 + Usage.MAX_CANONICAL_BYTES;
    private static final byte[] DEFINITION_DOMAIN = Bytes.utf8("nereus-delay-target-scope-control-obligation-definition\0");
    private static final byte[] RECORD_DOMAIN = Bytes.utf8("nereus-delay-target-scope-control-obligation\0");

    /** Full header/parent/plan definition. The plan has no header digest, so these codec bounds are acyclic. */
    public record Definition(TargetParentControlCredit.Reference parentReference, TargetSloControlScopeHeader header,
            TargetSloScopeNativePlan plan) {
        public static final int V2_MAX_CANONICAL_BYTES = 3_185_345;
        public static final int MAX_CANONICAL_BYTES = 64 + 6 + TargetParentControlCredit.Reference.MAX_CANONICAL_BYTES
                + 6 + TargetSloControlScopeHeader.MAX_CANONICAL_BYTES + 6 + TargetSloScopeNativePlan.MAX_CANONICAL_BYTES;
        public Definition {
            Objects.requireNonNull(parentReference); Objects.requireNonNull(header); Objects.requireNonNull(plan);
            plan.requireHeader(header);
            // Validate every checked derived amount at definition time, before an input can be signed.
            plan.futureNative(header);
            if (!plan.settlementEnabled() && header.canonicalBytes().length > 1_087_100)
                throw new IllegalArgumentException("legacy Scope header exceeds its unchanged codec bound");
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
        public int schemaVersion() { return plan.schemaVersion() == EXPOSURE_VERSION ? EXPOSURE_VERSION : VERSION; }
        private byte[] fields() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, schemaVersion()); CanonicalProtobuf.bytes(out, 2, parentReference.canonicalBytes());
                CanonicalProtobuf.bytes(out, 3, header.canonicalBytes()); CanonicalProtobuf.bytes(out, 4, plan.canonicalBytes());
            });
        }
        public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, 5, digest()); }); }
        public static Definition decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 5, false, "Scope obligation definition");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5}, "Scope obligation definition");
            final int version = QueryCodecSupport.uint32(f.getFirst(), 1);
            if (version != VERSION && version != EXPOSURE_VERSION) throw new IllegalArgumentException("unknown Scope obligation definition schema");
            final var value = new Definition(TargetParentControlCredit.Reference.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                    TargetSloControlScopeHeader.decode(QueryCodecSupport.bytes(f.get(2), 3)), TargetSloScopeNativePlan.decode(QueryCodecSupport.bytes(f.get(3), 4)));
            if (!value.plan().settlementEnabled() && encoded.length > 3_185_218)
                throw new IllegalArgumentException("legacy Scope definition exceeds its unchanged codec bound");
            if (value.schemaVersion() != version || version == VERSION && encoded.length > V2_MAX_CANONICAL_BYTES)
                throw new IllegalArgumentException("Scope definition changed its original schema or codec bound");
            if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.get(4), 5, 32))) throw new IllegalArgumentException("Scope obligation definition digest mismatch");
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Scope obligation definition"); return value;
        }
        @Override public boolean equals(Object other) { return other instanceof Definition that && Arrays.equals(canonicalBytes(), that.canonicalBytes()); }
        @Override public int hashCode() { return Arrays.hashCode(digest()); }
    }

    /** Complete original signed Control and actual Source stamp. Independent first/POS/parent joins remain required. */
    public record Control(SystemMutation input, TargetQuotaMutation appliedAt) {
        public static final int V2_MAX_INPUT_BYTES = 3_186_625;
        public static final int V2_MAX_CANONICAL_BYTES = 4_235_360;
        public static final int MAX_INPUT_BYTES = TargetScopeControlObligationBody.MAX_INPUT_BYTES;
        public static final int MAX_CANONICAL_BYTES = 16 + MAX_INPUT_BYTES + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES;
        public Control {
            Objects.requireNonNull(input); Objects.requireNonNull(appliedAt).requireSourceApplied();
            if (input.type() != SystemMutationType.APPLY_SHARD_CONTROL || input.canonicalEnvelope().length > MAX_INPUT_BYTES)
                throw new IllegalArgumentException("Scope obligation requires its complete bounded original Control");
            final var body = TargetScopeControlObligationBody.decode(input.canonicalBody());
            final var definition = body.request().definition(); definition.requireSource(appliedAt.source());
            if (body.schemaVersion() != definition.plan().schemaVersion()
                    || !definition.plan().settlementEnabled() && input.canonicalEnvelope().length > 3_186_498
                    || body.schemaVersion() == SETTLEMENT_VERSION && input.canonicalEnvelope().length > V2_MAX_INPUT_BYTES
                    || !Arrays.equals(input.logicalOperationIdentity(), body.logicalIdentity()) || !input.shardId().equals(body.shard())
                    || !input.shardId().equals(appliedAt.source().shardId()) || input.retryUntilEpochMs() != body.retryUntil()
                    || appliedAt.source().brokerPersistenceTimeEpochMs() > input.retryUntilEpochMs()
                    || !Arrays.equals(appliedAt.mutationDigest(), Bytes.sha256(input.canonicalEnvelope()))
                    || AuthorIdentity.decode(input.authorIdentity()).kind() != AuthorIdentity.Kind.CONTROL
                    || appliedAt.source().compareTo(definition.sourceAnchor()) <= 0)
                throw new IllegalArgumentException("Scope obligation changed its original Control/Source/anchor/retry binding");
            appliedAt.requireAfter(definition.originalRoot().allocation());
        }
        public TargetScopeControlObligationBody body() { return TargetScopeControlObligationBody.decode(input.canonicalBody()); }
        public int schemaVersion() { return body().schemaVersion(); }
        public ProtocolTuple tuple() { return body().tuple(); }
        public Definition definition() { return body().request().definition(); }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, schemaVersion() == EXPOSURE_VERSION ? EXPOSURE_VERSION : VERSION);
                CanonicalProtobuf.bytes(out, 2, input.canonicalEnvelope());
                CanonicalProtobuf.bytes(out, 3, appliedAt.canonicalBytes());
            });
        }
        public static Control decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 3, false, "Scope obligation original Control");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3}, "Scope obligation original Control");
            final int version = QueryCodecSupport.uint32(f.getFirst(), 1);
            if (version != VERSION && version != EXPOSURE_VERSION) throw new IllegalArgumentException("unknown Scope obligation Control schema");
            final var input = QueryCodecSupport.bytes(f.get(1), 2);
            if (input.length > MAX_INPUT_BYTES) throw new IllegalArgumentException("Scope obligation original Control exceeds bound");
            final var value = new Control(SystemMutation.decodeTargetEnvelope(input, SystemMutationType.APPLY_SHARD_CONTROL),
                    TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(2), 3)));
            if ((value.schemaVersion() == EXPOSURE_VERSION ? EXPOSURE_VERSION : VERSION) != version
                    || version == VERSION && encoded.length > V2_MAX_CANONICAL_BYTES)
                throw new IllegalArgumentException("Scope obligation Control changed its original schema or codec bound");
            QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "Scope obligation original Control"); return value;
        }
    }

    /** Schema 1 stays zero-only; schemas 2/3 project independently verified retained Native facts. */
    public record Usage(int schemaVersion, CapacityVector consumedNative, long sourceOccurrences, long samples, long writerEpochs,
            long generations, TargetQuotaMutation latestApplied) {
        public static final int MAX_CANONICAL_BYTES = 1_049_737;
        public Usage(CapacityVector consumedNative, long sourceOccurrences, long samples, long writerEpochs,
                long generations, TargetQuotaMutation latestApplied) {
            this(VERSION, consumedNative, sourceOccurrences, samples, writerEpochs, generations, latestApplied);
        }
        public Usage {
            TargetSloScopeNativePlan.requireFiniteNative(consumedNative); Objects.requireNonNull(latestApplied).requireSourceApplied();
            if (schemaVersion != VERSION && schemaVersion != SETTLEMENT_VERSION && schemaVersion != EXPOSURE_VERSION
                    || consumedNative.canonicalBytes().length > TargetSloScopeNativePlan.V1_VECTOR_BYTES
                    || latestApplied.source().canonicalBytes().length > TargetSloScopeNativePlan.V1_SOURCE_BYTES
                    || sourceOccurrences < 0 || sourceOccurrences == Long.MAX_VALUE || samples < 0 || samples == Long.MAX_VALUE
                    || writerEpochs < 0 || writerEpochs == Long.MAX_VALUE || generations < 0 || generations == Long.MAX_VALUE
                    || schemaVersion == VERSION && (!consumedNative.isZero() || sourceOccurrences != 0 || samples != 0 || writerEpochs != 0 || generations != 0))
                throw new IllegalArgumentException("unknown/nonfinite Scope usage or nonempty legacy settlement");
        }
        public CapacityVector convertedNative() { return consumedNative; }
        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, schemaVersion); CanonicalProtobuf.bytes(out, 2, consumedNative.canonicalBytes());
                CanonicalProtobuf.uint64(out, 3, sourceOccurrences); CanonicalProtobuf.uint64(out, 4, samples);
                CanonicalProtobuf.uint64(out, 5, writerEpochs); CanonicalProtobuf.uint64(out, 6, generations);
                CanonicalProtobuf.bytes(out, 7, latestApplied.canonicalBytes());
            });
        }
        public static long maximumCanonicalBytes(SourcePosition source) {
            return TargetSloScopeNativePlan.usageMaximum(TargetSloScopeNativePlan.legacyMaximumSourceBytes(source), TargetSloScopeNativePlan.V1_VECTOR_BYTES);
        }
        public static Usage decode(byte[] encoded) {
            final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "Scope obligation usage");
            QueryCodecSupport.requireNumbers(f, new int[]{1, 2, 3, 4, 5, 6, 7}, "Scope obligation usage");
            final int version = QueryCodecSupport.uint32(f.getFirst(), 1);
            final var vector = QueryCodecSupport.bytes(f.get(1), 2);
            if (vector.length > TargetSloScopeNativePlan.V1_VECTOR_BYTES) throw new IllegalArgumentException("Scope usage vector exceeds bound");
            final var value = new Usage(version, CapacityVector.decode(vector), QueryCodecSupport.uint64Bits(f.get(2), 3),
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
    private final CapacityVector metadataCeiling;
    private final CapacityVector actualStateCharge;
    private final CapacityVector growthReserve;
    private final byte[] digest;

    private TargetScopeControlObligation(Control funding, CapacityVector bootstrapResultsCharge, Usage usage,
            CapacityVector storedFutureNative, CapacityVector storedMetadataCeiling) {
        this.funding = Objects.requireNonNull(funding); definition = funding.definition();
        this.bootstrapResultsCharge = requireBootstrapResultsCharge(bootstrapResultsCharge, definition);
        this.usage = Objects.requireNonNull(usage);
        if (usage.schemaVersion() != schemaVersion()) throw new IllegalArgumentException("Scope outer/Usage schemas disagree");
        definition.requireSource(usage.latestApplied().source());
        if (!settlementEnabled()) {
            if (!usage.latestApplied().equals(funding.appliedAt()) || storedFutureNative != null || storedMetadataCeiling != null)
                throw new IllegalArgumentException("legacy Scope must retain its original zero-only funding shape");
        } else {
            funding.appliedAt().requireAtOrBefore(usage.latestApplied());
            definition.plan().requireConvertedNative(definition.header(), usage.convertedNative(), usage.sourceOccurrences(),
                    usage.samples(), usage.writerEpochs(), usage.generations());
            if (usage.latestApplied().equals(funding.appliedAt()) && (!usage.convertedNative().isZero()
                    || usage.sourceOccurrences() != 0 || usage.samples() != 0 || usage.writerEpochs() != 0 || usage.generations() != 0))
                throw new IllegalArgumentException("Scope funding cannot prewrite successful settlement");
        }
        futureNative = definition.futureNative();
        if (storedFutureNative != null && !futureNative.equals(storedFutureNative))
            throw new IllegalArgumentException("Scope changed its original frozen future vector");
        if (!futureNative.covers(usage.convertedNative())) throw new IllegalArgumentException("Scope converted more than its original F0");
        metadataCeiling = originalMetadataCeiling();
        if (storedMetadataCeiling != null && !metadataCeiling.equals(storedMetadataCeiling))
            throw new IllegalArgumentException("Scope changed its original metadata ceiling");
        digest = Bytes.sha256(RECORD_DOMAIN, fields());
        final long actualBytes = canonicalBytes().length;
        if (actualBytes > maximumCanonicalBytes(schemaVersion())
                || usage.canonicalBytes().length > definition.plan().maximumUsageBytes())
            throw new IllegalArgumentException("Scope metadata exceeds its original codec/Usage shape");
        actualStateCharge = definition.plan().metadataCharge(key().length, actualBytes);
        if (!metadataCeiling.covers(actualStateCharge)) throw new IllegalArgumentException("Scope metadata exhausted its fixed M0");
        growthReserve = metadataCeiling.subtract(actualStateCharge);
        TargetSloScopeNativePlan.requireFiniteNative(parentCommitment());
    }

    /** The M0 field uses its known maximum encoding width, avoiding a size/value recursion. */
    private CapacityVector originalMetadataCeiling() {
        long payload = Math.addExact(36L, Math.addExact(lengthDelimitedBytes(funding.canonicalBytes().length),
                Math.addExact(lengthDelimitedBytes(bootstrapResultsCharge.canonicalBytes().length),
                        lengthDelimitedBytes(definition.plan().maximumUsageBytes()))));
        if (settlementEnabled()) payload = Math.addExact(payload, Math.addExact(lengthDelimitedBytes(futureNative.canonicalBytes().length),
                lengthDelimitedBytes(definition.plan().maximumVectorBytes())));
        return TargetSloScopeNativePlan.requireFiniteNative(definition.plan().metadataCharge(key().length, payload));
    }

    public static TargetScopeControlObligation fund(Definition definition, SystemMutation originalControl,
            TargetQuotaMutation fundedAt, CapacityVector bootstrapResultsCharge) {
        final var control = new Control(originalControl, fundedAt);
        if (!Objects.requireNonNull(definition).equals(control.definition()))
            throw new IllegalArgumentException("Scope funding Control changed its complete original definition");
        return new TargetScopeControlObligation(control, bootstrapResultsCharge,
                new Usage(definition.plan().schemaVersion(), CapacityVector.empty(), 0, 0, 0, 0, fundedAt), null, null);
    }
    public int schemaVersion() { return definition.plan().schemaVersion(); }
    public boolean settlementEnabled() { return definition.plan().settlementEnabled(); }
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
    public CapacityVector frozenFutureNative() { return futureNative; }
    public CapacityVector remainingFuture() { return futureNative.subtract(usage.convertedNative()); }
    public CapacityVector metadataCeiling() { return metadataCeiling; }
    public CapacityVector actualStateCharge() { return actualStateCharge; }
    public CapacityVector growthReserve() { return growthReserve; }
    public CapacityVector metadataGrowthReserve() { return growthReserve; }
    public CapacityVector futureCharge() { return remainingFuture().add(growthReserve); }
    public TargetQuotaUsage ownContribution() { return new TargetQuotaUsage(metadataCeiling.add(remainingFuture()), 0, 0, 0, 0); }
    public TargetQuotaUsage tenantContribution() { return ownContribution(); }
    /** The allocation Control's own actual first and POS are additional to future N. */
    /** Original payment stays fixed when future capacity later becomes actual retained Native records. */
    public CapacityVector parentCommitment() {
        return metadataCeiling.add(futureNative).add(bootstrapResultsCharge);
    }
    /** Pure projection: independent hot/cold Source facts must authorize every retained-row transfer. */
    public TargetScopeControlObligation settled(CapacityVector convertedNative, long addedOccurrences, long addedSamples,
            long addedWriterEpochs, long addedGenerations, TargetQuotaMutation actual) {
        if (!settlementEnabled()) throw new IllegalStateException("legacy Scope does not support settlement");
        Objects.requireNonNull(actual).requireSourceApplied(); definition.requireSource(actual.source()); actual.requireAfter(mutation());
        if (addedOccurrences < 0 || addedSamples < 0 || addedWriterEpochs < 0 || addedGenerations < 0)
            throw new IllegalArgumentException("Scope lifetime counters cannot decrease");
        final var next = new Usage(schemaVersion(), convertedNative,
                Math.addExact(usage.sourceOccurrences(), addedOccurrences), Math.addExact(usage.samples(), addedSamples),
                Math.addExact(usage.writerEpochs(), addedWriterEpochs), Math.addExact(usage.generations(), addedGenerations), actual);
        return new TargetScopeControlObligation(funding, bootstrapResultsCharge, next, futureNative, metadataCeiling);
    }
    public byte[] key() { return definition.key(); }
    public byte[] digest() { return Bytes.copy(digest); }
    public void requireRoot(TargetQuotaIncarnation root) { definition.requireRoot(root); }
    public void requireSource(SourcePosition source) { definition.requireSource(source); }
    public void requireReference(TargetSloControlScopeHeader.Reference reference) {
        if (!reference().equals(reference)) throw new IllegalArgumentException("Scope obligation changed its immutable header reference");
    }
    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, schemaVersion()); CanonicalProtobuf.bytes(out, 2, funding.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, bootstrapResultsCharge.canonicalBytes()); CanonicalProtobuf.bytes(out, 4, usage.canonicalBytes());
            if (settlementEnabled()) {
                CanonicalProtobuf.bytes(out, 5, futureNative.canonicalBytes()); CanonicalProtobuf.bytes(out, 6, metadataCeiling.canonicalBytes());
            }
        });
    }
    public byte[] canonicalBytes() { return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, settlementEnabled() ? 7 : 5, digest); }); }
    public static TargetScopeControlObligation decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "Scope Control obligation");
        if (f.isEmpty()) throw new IllegalArgumentException("missing Scope obligation version");
        final int version = QueryCodecSupport.uint32(f.getFirst(), 1);
        QueryCodecSupport.requireNumbers(f, version == VERSION ? new int[]{1,2,3,4,5} : new int[]{1,2,3,4,5,6,7}, "Scope Control obligation");
        if (encoded.length > maximumCanonicalBytes(version)) throw new IllegalArgumentException("oversized Scope obligation schema");
        final var bootstrap = QueryCodecSupport.bytes(f.get(2), 3);
        if (bootstrap.length > TargetSloScopeNativePlan.V1_VECTOR_BYTES) throw new IllegalArgumentException("Scope bootstrap vector exceeds bound");
        final var value = new TargetScopeControlObligation(Control.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                CapacityVector.decode(bootstrap), Usage.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                version == VERSION ? null : decodeFrozenVector(QueryCodecSupport.bytes(f.get(4), 5)),
                version == VERSION ? null : decodeFrozenVector(QueryCodecSupport.bytes(f.get(5), 6)));
        if (value.schemaVersion() != version || !Arrays.equals(value.digest, QueryCodecSupport.fixed(f.getLast(), version == VERSION ? 5 : 7, 32)))
            throw new IllegalArgumentException("Scope obligation version/digest mismatch");
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
        final var ceiling = definition.plan().recordCeiling(TargetSloScopeNativePlan.NativeClass.FIRST)
                .add(definition.plan().recordCeiling(TargetSloScopeNativePlan.NativeClass.POSITION));
        if (charge.amount(CapacityDimension.RESULT_RECORDS) != 1 || charge.amount(CapacityDimension.EVIDENCE_RECORDS) != 1
                || charge.amount(CapacityDimension.RESULT_BYTES) == 0 || charge.amount(CapacityDimension.EVIDENCE_BYTES) == 0
                || !ceiling.covers(charge))
            throw new IllegalArgumentException("Scope bootstrap charge must retain exactly its bounded actual first and POS");
        return charge;
    }
    private static CapacityVector decodeFrozenVector(byte[] encoded) {
        if (encoded.length > TargetSloScopeNativePlan.V1_VECTOR_BYTES) throw new IllegalArgumentException("frozen Scope vector exceeds numeric format bound");
        return TargetSloScopeNativePlan.requireFiniteNative(CapacityVector.decode(encoded));
    }
    private static int maximumCanonicalBytes(int version) { return switch (version) {
        case VERSION -> V1_MAX_CANONICAL_BYTES; case SETTLEMENT_VERSION -> V2_MAX_CANONICAL_BYTES; case EXPOSURE_VERSION -> MAX_CANONICAL_BYTES;
        default -> throw new IllegalArgumentException("unknown Scope obligation schema");
    }; }
    private static long lengthDelimitedBytes(long payloadBytes) {
        if (payloadBytes < 0) throw new IllegalArgumentException("negative Scope usage encoding length");
        long remaining = payloadBytes; int lengthBytes = 1;
        while ((remaining >>>= 7) != 0) lengthBytes++;
        return Math.addExact(Math.addExact(1L, lengthBytes), payloadBytes);
    }
    @Override public boolean equals(Object other) { return other instanceof TargetScopeControlObligation that && Arrays.equals(canonicalBytes(), that.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest); }
}
