package com.nereusstream.delay.protocol;

import java.util.Arrays;
import java.util.Objects;

/** State2 retains the exact Source confirmation and frozen fees, without the retired object's raw copy. */
public record TargetEvidenceConfirmedRecord(SystemMutation confirmation, TargetQuotaMutation intentAppliedAt,
        TargetQuotaMutation appliedAt) {
    public static final int VALUE_TYPE = 52;
    private static final int MAX_BODY_BYTES = TargetExternalEvidenceObjectResource.MAX_CANONICAL_BYTES
            + TargetEvidenceRetirementContext.MAX_CANONICAL_BYTES
            + TargetEvidenceDeleteConfirmedBody.MAX_DELETE_EVIDENCE_BYTES
            + TargetEvidenceDeleteConfirmedBody.MAX_TIME_BYTES + 2048;
    public static final int MAX_CONFIRMATION_BYTES = MAX_BODY_BYTES + (1 << 20) + 2048;
    public static final int MAX_CANONICAL_BYTES = MAX_CONFIRMATION_BYTES
            + 2 * TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 256;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-evidence-confirmed-record\0");

    public TargetEvidenceConfirmedRecord {
        Objects.requireNonNull(confirmation); Objects.requireNonNull(intentAppliedAt).requireSourceApplied();
        Objects.requireNonNull(appliedAt).requireSourceApplied();
        if(confirmation.canonicalBodyLength()>MAX_BODY_BYTES)
            throw new IllegalArgumentException("confirmation body exceeds complete bounded projection");
        final var body = TargetEvidenceDeleteConfirmedBody.decode(confirmation.canonicalBody());
        if (confirmation.type() != SystemMutationType.RESOURCE_DELETE_CONFIRMED
                || confirmation.authorIdentity().length > (1 << 20)
                || AuthorIdentity.decode(confirmation.authorIdentity()).kind() != AuthorIdentity.Kind.SERVICE
                || confirmation.canonicalEnvelope().length > MAX_CONFIRMATION_BYTES
                || !confirmation.shardId().equals(body.shard())
                || confirmation.retryUntilEpochMs() != body.retryUntil()
                || !Arrays.equals(confirmation.logicalOperationIdentity(), body.logicalOperationIdentity())
                || !Arrays.equals(appliedAt.mutationDigest(), Bytes.sha256(confirmation.canonicalEnvelope()))
                || appliedAt.source().brokerPersistenceTimeEpochMs() > body.retryUntil()
                || body.confirmedAt().latestEpochMs() > body.retryUntil())
            throw new IllegalArgumentException("confirmed row changed exact applied Source confirmation");
        intentAppliedAt.requireAfter(body.context().preparedCut()); appliedAt.requireAfter(intentAppliedAt);
        if(body.markerRetained()&&!body.markerEvidence().marker().intentAppliedAt().equals(intentAppliedAt))
            throw new IllegalArgumentException("retained marker changed actual Source intent stamp");
    }

    /** Exact state1 replacement facts; this does not authenticate a provider response or authorize a write. */
    public static TargetEvidenceConfirmedRecord fromRetiring(TargetEvidenceRetiringRecord original,
            SystemMutation confirmation, TargetQuotaMutation appliedAt) {
        final var prior = Objects.requireNonNull(original);
        final var next = new TargetEvidenceConfirmedRecord(confirmation, prior.appliedAt(), appliedAt);
        final var body = next.body();
        if (!Arrays.equals(body.intent().mutationId(), prior.intent().systemMutationId())
                || !Arrays.equals(body.intent().mutationHash(), prior.intent().mutationHash())
                || !Arrays.equals(body.resource().canonicalBytes(), prior.copy().resource().canonicalBytes())
                || !Arrays.equals(body.context().canonicalBytes(), prior.body().context().canonicalBytes()))
            throw new IllegalArgumentException("confirmation replaced the original retiring intent/resource/context");
        return next;
    }

    public long stateVersion() { return 2; }
    public TargetEvidenceDeleteConfirmedBody body() { return TargetEvidenceDeleteConfirmedBody.decode(confirmation.canonicalBody()); }
    public TargetQuotaIncarnation feeOwner() { return body().context().feeOwner(); }
    public byte[] key() { return TargetEvidenceRetiringRecord.confirmedKey(body().resource().exactIdentity().identityHash()); }
    public CapacityVector stateCharge() {
        return feeOwner().accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE, key().length, canonicalBytes().length);
    }
    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, body().markerRetained()?2:1); CanonicalProtobuf.uint32(out, 2, 2);
            CanonicalProtobuf.bytes(out, 3, confirmation.canonicalEnvelope());
            CanonicalProtobuf.bytes(out, 4, intentAppliedAt.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, appliedAt.canonicalBytes());
        });
    }
    public byte[] canonicalBytes() {
        final byte[] fields = fields();
        final byte[] encoded = Bytes.concat(fields, CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out, 6, Bytes.sha256(DOMAIN, fields))));
        if (encoded.length > MAX_CANONICAL_BYTES) throw new IllegalArgumentException("confirmed row exceeds finite byte bound");
        return encoded;
    }
    public static TargetEvidenceConfirmedRecord decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 6, false, "Target confirmed record");
        QueryCodecSupport.requireNumbers(f, new int[]{1,2,3,4,5,6}, "Target confirmed record");
        final long version=QueryCodecSupport.uint(f.get(0),1);
        if ((version!=1&&version!=2) || QueryCodecSupport.uint(f.get(1), 2) != 2)
            throw new IllegalArgumentException("unknown Target confirmed schema/state");
        final byte[] frame = QueryCodecSupport.nested(f.get(2), 3);
        if (frame.length > MAX_CONFIRMATION_BYTES) throw new IllegalArgumentException("confirmation exceeds byte bound");
        final var record = new TargetEvidenceConfirmedRecord(SystemMutation.decodeEnvelope(frame),
                TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(3), 4)),
                TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(4), 5)));
        QueryCodecSupport.fixed(f.get(5), 6, 32);
        QueryCodecSupport.requireCanonical(encoded, record.canonicalBytes(), "Target confirmed record"); return record;
    }
    public static TargetEvidenceConfirmedRecord decodeForStore(byte[] key, byte[] encoded, ShardId shard, byte[] tenant, byte[] lineage) {
        final var record = decode(encoded); final var owner = record.feeOwner();
        if (!Arrays.equals(key, record.key()) || !owner.identity().shard().equals(shard)
                || !Arrays.equals(owner.tenantScope(), tenant) || !Arrays.equals(owner.recoveryLineage(), lineage))
            throw new IllegalArgumentException("confirmed key/Shard/tenant/lineage mismatch");
        return record;
    }
}
