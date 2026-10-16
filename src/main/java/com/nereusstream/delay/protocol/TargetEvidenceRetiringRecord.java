package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** State1: immutable Source intent plus a fully charged public offline copy; no delete permission. */
public record TargetEvidenceRetiringRecord(SystemMutation intent,TargetQuotaMutation appliedAt,TargetEvidenceRetirementCopy copy) {
    public static final int VALUE_TYPE = 51;
    public static final int MAX_INTENT_BYTES = TargetEvidenceRetireIntentBody.MAX_CANONICAL_BYTES + (1 << 20) + 1024;
    public static final int MAX_CANONICAL_BYTES = MAX_INTENT_BYTES + TargetEvidenceRetirementCopy.MAX_CANONICAL_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 256;
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-evidence-retiring-record\0");
    public TargetEvidenceRetiringRecord {
        Objects.requireNonNull(intent); Objects.requireNonNull(appliedAt).requireSourceApplied(); Objects.requireNonNull(copy);
        final var body = TargetEvidenceRetireIntentBody.decode(intent.canonicalBody());
        if (intent.type() != SystemMutationType.RESOURCE_RETIRE_INTENT || intent.authorIdentity().length > (1 << 20)
                || intent.canonicalEnvelope().length > MAX_INTENT_BYTES || !intent.shardId().equals(body.shard())
                || intent.retryUntilEpochMs() != body.retryUntil()
                || !Arrays.equals(intent.logicalOperationIdentity(), body.logicalOperationIdentity())
                || !Arrays.equals(body.resource().canonicalBytes(), copy.resource().canonicalBytes())
                || !Arrays.equals(appliedAt.mutationDigest(), Bytes.sha256(intent.canonicalEnvelope())))
            throw new IllegalArgumentException("retiring record changed exact signed Source intent/copy");
        appliedAt.requireAfter(body.context().preparedCut()); copy.requireContext(body.context());
    }
    public long stateVersion() { return 1; }
    public TargetEvidenceRetireIntentBody body() { return TargetEvidenceRetireIntentBody.decode(intent.canonicalBody()); }
    public TargetQuotaIncarnation feeOwner() { return body().context().feeOwner(); }
    public byte[] key() { return key(copy.resource().exactIdentity().identityHash()); }
    public static byte[] key(byte[] identityHash) {
        return Bytes.concat(new byte[]{TargetKeyCodec.EVIDENCE_RETIRING_TAG,TargetKeyCodec.KEY_FORMAT},
                TargetCompatibilityCodec.assigned(identityHash,32,"retiring exact identity"));
    }
    /** Reserved confirmed identity; its presence must never be interpreted as unretired state0. */
    public static byte[] confirmedKey(byte[] identityHash) {
        return Bytes.concat(new byte[]{TargetKeyCodec.EVIDENCE_CONFIRMED_TAG,TargetKeyCodec.KEY_FORMAT},
                TargetCompatibilityCodec.assigned(identityHash,32,"confirmed exact identity"));
    }
    public CapacityVector stateCharge() {
        return feeOwner().accounting().recordCharge(TargetQuotaAccounting.RecordClass.STATE,key().length,canonicalBytes().length);
    }
    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out,1,1); CanonicalProtobuf.uint32(out,2,1);
            CanonicalProtobuf.bytes(out,3,intent.canonicalEnvelope()); CanonicalProtobuf.bytes(out,4,appliedAt.canonicalBytes());
            CanonicalProtobuf.bytes(out,5,copy.canonicalBytes());
        });
    }
    public byte[] canonicalBytes() {
        final byte[] fields = fields();
        final byte[] encoded = Bytes.concat(fields,CanonicalProtobuf.message(out -> CanonicalProtobuf.bytes(out,6,Bytes.sha256(DOMAIN,fields))));
        if (encoded.length > MAX_CANONICAL_BYTES) throw new IllegalArgumentException("retiring row exceeds finite byte bound");
        return encoded;
    }
    public static TargetEvidenceRetiringRecord decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded,MAX_CANONICAL_BYTES,6,false,"Target retiring record");
        QueryCodecSupport.requireNumbers(f,new int[]{1,2,3,4,5,6},"Target retiring record");
        if (QueryCodecSupport.uint(f.get(0),1) != 1 || QueryCodecSupport.uint(f.get(1),2) != 1)
            throw new IllegalArgumentException("unknown Target retiring schema/state");
        final byte[] source = QueryCodecSupport.nested(f.get(2),3);
        if (source.length > MAX_INTENT_BYTES) throw new IllegalArgumentException("retiring intent exceeds byte bound");
        final var record = new TargetEvidenceRetiringRecord(SystemMutation.decodeEnvelope(source),
                TargetQuotaMutation.decode(QueryCodecSupport.nested(f.get(3),4)),
                TargetEvidenceRetirementCopy.decode(QueryCodecSupport.nested(f.get(4),5)));
        QueryCodecSupport.fixed(f.get(5),6,32);
        QueryCodecSupport.requireCanonical(encoded,record.canonicalBytes(),"Target retiring record"); return record;
    }
    public static TargetEvidenceRetiringRecord decodeForStore(byte[] key,byte[] encoded,ShardId shard,byte[] tenant,byte[] lineage) {
        final var record = decode(encoded); final var owner = record.feeOwner();
        if (!Arrays.equals(key,record.key()) || !owner.identity().shard().equals(shard)
                || !Arrays.equals(owner.tenantScope(),tenant) || !Arrays.equals(owner.recoveryLineage(),lineage))
            throw new IllegalArgumentException("retiring key/Shard/tenant/lineage mismatch");
        return record;
    }
}
