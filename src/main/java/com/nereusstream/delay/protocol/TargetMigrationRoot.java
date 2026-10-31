package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.Objects;

/** Native migration anchor. PREPARED is never normal restore/READY; only its actual signed Source A advances it. */
public record TargetMigrationRoot(TargetMigrationPlan plan, TargetQuotaIncarnation owner,
        SystemMutation activation, TargetQuotaMutation activatedAt) {
    public static final int VALUE_TYPE = 65;
    public static final int V1_MAX_CANONICAL_BYTES = TargetMigrationPlan.MAX_CANONICAL_BYTES
            + TargetQuotaIncarnation.MAX_CANONICAL_BYTES + TargetMigrationControlBody.LEGACY_MAX_INPUT_BYTES
            + TargetQuotaMutation.MAX_SOURCE_CANONICAL_BYTES + 64;
    public static final int MAX_CANONICAL_BYTES=V1_MAX_CANONICAL_BYTES+TargetMigrationControlBody.ACTIVATE_V2_MAX_INPUT_BYTES-TargetMigrationControlBody.LEGACY_MAX_INPUT_BYTES;
    private static final byte[] V2_DOMAIN=Bytes.utf8("nereus-delay-target-migration-root-v2\0");
    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-target-migration-root-v1\0");
    public enum State { MIGRATION_PREPARED, ACTIVATED }

    public TargetMigrationRoot {
        Objects.requireNonNull(plan); Objects.requireNonNull(owner);
        if ((activation == null) != (activatedAt == null) || owner.identity().kind() != TargetQuotaIdentity.Kind.SHARD
                || !owner.scope().equals(plan.scope()) || !Arrays.equals(owner.recoveryLineage(), plan.newLineage())
                || !Arrays.equals(owner.allocation().source().canonicalBytes(), plan.baseline().source().canonicalBytes())
                || owner.allocation().sequence() != plan.baseline().sequence()
                || !Arrays.equals(owner.allocation().mutationDigest(), plan.digest())) {
            throw new IllegalArgumentException("migration root owner is not its explicit imported baseline allocation");
        }
        if (activation != null) {
            final var body = TargetMigrationControlBody.decode(activation.canonicalBody()); body.requireInput(activation);
            if (!(body.request() instanceof TargetMigrationActivateRequest request) || !plan.equals(request.plan())) {
                throw new IllegalArgumentException("migration root changed exact sealed ACTIVATE plan");
            }
            activatedAt.requireAfter(plan.baseline());
            if (!Arrays.equals(activatedAt.mutationDigest(), Bytes.sha256(activation.canonicalEnvelope()))) {
                throw new IllegalArgumentException("migration root actual Source A differs from original signed input");
            }
        }
    }
    public static TargetMigrationRoot prepared(TargetMigrationPlan plan, TargetQuotaIncarnation owner) {
        return new TargetMigrationRoot(plan, owner, null, null);
    }
    /** Explicit import accounting stamp at C; never represents an invented old Source frame. */
    public static TargetQuotaMutation baselineFor(TargetMigrationPlan plan) {
        Objects.requireNonNull(plan);
        return new TargetQuotaMutation(plan.baseline().sequence(), plan.baseline().source(), plan.digest());
    }
    public TargetMigrationRoot activate(SystemMutation input, TargetQuotaMutation at) {
        if (activation != null) {
            if (!Arrays.equals(activation.canonicalEnvelope(), input.canonicalEnvelope()) || !activatedAt.equals(at)) {
                throw new IllegalStateException("activated migration is roll-forward only; exact A cannot change");
            }
            return this;
        }
        return new TargetMigrationRoot(plan, owner, Objects.requireNonNull(input), Objects.requireNonNull(at));
    }
    public TargetMigrationRootIssuerAuthorization rootIssuerAuthorization(){return activated()?((TargetMigrationActivateRequest)TargetMigrationControlBody.decode(activation.canonicalBody()).request()).rootIssuerAuthorization():null;}
    public int schemaVersion(){return rootIssuerAuthorization()==null?1:2;}
    public State state() { return activation == null ? State.MIGRATION_PREPARED : State.ACTIVATED; }
    public boolean activated() { return activation != null; }
    public TargetQuotaMutation mutation() { return activation == null ? owner.allocation() : activatedAt; }
    public static byte[] keyFor(ShardId shard) {
        return Bytes.concat(new byte[] {TargetKeyCodec.MIGRATION_ROOT_TAG, TargetKeyCodec.KEY_FORMAT}, TargetQuotaIdentity.shardBytes(shard));
    }
    public byte[] key() { return keyFor(plan.scope().shard()); }
    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, schemaVersion()); CanonicalProtobuf.uint32(out, 2, activated() ? 2 : 1);
            CanonicalProtobuf.bytes(out, 3, plan.canonicalBytes()); CanonicalProtobuf.bytes(out, 4, owner.canonicalBytes());
            if (activated()) { CanonicalProtobuf.bytes(out, 5, activation.canonicalEnvelope()); CanonicalProtobuf.bytes(out, 6, activatedAt.canonicalBytes()); }
        });
    }
    public byte[] digest() { return Bytes.sha256(schemaVersion()==1?DOMAIN:V2_DOMAIN, fields()); }
    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> { out.writeBytes(fields()); CanonicalProtobuf.bytes(out, 7, digest()); });
    }
    public static TargetMigrationRoot decode(byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 7, false, "migration root");
        final boolean active = f.size() == 7;
        QueryCodecSupport.requireNumbers(f, active ? new int[] {1,2,3,4,5,6,7} : new int[] {1,2,3,4,7}, "migration root");
        if ((QueryCodecSupport.uint32(f.getFirst(), 1)<1||QueryCodecSupport.uint32(f.getFirst(), 1)>2) || QueryCodecSupport.uint32(f.get(1), 2) != (active ? 2 : 1)) {
            throw new IllegalArgumentException("unknown migration root schema/state");
        }
        final var value = new TargetMigrationRoot(TargetMigrationPlan.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                TargetQuotaIncarnation.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                active ? SystemMutation.decodeTargetEnvelope(QueryCodecSupport.bytes(f.get(4), 5), SystemMutationType.APPLY_SHARD_CONTROL) : null,
                active ? TargetQuotaMutation.decode(QueryCodecSupport.bytes(f.get(5), 6)) : null);
        if(QueryCodecSupport.uint32(f.getFirst(),1)!=value.schemaVersion()||value.schemaVersion()==1&&encoded.length>V1_MAX_CANONICAL_BYTES)throw new IllegalArgumentException("migration Root schema/legacy bound differs from actual A");
        if (!Arrays.equals(value.digest(), QueryCodecSupport.fixed(f.getLast(), 7, 32))) throw new IllegalArgumentException("migration root digest changed");
        QueryCodecSupport.requireCanonical(encoded, value.canonicalBytes(), "migration root"); return value;
    }
    @Override public boolean equals(Object other) { return other instanceof TargetMigrationRoot value && Arrays.equals(canonicalBytes(), value.canonicalBytes()); }
    @Override public int hashCode() { return Arrays.hashCode(digest()); }
}
