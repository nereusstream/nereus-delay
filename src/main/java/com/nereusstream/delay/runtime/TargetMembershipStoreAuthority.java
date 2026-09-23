package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetMembershipClosureRecord;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.Arrays;
import java.util.Objects;

/** Exact membership history from the same bounded Store view as source control or first binding. */
public final class TargetMembershipStoreAuthority {
    private TargetMembershipStoreAuthority() {}

    public static TargetMembershipAuthority.AppliedGrant resolve(
            final TargetStoreBackend.Reader reader,
            final TargetQuotaScope shardScope,
            final byte[] lineage,
            final byte[] grantRef) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(shardScope, "shardScope");
        Bytes.requireLength(lineage, 16, "lineage");
        if (shardScope.target() != null || !shardScope.shard().equals(reader.shardId())) {
            throw new IllegalArgumentException("membership Store authority needs its exact Shard scope");
        }
        final byte[] grantKey = TargetKeyCodec.membershipGrant(grantRef);
        final byte[] closureKey = TargetKeyCodec.membershipClosure(grantRef);
        final byte[] rawGrant = reader.get(ColumnFamily.META, grantKey);
        final byte[] rawClosure = reader.get(ColumnFamily.META, closureKey);
        if (rawGrant == null) {
            if (rawClosure != null) {
                throw new IllegalStateException("membership closure has no durable grant");
            }
            return null;
        }
        final var grant = TargetMembershipGrant.decodeForStore(
                grantKey, TargetValueEnvelope.decode(rawGrant, TargetMembershipGrant.VALUE_TYPE).payload(),
                shardScope.shard());
        final var frontier = reader.source();
        if (!Arrays.equals(grant.tenantScope(), shardScope.tenantScope()) || frontier == null) {
            throw new IllegalStateException("membership grant differs from Store tenant/source frontier");
        }
        final int order = grant.activationSource().compareTo(frontier);
        if (order > 0
                || (order == 0
                        && !Arrays.equals(grant.activationSource().canonicalBytes(), frontier.canonicalBytes()))) {
            throw new IllegalStateException("membership grant is ahead of Store source frontier");
        }
        if (rawClosure == null) {
            return new TargetMembershipAuthority.AppliedGrant(grant, null);
        }
        final var closure = TargetMembershipClosureRecord.decodeForStore(
                closureKey, TargetValueEnvelope.decode(rawClosure, TargetMembershipClosureRecord.VALUE_TYPE).payload(),
                shardScope.shard(), lineage);
        closure.requireGrant(grant);
        closure.mutation().requireAtOrBefore(reader.aggregate().mutation());
        final int closureOrder = closure.closedAt().compareTo(frontier);
        if (closureOrder > 0
                || (closureOrder == 0
                        && !Arrays.equals(closure.closedAt().canonicalBytes(), frontier.canonicalBytes()))) {
            throw new IllegalStateException("membership closure is ahead of Store source frontier");
        }
        return new TargetMembershipAuthority.AppliedGrant(grant, closure.closedAt());
    }
}
