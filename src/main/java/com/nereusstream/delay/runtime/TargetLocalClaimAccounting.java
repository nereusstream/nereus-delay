package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaClaimCharge;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Reversible local Claim accounting uses actual record differences and never advances the source frontier. */
public final class TargetLocalClaimAccounting implements TargetMessageStore.AccountingAssembler {
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final byte[] digest;
    private final TargetQuotaDelta.LocalClaimKind kind;
    private final TargetQuotaDelta.LocalClaimAuthority authority;
    private final int maximumDomains;

    public TargetLocalClaimAccounting(
            final TargetQuotaScope scope,
            final byte[] lineage,
            final byte[] operationDigest,
            final TargetQuotaDelta.LocalClaimKind kind,
            final TargetQuotaDelta.LocalClaimAuthority authority,
            final int maximumDomains) {
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        Bytes.requireLength(operationDigest, 32, "operationDigest");
        this.lineage = Bytes.copy(lineage);
        digest = Bytes.copy(operationDigest);
        this.kind = Objects.requireNonNull(kind, "kind");
        this.authority = Objects.requireNonNull(authority, "localClaimAuthority");
        if (scope.target() != null || maximumDomains < 1 || maximumDomains > 64) {
            throw new IllegalArgumentException("local Claim accounting requires bounded Shard scope");
        }
        this.maximumDomains = maximumDomains;
    }

    @Override
    public TargetStoreBackend.Mutation assemble(
            final TargetStoreBackend.Reader reader, final List<TargetStoreBackend.Edit> business) {
        if (!scope.shard().equals(reader.shardId())
                || business.size() > reader.maximumWriteRecords()
                || reader.sourceSequence() == 0
                || reader.source() == null) {
            throw new IllegalStateException("local Claim requires an existing bounded source view");
        }
        final var aggregate = reader.aggregate();
        final long ordinal =
                aggregate.mutation() != null && aggregate.mutation().sequence() == reader.sourceSequence()
                        ? TargetQuotaMutation.increment(aggregate.mutation().localClaimOrdinal())
                        : 1;
        final var operation = new TargetQuotaMutation(reader.sourceSequence(), reader.source(), digest, ordinal);
        final var rootId = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD, scope.shard(), aggregate.accountingIncarnation(), null, null);
        final byte[] rootKey = aggregate.key();
        rootKey[0] = TargetKeyCodec.QUOTA_BOOKKEEPING_TAG;
        final byte[] rootRaw = reader.get(ColumnFamily.META, rootKey);
        if (rootRaw == null) {
            throw new IllegalStateException("local Claim lacks bookkeeping root");
        }
        final var root = TargetQuotaBookkeeping.decodeForStore(
                rootKey,
                TargetValueEnvelope.decode(rootRaw, TargetQuotaBookkeeping.VALUE_TYPE)
                        .payload(),
                rootId,
                scope.tenantScope());
        root.requireRoot(aggregate);
        final var beforeView =
                new TargetRecordAccounting(reader, List.of(), scope, lineage, operation, root, maximumDomains);
        final var afterView =
                new TargetRecordAccounting(reader, business, scope, lineage, operation, root, maximumDomains);
        final Map<TargetQuotaIdentity, TargetQuotaUsage> removed = new LinkedHashMap<>();
        final Map<TargetQuotaIdentity, TargetQuotaUsage> added = new LinkedHashMap<>();
        final var keys = new LinkedHashSet<String>();
        int claimRecords = 0;
        int businessClaims = 0;
        for (var edit : business) {
            if (!keys.add(edit.family() + ":" + Bytes.hex(edit.key()))
                    || !Arrays.equals(edit.before(), reader.get(edit.family(), edit.key()))) {
                throw new IllegalStateException("local Claim contains duplicate or changed before record");
            }
            final int type = TargetValueEnvelope.decodeAny(edit.after() == null ? edit.before() : edit.after())
                    .valueType();
            if (type != TargetClaimRecord.VALUE_TYPE
                    && type != TargetMessageRecord.VALUE_TYPE
                    && type != TargetTimelineWorkRef.VALUE_TYPE
                    && type != TargetExpiryRef.VALUE_TYPE
                    && type != com.nereusstream.delay.protocol.TargetQueueState.VALUE_TYPE
                    && type != TargetOrderState.VALUE_TYPE
                    && type != TargetQuotaClaimCharge.VALUE_TYPE) {
                throw new IllegalArgumentException("local Claim cannot allocate/reprice unrelated record families");
            }
            if (type == TargetClaimRecord.VALUE_TYPE) {
                businessClaims++;
                if ((kind == TargetQuotaDelta.LocalClaimKind.CLAIM && (edit.before() != null || edit.after() == null))
                        || (kind == TargetQuotaDelta.LocalClaimKind.REVOKE
                                && (edit.before() == null || edit.after() != null))) {
                    throw new IllegalStateException("local operation must create/delete its complete business Claim");
                }
            }
            if (type == TargetQuotaClaimCharge.VALUE_TYPE) {
                claimRecords++;
                if (kind == TargetQuotaDelta.LocalClaimKind.CLAIM) {
                    if (edit.before() != null || edit.after() == null) {
                        throw new IllegalStateException("local Claim must create an absent immutable charge");
                    }
                    final var charge = TargetQuotaClaimCharge.decode(
                            TargetValueEnvelope.decode(edit.after(), type).payload());
                    if (!charge.creation().equals(operation)) {
                        throw new IllegalStateException("new Claim charge uses another local operation stamp");
                    }
                } else {
                    if (edit.before() == null || edit.after() != null) {
                        throw new IllegalStateException("local revoke must consume its exact original charge");
                    }
                    final var charge = TargetQuotaClaimCharge.decode(
                            TargetValueEnvelope.decode(edit.before(), type).payload());
                    operation.requireStoreSuccessorOf(charge.creation());
                }
            }
            final var before =
                    edit.before() == null ? null : beforeView.charge(edit.family(), edit.key(), edit.before());
            final var after = edit.after() == null ? null : afterView.charge(edit.family(), edit.key(), edit.after());
            if (before != null
                    && after != null
                    && (!before.owner().identity().equals(after.owner().identity())
                            || !before.owner().accounting().equals(after.owner().accounting()))) {
                throw new IllegalStateException("local Claim changed a frozen record owner");
            }
            accumulate(removed, before);
            accumulate(added, after);
        }
        if (claimRecords != 1 || businessClaims != 1) {
            throw new IllegalStateException("one local operation requires exactly one business Claim and its charge");
        }
        final var identities = new LinkedHashSet<>(removed.keySet());
        identities.addAll(added.keySet());
        if (identities.size() > TargetQuotaDelta.MAX_LOCAL_CLAIM_COUNTERS) {
            throw new IllegalArgumentException("local Claim exceeded execution/payload counter pairs");
        }
        final var updates = new ArrayList<TargetQuotaDelta.Update>();
        for (var identity : identities) {
            final var prior = reader.counter(identity);
            if (prior == null) {
                throw new IllegalStateException("local Claim cannot allocate a quota counter");
            }
            updates.add(new TargetQuotaDelta.Update(
                    identity,
                    prior.usage()
                            .subtract(removed.getOrDefault(identity, TargetQuotaUsage.empty()))
                            .add(added.getOrDefault(identity, TargetQuotaUsage.empty()))));
        }
        final var delta = TargetQuotaDelta.prepareLocalClaim(
                aggregate,
                reader.sourceSequence(),
                reader.source(),
                digest,
                kind,
                updates,
                TargetQuotaDelta.MAX_LOCAL_CLAIM_COUNTERS,
                reader::counter,
                authority);
        final var totals = TargetQuotaTotalsDelta.prepare(delta, scope, 1, reader::total);
        return new TargetStoreBackend.Mutation(totals, business);
    }

    private static void accumulate(
            final Map<TargetQuotaIdentity, TargetQuotaUsage> values, final TargetRecordAccounting.Charge charge) {
        if (charge == null) {
            return;
        }
        values.merge(charge.owner().identity(), charge.primary(), TargetQuotaUsage::add);
        values.merge(charge.owner().tenantIdentity(), charge.mirror(), TargetQuotaUsage::add);
    }
}
