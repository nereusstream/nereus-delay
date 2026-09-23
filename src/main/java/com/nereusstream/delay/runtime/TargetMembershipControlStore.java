package com.nereusstream.delay.runtime;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetMembershipClosureRecord;
import com.nereusstream.delay.protocol.TargetMembershipControlBody;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMembershipPolicy;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;

/** First authenticated membership issuance and closure over one Target Store/source batch. */
public final class TargetMembershipControlStore {
    public static final class Prepared {
        private final TargetMembershipControlStore owner;
        private final TargetStoreBackend.Prepared batch;
        private final SystemMutationResult result;

        private Prepared(
                TargetMembershipControlStore owner, TargetStoreBackend.Prepared batch, SystemMutationResult result) {
            this.owner = owner;
            this.batch = batch;
            this.result = result;
        }
    }

    private final TargetStoreBackend backend;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final int maximumCounters;
    private final int maximumDomains;

    public TargetMembershipControlStore(
            TargetStoreBackend backend,
            TargetQuotaScope scope,
            byte[] lineage,
            int maximumCounters,
            int maximumDomains) {
        this.backend = Objects.requireNonNull(backend, "backend");
        this.scope = Objects.requireNonNull(scope, "scope");
        Bytes.requireLength(lineage, 16, "lineage");
        if (scope.target() != null || Arrays.equals(lineage, new byte[16])
                || maximumCounters < 2 || maximumDomains < 1 || maximumDomains > 64) {
            throw new IllegalArgumentException("membership control needs bounded Shard/lineage accounting");
        }
        this.lineage = Bytes.copy(lineage);
        this.maximumCounters = maximumCounters;
        this.maximumDomains = maximumDomains;
    }

    /** Caller routes immutable result replay first and retains every external authority through native commit. */
    public Prepared prepareFirst(
            BoundedReadBudget budget,
            PreparedControlOperation control,
            SystemMutation mutation,
            SourcePosition source,
            CanonicalTargetPartition physical,
            TargetMembershipControlVerifier.Authority authority) {
        Objects.requireNonNull(control, "control");
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(physical, "physical");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (!scope.shard().equals(source.shardId()) || !scope.shard().equals(mutation.shardId())
                || mutation.type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            throw new IllegalArgumentException("membership control source/Shard mismatch");
        }
        final var body = TargetMembershipControlBody.decode(mutation.canonicalBody());
        final var result = new SystemMutationResult[1];
        final var batch = backend.prepare(budget, reader -> {
            if (reader.source() == null || source.compareTo(reader.source()) <= 0) {
                throw new IllegalStateException("first membership control needs a strictly earlier source frontier");
            }
            final byte[] firstKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                    mutation.systemMutationId());
            final byte[] positionKey = Bytes.concat(
                    new byte[] {TargetKeyCodec.RESULT_POSITION_TAG, TargetKeyCodec.KEY_FORMAT},
                    source.canonicalBytes());
            requireAbsent(reader, ColumnFamily.DEDUPE, firstKey);
            requireAbsent(reader, ColumnFamily.DEDUPE, positionKey);
            final var stamp = new TargetQuotaMutation(
                    TargetQuotaMutation.increment(reader.sourceSequence()), source,
                    Bytes.sha256(mutation.canonicalEnvelope()));
            final var root = root(reader, stamp);
            final var edits = new ArrayList<TargetStoreBackend.Edit>();
            StableCode rejection;
            TargetMembershipControlVerifier.Change change = null;
            if (reader.closedIngressDeadlineThrough() >= mutation.retryUntilEpochMs()) {
                rejection = StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED;
            } else {
                try {
                    change = TargetMembershipControlVerifier.verifyFirstApplication(
                            control, mutation, source, physical,
                            ref -> body.request().isIssue()
                                    ? null
                                    : TargetMembershipStoreAuthority.resolve(reader, scope, lineage, ref),
                            authority);
                    rejection = null;
                } catch (CommandResolutionException denied) {
                    if (denied.stableCode() != StableCode.UNAUTHORIZED_SYSTEM_MUTATION
                            && denied.stableCode() != StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED) {
                        throw denied;
                    }
                    rejection = denied.stableCode();
                } catch (ReadIncompleteException external) {
                    throw new IllegalStateException("membership authority did not complete", external);
                }
            }
            if (rejection == null) {
                final TargetMembershipGrant grant = change.after().grant();
                final TargetMembershipPolicy policy = body.request().policy();
                if (!Arrays.equals(policy.tenantScope(), scope.tenantScope())
                        || !physical.id().equals(grant.offered().target())) {
                    throw new IllegalStateException("membership control differs from Store tenant/Target");
                }
                if (change.action() == TargetMembershipControlVerifier.Action.GRANT) {
                    addImmutable(reader, edits, TargetKeyCodec.identity(physical.id()),
                            CanonicalTargetPartition.VALUE_TYPE, physical.canonicalBytes());
                    addImmutable(reader, edits, policy.encodedKey(), TargetMembershipPolicy.VALUE_TYPE,
                            policy.canonicalBytes());
                    requireAbsent(reader, ColumnFamily.META, grant.encodedKey());
                    edits.add(reader.replace(ColumnFamily.META, grant.encodedKey(),
                            TargetMembershipGrant.VALUE_TYPE, grant.canonicalBytes()));
                } else {
                    requireExact(reader, TargetKeyCodec.identity(physical.id()),
                            CanonicalTargetPartition.VALUE_TYPE, physical.canonicalBytes());
                    requireExact(reader, policy.encodedKey(), TargetMembershipPolicy.VALUE_TYPE,
                            policy.canonicalBytes());
                    if (change.action() == TargetMembershipControlVerifier.Action.CLOSE) {
                        final var closure = new TargetMembershipClosureRecord(body, stamp, lineage);
                        closure.requireGrant(grant);
                        requireAbsent(reader, ColumnFamily.META, closure.key());
                        edits.add(reader.replace(ColumnFamily.META, closure.key(),
                                TargetMembershipClosureRecord.VALUE_TYPE, closure.canonicalBytes()));
                    } else if (change.action() != TargetMembershipControlVerifier.Action.ALREADY_CLOSED) {
                        throw new IllegalStateException("membership verifier returned an unknown action");
                    }
                }
            }
            final var outcome = SystemMutationResult.from(mutation,
                    rejection == null ? ApplyStatus.APPLIED : ApplyStatus.REJECTED,
                    rejection == null ? StableCode.OK : rejection, source.canonicalBytes());
            final TargetResultRecord.CreationAuthority resultAuthority = (record, first) -> {
                record.requireOwner(root);
                if (!record.mutation().equals(stamp) || record.allocation() != null) {
                    throw new IllegalStateException("membership result changed its root/source");
                }
                requireAbsent(reader, ColumnFamily.DEDUPE, record.key());
                if (record.kind() == TargetResultRecord.Kind.SYSTEM) {
                    if (first != null || !Arrays.equals(record.key(), firstKey)
                            || !Arrays.equals(record.typedPayload(), outcome.encode())) {
                        throw new IllegalStateException("membership first result differs from decision");
                    }
                } else if (record.kind() == TargetResultRecord.Kind.POSITION_SYSTEM) {
                    if (first == null || !Arrays.equals(first.key(), firstKey)
                            || !Arrays.equals(first.typedPayload(), outcome.encode())
                            || !Arrays.equals(record.key(), positionKey)) {
                        throw new IllegalStateException("membership position differs from first result");
                    }
                    record.requireFirst(first);
                } else {
                    throw new IllegalStateException("unexpected membership result kind");
                }
            };
            final var first = TargetResultRecord.system(root, outcome, stamp, null, resultAuthority);
            final var position = TargetResultRecord.position(root, first, stamp, resultAuthority);
            edits.add(reader.replace(ColumnFamily.DEDUPE, first.key(), TargetResultRecord.VALUE_TYPE,
                    first.canonicalBytes()));
            edits.add(reader.replace(ColumnFamily.DEDUPE, position.key(), TargetResultRecord.VALUE_TYPE,
                    position.canonicalBytes()));
            result[0] = outcome;
            return new TargetSourceAccounting(scope, lineage, source, stamp.mutationDigest(),
                    maximumCounters, 1, maximumDomains).assemble(reader, edits);
        });
        return new Prepared(this, batch, result[0]);
    }

    public SystemMutationResult commit(Prepared prepared, TargetStoreBackend.CommitAuthority authority) {
        Objects.requireNonNull(prepared, "prepared");
        if (prepared.owner != this) {
            throw new IllegalArgumentException("foreign membership control plan");
        }
        backend.commit(prepared.batch, Objects.requireNonNull(authority, "authority"));
        return prepared.result;
    }

    private TargetQuotaIncarnation root(TargetStoreBackend.Reader reader, TargetQuotaMutation operation) {
        final var identity = new TargetQuotaIdentity(TargetQuotaIdentity.Kind.SHARD,
                scope.shard(), reader.aggregate().accountingIncarnation(), null, null);
        final byte[] key = identity.key();
        key[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            throw new IllegalStateException("membership control lacks its controlled Shard root");
        }
        final var root = TargetQuotaIncarnation.decodeForStore(key,
                TargetValueEnvelope.decode(raw, TargetQuotaIncarnation.VALUE_TYPE).payload(),
                scope.shard(), scope.tenantScope());
        if (!root.identity().equals(identity) || !Arrays.equals(root.recoveryLineage(), lineage)) {
            throw new IllegalStateException("membership result root differs from Store/lineage");
        }
        final var prior = Objects.requireNonNull(reader.aggregate().mutation(), "applied root mutation");
        root.latestMutation().requireAtOrBefore(prior);
        prior.requireAtOrBefore(operation);
        return root;
    }

    private static void addImmutable(TargetStoreBackend.Reader reader,
            ArrayList<TargetStoreBackend.Edit> edits, byte[] key, int type, byte[] payload) {
        final byte[] prior = reader.get(ColumnFamily.META, key);
        if (prior != null) {
            if (!Arrays.equals(TargetValueEnvelope.decode(prior, type).payload(), payload)) {
                throw new IllegalStateException("membership immutable record differs at its Store key");
            }
        } else {
            edits.add(reader.replace(ColumnFamily.META, key, type, payload));
        }
    }

    private static void requireExact(
            TargetStoreBackend.Reader reader, byte[] key, int type, byte[] payload) {
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null || !Arrays.equals(TargetValueEnvelope.decode(raw, type).payload(), payload)) {
            throw new IllegalStateException("membership retained metadata differs from authorized Control");
        }
    }

    private static void requireAbsent(TargetStoreBackend.Reader reader, ColumnFamily family, byte[] key) {
        if (reader.get(family, key) != null) {
            throw new IllegalStateException("first membership control encountered an existing result/grant");
        }
    }
}
