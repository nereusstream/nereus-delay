package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;

/** Authenticated grant activation plan; source dedupe, atomic bookkeeping and publication belong to the applier. */
public final class TargetQuotaGrantControlVerifier {
    private TargetQuotaGrantControlVerifier() {}

    @FunctionalInterface
    public interface KeyAuthority {
        PublicKey resolve(int signingKeyVersion, SourcePosition source);
    }

    /** Verifies immutable Route tenantRoutingScope and exact physical source identity at this source position. */
    @FunctionalInterface
    public interface RouteAuthority {
        void requireAuthorized(TargetQuotaScope scope, SourcePosition source);
    }

    /**
     * Resolves the complete source-protected tenant policy and current grant/placement set, not merely its hash.
     * Must prove static hard cuts, grandfathered usage, physical reservation, and any exact parent transfer plan:
     * shrink first, preserve donor excess until drained, reserve recipient placement before increase, one plan.
     * A hash/ref, caller-provided usage, timeout or unproven absence cannot authorize an allocation.
     * A non-null allocation is the proposed first Target descriptor: prove exact descriptor/counter absence,
     * full root projection slot fees and physical reserve, parent grant and tenant cuts for the same batch.
     * Null means no allocation; never infer descriptor creation or revival from a later grant update.
     * Its authority snapshot must remain valid through the atomic commit, including initial/zero grants.
     */
    @FunctionalInterface
    public interface CapacityAuthority {
        void requireAuthorized(
                TargetQuotaGrantControlBody body, View view, SourcePosition source, TargetQuotaIncarnation allocation);
    }

    public record Authority(
            ControlTargetRegistrationAuthority registrations,
            KeyAuthority keys,
            RouteAuthority routes,
            CapacityAuthority capacity,
            ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scopeProof) {
        public Authority {
            Objects.requireNonNull(registrations, "registrations");
            Objects.requireNonNull(keys, "keys");
            Objects.requireNonNull(routes, "routes");
            Objects.requireNonNull(capacity, "capacity");
            Objects.requireNonNull(actor, "actor");
            Objects.requireNonNull(scopeProof, "scopeProof");
        }
    }

    /** Exact local Store read set; total is null only when that Target has no accounting record in this view. */
    public record View(
            TargetQuotaGrantActivation grant,
            TargetQuotaAggregate aggregate,
            TargetQuotaTotal total,
            long sequence,
            SourcePosition source,
            byte[] recoveryLineage) {
        public View {
            recoveryLineage = Bytes.copy(Objects.requireNonNull(recoveryLineage, "recoveryLineage"));
            if (recoveryLineage.length != 16 || Arrays.equals(recoveryLineage, new byte[16])) {
                throw new IllegalArgumentException("quota grant view requires an assigned recovery lineage");
            }
            Objects.requireNonNull(aggregate, "aggregate");
            if ((sequence == 0) != (source == null)
                    || (source != null && !aggregate.shard().equals(source.shardId()))) {
                throw new IllegalArgumentException("quota grant view source/sequence mismatch");
            }
            if (source != null) {
                TargetSourcePosition.requireBounded(source);
            }
            requireAtOrBefore(aggregate.mutation(), sequence, source);
            if (grant != null) {
                if (!grant.grant().scope().shard().equals(aggregate.shard())) {
                    throw new IllegalArgumentException("quota grant view has a foreign current grant");
                }
                if (grant.allocation() != null
                        && !Arrays.equals(recoveryLineage, grant.allocation().recoveryLineage())) {
                    throw new IllegalStateException("quota grant allocation belongs to another recovery lineage");
                }
                requireAtOrBefore(grant.mutation(), sequence, source);
                requireConsistentStamps(grant.mutation(), aggregate.mutation());
            }
            if (total != null) {
                total.requireAggregate(aggregate);
                if (grant != null) {
                    requireConsistentStamps(grant.mutation(), total.mutation());
                }
            }
        }

        @Override
        public byte[] recoveryLineage() {
            return Bytes.copy(recoveryLineage);
        }

        public TargetQuotaUsage usage(final TargetQuotaScope scope) {
            if (!scope.shard().equals(aggregate.shard())
                    || (grant != null && !scope.equals(grant.grant().scope()))
                    || (total != null && !scope.equals(total.scope()))
                    || (scope.target() == null && total != null)) {
                throw new IllegalStateException("quota grant read set does not match the exact requested scope");
            }
            return scope.target() == null
                    ? aggregate.usage()
                    : total == null ? TargetQuotaUsage.empty() : total.usage();
        }

        private void requireSame(final View actual) {
            if (actual == null
                    || !Arrays.equals(recoveryLineage, actual.recoveryLineage)
                    || sequence != actual.sequence
                    || !sameSource(source, actual.source)
                    || !sameGrant(grant, actual.grant)
                    || !Arrays.equals(aggregate.canonicalBytes(), actual.aggregate.canonicalBytes())
                    || (total == null
                            ? actual.total != null
                            : actual.total == null
                                    || !Arrays.equals(total.canonicalBytes(), actual.total.canonicalBytes()))) {
                throw new IllegalStateException("quota grant read set changed before atomic commit");
            }
        }
    }

    public record Change(View before, TargetQuotaGrantActivation after) {
        public Change {
            Objects.requireNonNull(before, "before");
            Objects.requireNonNull(after, "after");
        }

        /** Run inside the Owner/Store guard; external authority snapshots must also remain valid. */
        public void requireCurrent(final View actual) {
            before.requireSame(actual);
        }
    }

    /** No writes, no local cache publication, and no replay shortcut. The caller resolves source dedupe first. */
    public static Change verifyFirstApplication(
            final PreparedControlOperation prepared,
            final SystemMutation mutation,
            final SourcePosition source,
            final View view,
            final Authority authority) {
        TargetSourcePosition.requireBounded(source);
        Objects.requireNonNull(authority, "authority");
        if (prepared.kind() != ControlOperationKind.PUBLISH_TARGET_QUOTA_GRANT
                || mutation.type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            throw unauthorized("not a Target quota grant control mutation");
        }
        final var body = TargetQuotaGrantControlBody.decode(mutation.canonicalBody());
        final var request = body.request();
        if (!source.shardId().equals(body.shard())
                || !source.shardId().equals(mutation.shardId())
                || !source.shardId().equals(view.aggregate.shard())
                || body.retryUntil() != mutation.retryUntilEpochMs()
                || !Arrays.equals(body.logicalIdentity(), mutation.logicalOperationIdentity())
                || !request.operationRequest().equals(prepared.request())
                || !Arrays.equals(body.controlRef().operationId(), prepared.operationId())) {
            throw unauthorized("quota grant prepared request/body/source mismatch");
        }
        if (source.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()) {
            throw new CommandResolutionException(
                    StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                    "quota grant source is outside its signed mutation retry window");
        }
        final var preparedKey = authority.keys.resolve((int) prepared.signingKeyVersion(), source);
        final var mutationKey = authority.keys.resolve(mutation.signingKeyVersion(), source);
        if (preparedKey == null
                || mutationKey == null
                || !prepared.verifySignature(preparedKey)
                || !mutation.verifySignature(mutationKey)) {
            throw unauthorized("quota grant signing key/signature is not authorized at source");
        }
        final var author = AuthorIdentity.decode(mutation.authorIdentity());
        if (author.kind() != AuthorIdentity.Kind.CONTROL
                || !Arrays.equals(author.first(), prepared.author().operationActorIdHash())
                || !Arrays.equals(author.second(), prepared.author().authenticatedRoleSetHash())
                || !Arrays.equals(author.digest(), prepared.author().tenantResourceScopeHash())) {
            throw unauthorized("quota grant signed author differs from the registered author");
        }
        try {
            ControlOperationAuthorization.authorize(prepared, authority.actor, ignored -> true);
        } catch (IllegalArgumentException rejected) {
            throw unauthorized(rejected.getMessage());
        }
        if (!authority.scopeProof.covers(prepared)) {
            throw unauthorized("authenticated Control scope does not cover the quota grant");
        }
        final var registered =
                authority.registrations.find(prepared.operationId()).orElse(null);
        if (registered == null || !Arrays.equals(prepared.canonicalBytes(), registered.canonicalBytes())) {
            throw unauthorized("quota grant Control operation is not registered exactly");
        }
        authority.registrations.validateMutation(prepared, prepared.targets().getFirst(), mutation);
        authority.routes.requireAuthorized(request.next().scope(), source);
        view.usage(request.next().scope());
        if (request.prior() == null
                ? view.grant != null
                : view.grant == null
                        || !Arrays.equals(
                                request.prior().canonicalBytes(),
                                view.grant.grant().canonicalBytes())) {
            throw unauthorized("quota grant exact prior version/artifact differs from the Store");
        }
        if (view.source != null && source.compareTo(view.source) <= 0) {
            throw new IllegalStateException("quota grant source must strictly advance the Store view");
        }
        final var stamp = new TargetQuotaMutation(
                TargetQuotaMutation.increment(view.sequence), source, Bytes.sha256(mutation.canonicalEnvelope()));
        if (view.grant != null) {
            stamp.requireAfter(view.grant.mutation());
        }
        // Ordinary updates retain the exact initial origin, including a reduction to zero or a later increase.
        final var priorAllocation = view.grant == null ? null : view.grant.allocation();
        final TargetQuotaIncarnation allocation;
        if (request.next().scope().target() != null
                && priorAllocation == null
                && !request.next().limit().isZero()) {
            if (view.total != null) {
                throw new IllegalStateException("cannot invent an allocation origin over an existing Target total");
            }
            allocation = TargetQuotaIncarnation.allocate(
                    request.next().scope(), request.next().accounting(), view.recoveryLineage, stamp, (prior, next) -> {
                        if (!request.next().limit().permitsGrowth(TargetQuotaUsage.empty(), next.ownContribution())) {
                            throw new IllegalStateException(
                                    "initial Target grant cannot fund its incarnation descriptor");
                        }
                        authority.capacity.requireAuthorized(body, view, source, next);
                    });
        } else {
            allocation = priorAllocation;
            if (allocation != null
                    && !Arrays.equals(
                            allocation.accounting().canonicalBytes(),
                            request.next().accounting().canonicalBytes())) {
                throw new IllegalStateException("ordinary grant update cannot reprice its frozen allocation origin");
            }
            // Do not translate transient/fatal external authority failures into accepted updates or no-ops.
            authority.capacity.requireAuthorized(body, view, source, null);
        }
        return new Change(
                view,
                new TargetQuotaGrantActivation(
                        request,
                        body.controlRef(),
                        stamp,
                        mutation.systemMutationId(),
                        mutation.mutationHash(),
                        allocation));
    }

    private static void requireAtOrBefore(
            final TargetQuotaMutation stamp, final long sequence, final SourcePosition source) {
        if (stamp == null) {
            return;
        }
        stamp.requireAtOrBefore(sequence, source);
    }

    private static void requireConsistentStamps(final TargetQuotaMutation left, final TargetQuotaMutation right) {
        if (left == null || right == null) {
            return;
        }
        if (Long.compareUnsigned(left.sequence(), right.sequence()) < 0
                || (left.sequence() == right.sequence()
                        && Long.compareUnsigned(left.localClaimOrdinal(), right.localClaimOrdinal()) <= 0)) {
            left.requireAtOrBefore(right);
        } else {
            right.requireAtOrBefore(left);
        }
    }

    private static boolean sameSource(final SourcePosition left, final SourcePosition right) {
        return left == null
                ? right == null
                : right != null && Arrays.equals(left.canonicalBytes(), right.canonicalBytes());
    }

    private static boolean sameGrant(final TargetQuotaGrantActivation left, final TargetQuotaGrantActivation right) {
        return left == null
                ? right == null
                : right != null && Arrays.equals(left.canonicalBytes(), right.canonicalBytes());
    }

    private static CommandResolutionException unauthorized(final String message) {
        return new CommandResolutionException(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, message);
    }
}
