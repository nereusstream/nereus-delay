package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.CheckpointInputFuture;
import com.nereusstream.delay.ownership.SourceReplayMutation;
import com.nereusstream.delay.ownership.TargetSourceApplyRuntime;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.QuotaTransferPlanRef;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetQuotaGrant;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlRequest;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Business validation of a complete, externally authenticated capacity cut; no allocator or new wire artifact. */
public final class TargetQuotaCapacitySnapshot {
    private TargetQuotaCapacitySnapshot() {}

    public record Limits(int shards, int grants, int transferSteps, long bytes) {
        public Limits {
            if (shards < 1 || grants < shards || transferSteps < 1 || bytes < 1
                    || shards == Integer.MAX_VALUE || grants == Integer.MAX_VALUE
                    || transferSteps == Integer.MAX_VALUE || bytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("capacity collection requires explicit finite limits");
            }
        }
    }

    /**
     * A deployment-owned authority, not an arbitrary DTO source. It authenticates the complete domain, policy,
     * original records/absence, reservations and parent plan at the requested Source use point. load is called
     * outside Store locks on the root's bounded I/O path; its returned protection performs only held local checks.
     */
    @FunctionalInterface
    public interface Provider {
        Held load(TargetQuotaGrantControlBody publication, SourcePosition use, Limits limits);
    }

    public interface Held extends AutoCloseable {
        Image image();
        /** Pins the exact revisions, original history and non-borrowable reservation; no remote I/O here. */
        void requireCurrent();
        @Override void close();
    }

    /** The membership list is the authoritative complete tenant allocation domain, including zero/unused cuts. */
    public record Policy(byte[] tenant, long version, byte[] hash, TargetQuotaUsage hardLimit, List<ShardId> members) {
        public Policy {
            tenant = assigned(tenant); hash = assigned(hash);
            if (version == 0) throw invalid("policy version is missing");
            Objects.requireNonNull(hardLimit); Objects.requireNonNull(members);
        }
        @Override public byte[] tenant() { return Bytes.copy(tenant); }
        @Override public byte[] hash() { return Bytes.copy(hash); }
    }

    /** Full grandfathered usage is independently collected from retained ledgers, not copied from the grant. */
    public record Cut(TargetQuotaScope scope, TargetQuotaGrantActivation grant, TargetQuotaUsage usage,
            SourcePosition frontier, byte[] counterDigest, List<TargetQuotaScope> targetScopes) {
        public Cut {
            Objects.requireNonNull(scope); Objects.requireNonNull(usage); counterDigest = assigned(counterDigest);
            Objects.requireNonNull(targetScopes, "complete retained Target grant membership");
            if (scope.target() != null || grant != null && !scope.equals(grant.grant().scope())) {
                throw invalid("capacity cut must be one complete Source Shard");
            }
            if (frontier != null) {
                TargetSourcePosition.requireBounded(frontier);
                if (!scope.shard().equals(frontier.shardId())) throw invalid("cut frontier changes its Shard");
                if (grant != null) atOrBefore(grant.mutation().source(), frontier);
            } else if (grant != null || !usage.isZero()) throw invalid("nonempty cut has no retained Source frontier");
        }
        @Override public byte[] counterDigest() { return Bytes.copy(counterDigest); }
    }

    /** Per-Target retained totals; these remain inside the containing Shard usage rather than adding a tenant cut. */
    public record TargetCut(TargetQuotaGrantActivation grant, TargetQuotaUsage usage, SourcePosition frontier,
            byte[] counterDigest) {
        public TargetCut {
            Objects.requireNonNull(grant); Objects.requireNonNull(usage); Objects.requireNonNull(frontier);
            counterDigest = assigned(counterDigest); TargetSourcePosition.requireBounded(frontier);
            if (grant.grant().scope().target() == null) throw invalid("Target cut has a Shard grant");
            atOrBefore(grant.mutation().source(), frontier);
        }
        @Override public byte[] counterDigest() { return Bytes.copy(counterDigest); }
    }

    /** Original authority proves this is held exclusively for the exact publication, including old excess. */
    public record Reservation(byte[] identity, long revision, byte[] evidence, TargetQuotaScope scope,
            TargetQuotaUsage held, TargetQuotaIncarnation allocation, TargetQuotaGrant reservedGrant,
            CapacityVector physicalRequired, CapacityVector physicalReserved) {
        public Reservation {
            identity = assigned(identity); evidence = assigned(evidence);
            if (revision == 0) throw invalid("reservation revision is missing");
            Objects.requireNonNull(scope); Objects.requireNonNull(held);
            Objects.requireNonNull(reservedGrant); Objects.requireNonNull(physicalRequired); Objects.requireNonNull(physicalReserved);
            if (!scope.equals(reservedGrant.scope()) || !physicalReserved.covers(physicalRequired)) {
                throw invalid("physical reservation changes scope or does not cover its actual placement demand");
            }
        }
        @Override public byte[] identity() { return Bytes.copy(identity); }
        @Override public byte[] evidence() { return Bytes.copy(evidence); }
    }

    public record Step(ControlRef control, TargetQuotaGrantControlRequest request) {
        public Step { Objects.requireNonNull(control); Objects.requireNonNull(request); request.requireControlRef(control); }
    }

    /**
     * The provider authenticates the original complete parent bytes against ref, including the full old/new sets
     * and ordered child publications. This is their parsed projection, not a replacement parent-plan encoding.
     */
    public record Parent(QuotaTransferPlanRef ref, List<TargetQuotaGrant> oldGrants, List<TargetQuotaGrant> newGrants,
            List<Step> steps, boolean increaseApplied) {
        public Parent { Objects.requireNonNull(ref); Objects.requireNonNull(oldGrants); Objects.requireNonNull(newGrants); Objects.requireNonNull(steps); }
    }

    /** Original source-applied hold/shrink and GRANT_SHRINK_DRAINED evidence, with its complete counter cut. */
    public record Drained(TargetQuotaGrantActivation shrink, SourcePosition source, TargetQuotaUsage usage,
            byte[] counterDigest, byte[] evidence) {
        public Drained {
            Objects.requireNonNull(shrink); Objects.requireNonNull(source); Objects.requireNonNull(usage);
            counterDigest = assigned(counterDigest); evidence = assigned(evidence);
            TargetSourcePosition.requireBounded(source); atOrBefore(shrink.mutation().source(), source);
            if (!covers(shrink.grant().limit(), usage)) throw invalid("donor is not drained to its held cut");
        }
        @Override public byte[] counterDigest() { return Bytes.copy(counterDigest); }
        @Override public byte[] evidence() { return Bytes.copy(evidence); }
    }

    public record Image(long revision, Policy policy, List<Cut> cuts, List<TargetCut> targets,
            TargetQuotaGrantControlBody publication, SourcePosition use, TargetQuotaGrantControlVerifier.View view,
            Reservation reservation, Parent activeParent, List<Drained> drained) {
        public Image {
            if (revision == 0) throw invalid("capacity revision is missing");
            Objects.requireNonNull(policy); Objects.requireNonNull(cuts); Objects.requireNonNull(targets);
            Objects.requireNonNull(publication); Objects.requireNonNull(use); Objects.requireNonNull(view);
            Objects.requireNonNull(reservation); Objects.requireNonNull(drained);
            TargetSourcePosition.requireBounded(use);
        }
    }

    /** Only this collection path can create the reusable capacity authority and its paired commit guard. */
    public static Verified collect(Provider provider, SourceReplayMutation entry, Limits limits) {
        Objects.requireNonNull(entry, "original Source mutation");
        final var publication = TargetQuotaGrantControlBody.decode(entry.mutation().canonicalBody());
        final var use = entry.position();
        final var held = Objects.requireNonNull(provider, "deployment capacity provider").load(publication, use, limits);
        Objects.requireNonNull(held, "original protected capacity snapshot");
        try {
            held.requireCurrent();
            final var image = freeze(Objects.requireNonNull(held.image(), "capacity image"), limits, entry);
            if (!same(publication.canonicalBytes(), image.publication().canonicalBytes()) || !sameSource(use, image.use())) {
                throw invalid("capacity snapshot belongs to another publication or Source use point");
            }
            validate(image);
            held.requireCurrent();
            return new Verified(held, image, Bytes.sha256(entry.mutation().canonicalEnvelope()));
        } catch (RuntimeException | Error failure) {
            try { held.close(); } catch (RuntimeException | Error cleanup) {
                final var retained = new CleanupFailure(held, failure);
                if (cleanup != failure) retained.addSuppressed(cleanup);
                throw retained;
            }
            throw failure;
        }
    }

    /** A rejected collection must still retain its original reservation/pins when release fails. */
    public static final class CleanupFailure extends IllegalStateException implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient Held held;
        private boolean complete;
        private CleanupFailure(Held held, Throwable cause) { super("capacity snapshot cleanup is not confirmed", cause); this.held = held; }
        @Override public synchronized boolean cleanupComplete() { return complete; }
        @Override public synchronized CompletableFuture<Void> retryCleanup() {
            if (complete) return CompletableFuture.completedFuture(null);
            try { held.close(); complete = true; return CompletableFuture.completedFuture(null); }
            catch (RuntimeException | Error failure) { return CompletableFuture.failedFuture(failure); }
        }
    }

    /** Not constructible from caller assertions; remains bound to the original source/revisions/reservation. */
    public static final class Verified implements TargetQuotaGrantControlVerifier.CapacityAuthority, AutoCloseable {
        private final Held held;
        private final Image image;
        private final byte[] sourceMutationDigest;
        private int guards;
        private boolean closed;
        private Verified(Held held, Image image, byte[] sourceMutationDigest) {
            this.held = held; this.image = image; this.sourceMutationDigest = sourceMutationDigest;
        }

        @Override public synchronized void requireAuthorized(TargetQuotaGrantControlBody body,
                TargetQuotaGrantControlVerifier.View view, SourcePosition source, TargetQuotaIncarnation allocation) {
            requireCurrent();
            if (!same(image.publication().canonicalBytes(), body.canonicalBytes()) || !sameSource(image.use(), source)
                    || !sameView(image.view(), view) || !sameAllocation(image.reservation().allocation(), allocation)) {
                throw invalid("capacity use differs from the collected exact Store/Source/allocation");
            }
            requireCurrent();
        }

        /** The startup root supplies its actual Owner/assignment/Store guard; this adds held capacity protection. */
        public TargetStoreBackend.CommitAuthority guardCommit(TargetStoreBackend.CommitAuthority physical) {
            Objects.requireNonNull(physical, "original root physical commit authority");
            return (store, scope, mutation) -> {
                synchronized (this) {
                    requireCurrent();
                    mutation.quota().counters().mutation().requireSourceApplied();
                    if (!scope.equals(image.publication().request().next().scope().shardScope())
                            || !sameSource(mutation.quota().counters().mutation().source(), image.use())
                            || !same(mutation.quota().counters().mutation().mutationDigest(), sourceMutationDigest)) {
                        throw invalid("capacity commit changes its original Source or Shard");
                    }
                    guards++;
                }
                final TargetStoreBackend.CommitGuard original;
                try { original = physical.acquire(store, scope, mutation); }
                catch (RuntimeException | Error failure) { synchronized (this) { guards--; } throw failure; }
                return new TargetStoreBackend.CommitGuard() {
                    private boolean released;
                    @Override public void requireCurrent() {
                        synchronized (Verified.this) {
                            if (released) throw invalid("capacity commit guard is closed");
                            Verified.this.requireCurrent(); original.requireCurrent(); Verified.this.requireCurrent();
                        }
                    }
                    @Override public void close() {
                        synchronized (Verified.this) {
                            if (released) return;
                            original.close(); released = true; guards--;
                        }
                    }
                };
            };
        }

        /** Wires the same snapshot into the existing verifier and Source atomic commit, without replacing other rights. */
        public TargetSourceApplyRuntime.GrantControl grantControl(PreparedControlOperation prepared,
                TargetQuotaGrantControlVerifier.Authority original, TargetStoreBackend.CommitAuthority physical) {
            if (!prepared.request().equals(image.publication().request().operationRequest())
                    || !same(prepared.operationId(), image.publication().controlRef().operationId())) {
                throw invalid("capacity preparation changes the registered Control");
            }
            final var authority = new TargetQuotaGrantControlVerifier.Authority(original.registrations(), original.keys(),
                    original.routes(), this, original.actor(), original.scopeProof());
            return new TargetSourceApplyRuntime.GrantControl(prepared, authority, guardCommit(physical));
        }

        private void requireCurrent() {
            if (closed) throw invalid("capacity snapshot is closed");
            held.requireCurrent();
        }
        @Override public synchronized void close() {
            if (closed) return;
            if (guards != 0) throw invalid("capacity snapshot still protects an atomic commit");
            held.close(); closed = true;
        }
    }

    private static Image freeze(Image image, Limits limits, SourceReplayMutation entry) {
        Objects.requireNonNull(limits);
        if (image.cuts().size() > limits.shards() || image.policy().members().size() > limits.shards()
                || (long) image.cuts().size() + image.targets().size() > limits.grants()
                || image.targets().size() > limits.grants() || image.drained().size() > limits.grants()) {
            throw invalid("capacity collection exceeds its configured record bound");
        }
        final var policy = image.policy();
        final var copiedPolicy = new Policy(policy.tenant(), policy.version(), policy.hash(), policy.hardLimit(), List.copyOf(policy.members()));
        final var copiedCuts = new java.util.ArrayList<Cut>();
        long targetMemberships = 0;
        for (var cut : image.cuts()) {
            targetMemberships = Math.addExact(targetMemberships, cut.targetScopes().size());
            if (targetMemberships > limits.grants()) throw invalid("Target grant membership exceeds its record bound");
            copiedCuts.add(new Cut(cut.scope(), cut.grant(), cut.usage(), cut.frontier(), cut.counterDigest(), List.copyOf(cut.targetScopes())));
        }
        final var parent = image.activeParent();
        if (parent != null && (parent.oldGrants().size() > limits.grants() || parent.newGrants().size() > limits.grants()
                || parent.steps().size() > limits.transferSteps())) throw invalid("parent plan exceeds its record bound");
        final var copiedParent = parent == null ? null : new Parent(parent.ref(), List.copyOf(parent.oldGrants()),
                List.copyOf(parent.newGrants()), List.copyOf(parent.steps()), parent.increaseApplied());
        final var frozen = new Image(image.revision(), copiedPolicy, List.copyOf(copiedCuts), List.copyOf(image.targets()),
                image.publication(), image.use(), image.view(), image.reservation(), copiedParent, List.copyOf(image.drained()));
        long bytes = image.publication().canonicalBytes().length + (long) image.use().canonicalBytes().length
                + entry.mutation().canonicalEnvelope().length;
        bytes = Math.addExact(bytes, policy.hardLimit().canonicalBytes().length + 72L + policy.members().size() * 20L);
        for (var cut : frozen.cuts()) bytes = Math.addExact(bytes, cut.scope().canonicalBytes().length + (long) cut.usage().canonicalBytes().length
                + 32 + (cut.frontier() == null ? 0 : cut.frontier().canonicalBytes().length)
                + (cut.grant() == null ? 0 : cut.grant().canonicalBytes().length));
        for (var cut : frozen.cuts()) for (var scope : cut.targetScopes()) bytes = Math.addExact(bytes, scope.canonicalBytes().length);
        for (var target : frozen.targets()) bytes = Math.addExact(bytes, target.grant().canonicalBytes().length
                + (long) target.frontier().canonicalBytes().length + target.usage().canonicalBytes().length + 32);
        for (var drain : frozen.drained()) bytes = Math.addExact(bytes, drain.shrink().canonicalBytes().length
                + (long) drain.source().canonicalBytes().length + drain.usage().canonicalBytes().length + 64);
        if (copiedParent != null) {
            bytes = Math.addExact(bytes, copiedParent.ref().canonicalBytes().length + 1L);
            for (var grant : copiedParent.oldGrants()) bytes = Math.addExact(bytes, grant.canonicalBytes().length);
            for (var grant : copiedParent.newGrants()) bytes = Math.addExact(bytes, grant.canonicalBytes().length);
            for (var step : copiedParent.steps()) bytes = Math.addExact(bytes, step.control().canonicalBytes().length + (long) step.request().canonicalBytes().length);
        }
        final var reservation = frozen.reservation();
        bytes = Math.addExact(bytes, reservation.held().canonicalBytes().length + (long) reservation.scope().canonicalBytes().length + 72
                + (reservation.allocation() == null ? 0 : reservation.allocation().canonicalBytes().length)
                + reservation.reservedGrant().canonicalBytes().length + reservation.physicalRequired().canonicalBytes().length
                + reservation.physicalReserved().canonicalBytes().length);
        final var view = frozen.view();
        bytes = Math.addExact(bytes, view.aggregate().canonicalBytes().length + 24L
                + (view.source() == null ? 0 : view.source().canonicalBytes().length)
                + (view.grant() == null ? 0 : view.grant().canonicalBytes().length)
                + (view.total() == null ? 0 : view.total().canonicalBytes().length));
        if (bytes > limits.bytes()) throw invalid("capacity collection exceeds its configured byte bound");
        return frozen;
    }

    private static void validate(Image image) {
        final var next = image.publication().request().next();
        final var policy = image.policy();
        if (!same(policy.tenant(), next.scope().tenantScope()) || policy.version() != next.tenantPolicyVersion()
                || !same(policy.hash(), next.tenantPolicyHash()) || !next.scope().shard().equals(image.use().shardId())) {
            throw invalid("grant changes its complete tenant policy or Source");
        }
        final var members = new HashSet<>(policy.members());
        if (members.isEmpty() || members.size() != policy.members().size()) throw invalid("tenant domain is empty or duplicated");
        final var cuts = new HashMap<ShardId, Cut>();
        final var targetMemberships = new HashSet<TargetQuotaScope>();
        for (var cut : image.cuts()) {
            if (!same(cut.scope().tenantScope(), policy.tenant()) || !members.contains(cut.scope().shard())
                    || cuts.putIfAbsent(cut.scope().shard(), cut) != null) throw invalid("duplicate or foreign Shard cut");
            for (var scope : cut.targetScopes()) {
                if (scope.target() == null || !scope.shardScope().equals(cut.scope()) || !targetMemberships.add(scope)) {
                    throw invalid("duplicate or foreign retained Target grant membership");
                }
            }
        }
        if (!cuts.keySet().equals(members)) throw invalid("capacity snapshot omits part of the complete tenant domain");
        final var requestedCut = Objects.requireNonNull(cuts.get(next.scope().shard()), "requested Shard cut");
        if (!sameSource(requestedCut.frontier(), image.view().source()) || !sameUsage(requestedCut.usage(), image.view().aggregate().usage())) {
            throw invalid("requested Shard cut differs from actual local aggregate/frontier");
        }
        final var grants = new HashMap<TargetQuotaScope, TargetQuotaGrant>();
        for (var cut : image.cuts()) if (cut.grant() != null) grants.put(cut.scope(), cut.grant().grant());
        final var targetCuts = new HashMap<TargetQuotaScope, TargetCut>();
        final var targetTotals = new HashMap<ShardId, TargetQuotaUsage>();
        for (var targetCut : image.targets()) {
            final var activation = targetCut.grant();
            final var grant = activation.grant();
            final var cut = cuts.get(grant.scope().shard());
            if (grant.scope().target() == null || cut == null || !same(grant.scope().tenantScope(), policy.tenant())
                    || grants.putIfAbsent(grant.scope(), grant) != null || cut.frontier() == null) throw invalid("duplicate or foreign Target grant");
            atOrBefore(activation.mutation().source(), cut.frontier());
            atOrBefore(targetCut.frontier(), cut.frontier());
            targetCuts.put(grant.scope(), targetCut);
            targetTotals.merge(grant.scope().shard(), targetCut.usage(), TargetQuotaUsage::add);
        }
        if (!targetCuts.keySet().equals(targetMemberships)) throw invalid("capacity snapshot omits part of the retained Target grant domain");
        targetTotals.forEach((shard, totals) -> {
            if (!covers(cuts.get(shard).usage(), totals)) throw invalid("retained Target totals exceed their containing Shard usage");
        });
        final var prior = image.publication().request().prior();
        if (!sameGrant(grants.get(next.scope()), prior) || !sameGrant(image.view().grant() == null ? null : image.view().grant().grant(), prior)) {
            throw invalid("capacity snapshot changes the exact prior grant or absence");
        }
        if (next.scope().target() != null) {
            final var targetCut = targetCuts.get(next.scope());
            if (targetCut == null ? !image.view().usage(next.scope()).isZero()
                    : !sameUsage(targetCut.usage(), image.view().usage(next.scope()))) throw invalid("Target retained totals differ from its actual Store view");
        }
        final var reservation = image.reservation();
        if (!reservation.scope().equals(next.scope()) || !sameGrant(reservation.reservedGrant(), next)
                || !covers(reservation.held(), maximum(next.limit(), image.view().usage(next.scope())))
                || !reservation.physicalRequired().covers(maximum(next.limit(), image.view().usage(next.scope())).resources())) {
            throw invalid("publication lacks its original non-borrowable reservation including grandfathered excess");
        }
        final var changed = new HashMap<>(grants); changed.put(next.scope(), next);
        validateCuts(policy, cuts, changed, true);
        validateTransfer(image, grants, cuts, targetCuts);
    }

    private static void validateCuts(Policy policy, Map<ShardId, Cut> cuts, Map<TargetQuotaScope, TargetQuotaGrant> grants, boolean retainUsage) {
        TargetQuotaUsage sum = TargetQuotaUsage.empty();
        for (var cut : cuts.values()) {
            final var grant = grants.get(cut.scope());
            final var limit = grant == null ? TargetQuotaUsage.empty() : grant.limit();
            sum = sum.add(retainUsage ? maximum(limit, cut.usage()) : limit);
        }
        if (!covers(policy.hardLimit(), sum)) throw invalid("tenant sum of effective cuts and grandfathered usage exceeds policy");
        // Target reservations live inside their Shard cut; do not add them to the tenant sum a second time.
        for (var grant : grants.values()) if (grant.scope().target() != null) {
            final var shardGrant = grants.get(grant.scope().shardScope());
            if (shardGrant == null || !covers(shardGrant.limit(), grant.limit())) throw invalid("Target grant exceeds its Source Shard cut");
        }
    }

    private static void validateTransfer(Image image, Map<TargetQuotaScope, TargetQuotaGrant> current, Map<ShardId, Cut> cuts,
            Map<TargetQuotaScope, TargetCut> targetCuts) {
        final var request = image.publication().request();
        final var parent = image.activeParent();
        if (request.transfer() == null) {
            if (parent != null) throw invalid("publication bypasses the tenant policy's active parent plan");
            return;
        }
        if (parent == null || !request.transfer().equals(parent.ref()) || parent.ref().tenantPolicyVersion() != image.policy().version()) {
            throw invalid("transfer reference lacks its exact original complete parent plan");
        }
        final var old = index(parent.oldGrants(), image.policy());
        final var goal = index(parent.newGrants(), image.policy());
        final var state = new HashMap<>(old);
        final var possible = new HashMap<TargetQuotaScope, Set<String>>();
        old.forEach((scope, grant) -> possible.computeIfAbsent(scope, ignored -> new HashSet<>()).add(Bytes.hex(grant.digest())));
        final var controls = new HashSet<String>();
        boolean publicationFound = false;
        boolean observedIncrease = false;
        for (var step : parent.steps()) {
            final var child = step.request();
            final var scope = child.next().scope();
            if (!parent.ref().equals(child.transfer()) || !sameGrant(state.get(scope), child.prior())
                    || !same(child.next().tenantPolicyHash(), image.policy().hash())
                    || !controls.add(Bytes.hex(step.control().operationId()))) throw invalid("parent child sequence/control/prior is not exact");
            state.put(scope, child.next());
            possible.computeIfAbsent(scope, ignored -> new HashSet<>()).add(Bytes.hex(child.next().digest()));
            final var actual = current.get(scope);
            if (actual != null && Long.compareUnsigned(actual.version(), child.next().version()) >= 0
                    && (child.prior() == null ? !child.next().limit().isZero() : !covers(child.prior().limit(), child.next().limit()))) {
                observedIncrease = true;
            }
            if (same(step.control().canonicalBytes(), image.publication().controlRef().canonicalBytes())
                    && child.equals(request)) publicationFound = true;
        }
        if (!publicationFound || !sameSet(state, goal)) throw invalid("publication is not in the complete roll-forward plan");
        for (var entry : current.entrySet()) {
            if (!possible.getOrDefault(entry.getKey(), Set.of()).contains(Bytes.hex(entry.getValue().digest()))) {
                throw invalid("current grant is outside the parent's old/new/intermediate sets");
            }
        }
        for (var scope : old.keySet()) if (!current.containsKey(scope)) throw invalid("parent old grant disappeared without Source proof");
        // The final plan uses grants alone; grandfathered excess remains in each actual intermediate cut until drained.
        validateCuts(image.policy(), cuts, goal, false);
        final boolean increasing = request.prior() == null ? !request.next().limit().isZero()
                : !covers(request.prior().limit(), request.next().limit());
        if (increasing || parent.increaseApplied() || observedIncrease) {
            final var drains = new HashMap<TargetQuotaScope, Drained>();
            for (var drain : image.drained()) if (drains.putIfAbsent(drain.shrink().grant().scope(), drain) != null) throw invalid("duplicate donor proof");
            for (var entry : old.entrySet()) {
                final var after = goal.get(entry.getKey());
                if (after == null) throw invalid("parent removes a grant instead of Source-applying a zero successor");
                if (covers(after.limit(), entry.getValue().limit())) continue;
                final var drain = drains.get(entry.getKey());
                if (drain == null || !covers(minimum(entry.getValue().limit(), after.limit()), drain.shrink().grant().limit())) {
                    throw invalid("recipient increase precedes an exact donor Source shrink/hold and drained proof");
                }
                boolean shrinkFound = false;
                for (var step : parent.steps()) if (sameGrant(step.request().next(), drain.shrink().grant())
                        && same(step.control().canonicalBytes(), drain.shrink().controlRef().canonicalBytes())) shrinkFound = true;
                if (!shrinkFound) throw invalid("donor shrink is not a Source-applied child of the original parent");
                final var cut = Objects.requireNonNull(cuts.get(entry.getKey().shard()));
                if (cut.frontier() == null) throw invalid("donor has no protected current Source cut");
                atOrBefore(drain.source(), cut.frontier());
                final var targetCut = targetCuts.get(entry.getKey());
                final var frontier = entry.getKey().target() == null ? cut.frontier() : targetCut == null ? null : targetCut.frontier();
                final var digest = entry.getKey().target() == null ? cut.counterDigest() : targetCut == null ? null : targetCut.counterDigest();
                final var usage = entry.getKey().target() == null ? cut.usage() : targetCut == null ? null : targetCut.usage();
                if (frontier == null) throw invalid("donor lacks its complete retained scope cut");
                atOrBefore(drain.source(), frontier);
                if (sameSource(drain.source(), frontier) && (!same(drain.counterDigest(), digest)
                        || !sameUsage(drain.usage(), usage))) throw invalid("donor drained counters differ from its exact Source cut");
            }
        }
    }

    private static Map<TargetQuotaScope, TargetQuotaGrant> index(List<TargetQuotaGrant> grants, Policy policy) {
        final var result = new HashMap<TargetQuotaScope, TargetQuotaGrant>();
        for (var grant : grants) if (!same(grant.scope().tenantScope(), policy.tenant()) || !policy.members().contains(grant.scope().shard())
                || result.putIfAbsent(grant.scope(), grant) != null) throw invalid("parent has duplicate or foreign grant scopes");
        return result;
    }
    private static boolean sameSet(Map<TargetQuotaScope, TargetQuotaGrant> left, Map<TargetQuotaScope, TargetQuotaGrant> right) {
        return left.keySet().equals(right.keySet()) && left.entrySet().stream().allMatch(entry -> sameGrant(entry.getValue(), right.get(entry.getKey())));
    }
    private static boolean sameView(TargetQuotaGrantControlVerifier.View left, TargetQuotaGrantControlVerifier.View right) {
        return left.sequence() == right.sequence() && sameSource(left.source(), right.source()) && same(left.recoveryLineage(), right.recoveryLineage())
                && same(left.aggregate().canonicalBytes(), right.aggregate().canonicalBytes())
                && (left.grant() == null ? right.grant() == null : right.grant() != null && same(left.grant().canonicalBytes(), right.grant().canonicalBytes()))
                && (left.total() == null ? right.total() == null : right.total() != null && same(left.total().canonicalBytes(), right.total().canonicalBytes()));
    }
    private static boolean sameAllocation(TargetQuotaIncarnation left, TargetQuotaIncarnation right) {
        return left == null ? right == null : right != null && same(left.canonicalBytes(), right.canonicalBytes());
    }
    private static boolean sameGrant(TargetQuotaGrant left, TargetQuotaGrant right) {
        return left == null ? right == null : right != null && same(left.canonicalBytes(), right.canonicalBytes());
    }
    private static boolean sameSource(SourcePosition left, SourcePosition right) {
        return left == null ? right == null : right != null && same(left.canonicalBytes(), right.canonicalBytes());
    }
    private static void atOrBefore(SourcePosition earlier, SourcePosition frontier) {
        if (!earlier.shardId().equals(frontier.shardId()) || earlier.compareTo(frontier) > 0
                || earlier.compareTo(frontier) == 0 && !sameSource(earlier, frontier)) throw invalid("capacity evidence is ahead of or foreign to its Source cut");
    }
    private static boolean sameUsage(TargetQuotaUsage left, TargetQuotaUsage right) { return same(left.canonicalBytes(), right.canonicalBytes()); }
    private static boolean covers(TargetQuotaUsage limit, TargetQuotaUsage used) {
        return limit.resources().covers(used.resources()) && limit.targets() >= used.targets()
                && limit.executionDomains() >= used.executionDomains() && limit.strictOrderDomains() >= used.strictOrderDomains()
                && limit.accountingIncarnations() >= used.accountingIncarnations();
    }
    private static TargetQuotaUsage maximum(TargetQuotaUsage a, TargetQuotaUsage b) { return combine(a, b, true); }
    private static TargetQuotaUsage minimum(TargetQuotaUsage a, TargetQuotaUsage b) { return combine(a, b, false); }
    private static TargetQuotaUsage combine(TargetQuotaUsage a, TargetQuotaUsage b, boolean maximum) {
        final long[] amounts = a.resources().amounts(), other = b.resources().amounts();
        for (int i = 0; i < amounts.length; i++) amounts[i] = maximum ? Math.max(amounts[i], other[i]) : Math.min(amounts[i], other[i]);
        return new TargetQuotaUsage(new CapacityVector(amounts), choose(a.targets(), b.targets(), maximum),
                choose(a.executionDomains(), b.executionDomains(), maximum), choose(a.strictOrderDomains(), b.strictOrderDomains(), maximum),
                choose(a.accountingIncarnations(), b.accountingIncarnations(), maximum));
    }
    private static long choose(long a, long b, boolean maximum) { return maximum ? Math.max(a, b) : Math.min(a, b); }
    private static byte[] assigned(byte[] value) {
        Bytes.requireLength(value, 32, "capacity evidence identity");
        if (Arrays.equals(value, new byte[32])) throw invalid("capacity evidence identity must be assigned");
        return Bytes.copy(value);
    }
    private static boolean same(byte[] a, byte[] b) { return Arrays.equals(a, b); }
    private static IllegalStateException invalid(String reason) { return new IllegalStateException(reason); }
}
