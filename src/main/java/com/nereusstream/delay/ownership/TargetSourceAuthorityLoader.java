package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import com.nereusstream.delay.runtime.*;
import com.nereusstream.delay.store.TargetStoreBackend;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Repository collector for the original deployment-owned, protected content/history inputs. */
public final class TargetSourceAuthorityLoader implements TargetSourceAuthorityPreparation.Loader {
    /**
     * Loads and authenticates original issuer/role/Route/history/materialization data at this Source point.
     * Except the Control keys/registration explicitly collected below, returned authority callbacks must
     * consume only this hold's immutable local snapshots. No current-role substitution or remote I/O at commit.
     */
    @FunctionalInterface public interface Provider {
        HeldInputs load(SourceReplayEntry entry, TargetSourceAuthorityPreparation.Family family, Runnable guard);
    }

    public interface HeldInputs extends AutoCloseable {
        SourceReplayEntry entry();
        Inputs inputs();
        long retainedBytes();
        /** Local version, complete-history and pin protection only. */
        void requireCurrent();
        /** Original hold release; a failed close must retain the original release state for retry. */
        @Override void close();
    }

    public sealed interface Inputs permits Command, Grant, Fence, Expiry, Close, Membership, NativePolicy, Admission, Outcome, Channel, Retirement, Activation {}
    public record Activation(PreparedControlOperation prepared,TargetProtocolActivationStore.Authority authority) implements Inputs {}
    public record Command(TargetCommandStore.Policy policy, TargetCommandStore.CancellationControls cancellations,
            TargetReservationControls.Authority reservations, TargetCommandStore.Schedules schedules,
            TargetCommandStore.PayloadProofControls payloadProofs) implements Inputs {}
    public record Grant(PreparedControlOperation prepared, TargetQuotaGrantControlVerifier.Authority authority,
            TargetQuotaCapacitySnapshot.Provider capacity, TargetQuotaCapacitySnapshot.Limits capacityLimits) implements Inputs {}
    public record Fence(TargetTimeFenceVerifier.Authority authority) implements Inputs {}
    public record Expiry(TargetExpireGenerationVerifier.Authority authority) implements Inputs {}
    public record Close(PreparedControlOperation prepared, TargetCloseVerifier.Authority authority) implements Inputs {}
    public record Membership(PreparedControlOperation prepared, CanonicalTargetPartition physical,
            TargetMembershipControlVerifier.Authority authority) implements Inputs {}
    public record NativePolicy(PreparedControlOperation prepared, TargetNativePolicyControlVerifier.Authority authority) implements Inputs {}
    public record Admission(TargetPublishAdmissionVerifier.Authority authority) implements Inputs {}
    public record Outcome(TargetPublishOutcomeVerifier.Authority authority) implements Inputs {}
    public record Channel(PreparedControlOperation prepared, TargetChannelControlVerifier.Authority authority) implements Inputs {}
    /** Provider-approved original Source purpose/Root/capacity and actual Catalog copy; null approval is a proved denial. */
    public record Retirement(TargetQuotaScope scope, TargetEvidenceRetirementStore.Approval approval) implements Inputs {
        public Retirement { Objects.requireNonNull(scope); if (scope.target() != null) throw new IllegalArgumentException("retirement needs Source scope"); }
    }

    private final Provider provider;
    private final TargetStoreBackend.CommitAuthority originalCommit;
    private final long maximumBytes;

    public TargetSourceAuthorityLoader(Provider provider, TargetStoreBackend.CommitAuthority originalCommit, long maximumBytes) {
        this.provider = Objects.requireNonNull(provider, "original content/history provider");
        this.originalCommit = Objects.requireNonNull(originalCommit, "root's actual physical guard");
        if (maximumBytes < 1 || maximumBytes == Long.MAX_VALUE) throw new IllegalArgumentException("finite content bytes required");
        this.maximumBytes = maximumBytes;
    }

    @Override public Set<TargetSourceAuthorityPreparation.Family> families() {
        return Set.copyOf(EnumSet.allOf(TargetSourceAuthorityPreparation.Family.class));
    }

    @Override public TargetSourceAuthorityPreparation.Held load(SourceReplayEntry entry,
            TargetSourceAuthorityPreparation.Family family, Runnable guard) {
        guard.run();
        var original = Objects.requireNonNull(provider.load(entry, family, guard), "protected original inputs");
        var held = new Held(entry, original);
        try {
            guard.run(); original.requireCurrent();
            if (!SourceApplyCoordinator.sameEntry(entry, original.entry()) || original.retainedBytes() < 0
                    || original.retainedBytes() > maximumBytes) {
                throw new IllegalStateException("content provider changes original frame/proof or exceeds its byte bound");
            }
            final Inputs inputs = Objects.requireNonNull(original.inputs(), "typed original content");
            if (family(inputs) != family) throw new IllegalStateException("original Source content family differs");
            held.control = collect(entry, inputs, held, guard);
            original.requireCurrent(); guard.run();
            if (held.bytes > maximumBytes) throw new IllegalStateException("complete content snapshot exceeds byte bound");
            return held;
        } catch (RuntimeException | Error failure) {
            if (failure instanceof CheckpointInputFuture.Cleanup pending) held.pendingCapacityCleanup = pending;
            try { held.close(); }
            catch (RuntimeException | Error cleanup) {
                var retained = new TargetSourceAuthorityPreparation.CleanupFailure(held, failure);
                if (cleanup != failure) retained.addSuppressed(cleanup);
                throw retained;
            }
            throw failure;
        }
    }

    private Object collect(SourceReplayEntry entry, Inputs inputs, Held held, Runnable guard) {
        var commit = held.commit(originalCommit);
        return switch (inputs) {
            case Command value -> new TargetSourceApplyRuntime.CommandControl(value.policy(), value.cancellations(),
                    value.reservations(), value.schedules(), value.payloadProofs(), commit);
            case Fence value -> new TargetSourceApplyRuntime.FenceControl(value.authority(), commit);
            case Expiry value -> new TargetSourceApplyRuntime.ExpiryControl(value.authority(), commit);
            case Admission value -> new TargetSourceApplyRuntime.AdmissionControl(value.authority(), commit);
            case Outcome value -> new TargetSourceApplyRuntime.OutcomeControl(value.authority(), commit);
            case Retirement value -> retirement(entry, value, held, commit, guard);
            case Activation value -> {
                final var original=(SourceReplayMutation)entry;final var body=TargetProtocolActivationBody.decode(original.mutation().canonicalBody());
                final var a=value.authority();final var frozen=freeze(value.prepared(),entry,a.registrations(),v->a.keys().resolve(v,entry.position()),held,guard);
                final var captured=Objects.requireNonNull(a.inputs().capture(value.prepared(),original.mutation(),entry.position(),body),"original Target activation snapshot");
                captured.current().run();held.requireCurrent();guard.run();
                held.bytes=Math.addExact(held.bytes,captured.retainedBytes());
                if(held.bytes>maximumBytes)throw new IllegalStateException("complete Target activation inputs exceed Source snapshot bound");
                final var retained=new TargetProtocolActivationStore.Inputs(captured.readers(),captured.reset(),captured.resetIssuer(),()->{
                    held.requireCurrent();captured.current().run();held.requireCurrent();
                });
                final var authority=new TargetProtocolActivationStore.Authority(frozen,(v,s)->frozen.key(v,s),a.actor(),a.scopeProof(),(prepared,marker,source,actual)->{
                    held.requireCurrent();requireSource(entry,source);
                    if(!Arrays.equals(prepared.canonicalBytes(),value.prepared().canonicalBytes())||!marker.equals(original.mutation())
                            ||!Arrays.equals(actual.canonicalBytes(),body.canonicalBytes()))throw new IllegalStateException("Target activation snapshot changed original marker");
                    retained.current().run();return retained;
                });
                yield new TargetSourceApplyRuntime.ProtocolActivationControl(value.prepared(),authority,commit);
            }
            case Grant value -> {
                var a = value.authority(); var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve((int) version, entry.position()), held, guard);
                var captured = new TargetQuotaGrantControlVerifier.Authority(frozen, (version, source) -> frozen.key(version, source),
                        a.routes(), a.capacity(), a.actor(), a.scopeProof());
                held.capacity = TargetQuotaCapacitySnapshot.collect(value.capacity(), (SourceReplayMutation) entry, value.capacityLimits());
                // Retain the concrete collector's actual canonical-input accounting, not its configured maximum.
                held.bytes = Math.addExact(held.bytes, held.capacity.retainedBytes());
                yield held.capacity.grantControl(value.prepared(), captured, commit);
            }
            case Close value -> {
                var a = value.authority(); var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve((int) version, entry.position()), held, guard);
                yield new TargetSourceApplyRuntime.CloseControl(value.prepared(), new TargetCloseVerifier.Authority(
                        frozen, (version, source) -> frozen.key(version, source), a.coverage(), a.actor(), a.scopeProof()), commit);
            }
            case Membership value -> {
                var a = value.authority(); var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve((int) version, entry.position()), held, guard);
                yield new TargetSourceApplyRuntime.MembershipControl(value.prepared(), value.physical(),
                        new TargetMembershipControlVerifier.Authority(frozen, a.policies(),
                                (version, source) -> frozen.key(version, source), a.profiles(), a.actor(), a.scopeProof()), commit);
            }
            case NativePolicy value -> {
                var a = value.authority(); var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve(version, entry.position()), held, guard);
                yield new TargetSourceApplyRuntime.NativePolicyControl(value.prepared(), new TargetNativePolicyControlVerifier.Authority(
                        frozen, (version, source) -> frozen.key(version, source), a.actor(), a.scopeProof()), commit);
            }
            case Channel value -> {
                var a = value.authority(); var frozen = freeze(value.prepared(), entry, a.registrations(),
                        version -> a.keys().resolve(version, entry.position()), held, guard);
                yield new TargetSourceApplyRuntime.ChannelControl(value.prepared(), new TargetChannelControlVerifier.Authority(
                        frozen, (version, source) -> frozen.key(version, source), a.actor(), a.scopeProof(), a.materialization(),
                        a.activatedChannelSlots(), a.currentOwner()), commit);
            }
        };
    }

    private static TargetSourceAuthorityPreparation.Family family(Inputs inputs) {
        return switch (inputs) {
            case Command ignored -> TargetSourceAuthorityPreparation.Family.COMMAND;
            case Grant ignored -> TargetSourceAuthorityPreparation.Family.GRANT;
            case Fence ignored -> TargetSourceAuthorityPreparation.Family.FENCE;
            case Expiry ignored -> TargetSourceAuthorityPreparation.Family.EXPIRY;
            case Close ignored -> TargetSourceAuthorityPreparation.Family.CLOSE;
            case Membership ignored -> TargetSourceAuthorityPreparation.Family.MEMBERSHIP;
            case NativePolicy ignored -> TargetSourceAuthorityPreparation.Family.NATIVE_POLICY;
            case Admission ignored -> TargetSourceAuthorityPreparation.Family.ADMISSION;
            case Outcome ignored -> TargetSourceAuthorityPreparation.Family.OUTCOME;
            case Channel ignored -> TargetSourceAuthorityPreparation.Family.CHANNEL;
            case Retirement ignored -> TargetSourceAuthorityPreparation.Family.RETIREMENT;
            case Activation ignored -> TargetSourceAuthorityPreparation.Family.ACTIVATION;
        };
    }

    private TargetSourceApplyRuntime.RetirementControl retirement(SourceReplayEntry entry, Retirement input,
            Held held, TargetStoreBackend.CommitAuthority commit, Runnable guard) {
        if (!(entry instanceof SourceReplayMutation original) || original.mutation().type() != SystemMutationType.RESOURCE_RETIRE_INTENT)
            throw new IllegalArgumentException("retirement snapshot changed its original Source operation");
        final var mutation = original.mutation();
        final var body = TargetEvidenceRetireIntentBody.decode(mutation.canonicalBody());
        input.scope().requireRoute(body.shard(), body.resource().tenantScope());
        final var approved = input.approval();
        final TargetEvidenceRetirementStore.Approval frozen;
        if (approved == null) frozen = null;
        else {
            approved.current().run(); held.requireCurrent(); guard.run();
            final var copy = approved.copy().image();
            // Preflight using actual held lengths, before serializing/hashing the complete copy.
            final byte[] keyBytes = approved.writerKey().getEncoded();
            if (keyBytes == null || keyBytes.length != 44) throw new IllegalStateException("invalid original retirement Source public key");
            final long upper = Math.addExact(copy.estimatedBytes(), keyBytes.length);
            if (upper > maximumBytes - held.bytes) throw new IllegalStateException("complete retirement Source input exceeds byte bound");
            if (!Arrays.equals(copy.resource().canonicalBytes(), body.resource().canonicalBytes()))
                throw new IllegalStateException("retirement Source snapshot replaced the exact Root object");
            copy.requireContext(body.context());
            final PublicKey key;
            try { key = KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(keyBytes)); }
            catch (java.security.GeneralSecurityException invalid) { throw new IllegalStateException("invalid original retirement Source public key", invalid); }
            held.bytes = Math.addExact(held.bytes, Math.addExact(body.context().copyManifestBytes(), keyBytes.length));
            frozen = new TargetEvidenceRetirementStore.Approval(key, approved.copy(), () -> {
                held.requireCurrent(); approved.current().run(); held.requireCurrent();
            },approved.activatedTuple(),approved.historyGrant());
        }
        held.requireCurrent(); guard.run();
        return new TargetSourceApplyRuntime.RetirementControl((scope, actor, actual, source) -> {
            held.requireCurrent(); requireSource(entry, source);
            if (!scope.equals(input.scope()) || !Arrays.equals(actor.canonicalBytes(), mutation.authorIdentity())
                    || actual != mutation && !Arrays.equals(held.digest, Bytes.sha256(actual.canonicalEnvelope())))
                throw new IllegalStateException("retirement snapshot changed its original scope/actor/Source frame");
            if (frozen != null) frozen.current().run();
            return frozen;
        }, commit);
    }

    @FunctionalInterface private interface Keys { PublicKey resolve(long version); }

    private FrozenControl freeze(PreparedControlOperation prepared, SourceReplayEntry entry,
            ControlTargetRegistrationAuthority registrations, Keys keys, Held held, Runnable guard) {
        var mutation = ((SourceReplayMutation) entry).mutation();
        var registered = registrations.find(prepared.operationId()).orElse(null);
        guard.run(); held.requireCurrent();
        var preparedKey = copyKey(keys.resolve(prepared.signingKeyVersion()));
        var mutationKey = copyKey(keys.resolve(mutation.signingKeyVersion()));
        boolean bound = false;
        if (registered != null && Arrays.equals(registered.canonicalBytes(), prepared.canonicalBytes()) && !prepared.targets().isEmpty()) {
            // A local semantic denial stays with the existing verifier; external registry failures never become denials.
            try { ControlTargetMutationBinding.validate(prepared, prepared.targets().getFirst(), mutation); bound = true; }
            catch (IllegalArgumentException rejected) { bound = false; }
            if (bound) registrations.validateMutation(prepared, prepared.targets().getFirst(), mutation);
        }
        guard.run(); held.requireCurrent();
        held.bytes = Math.addExact(held.bytes, (long) prepared.canonicalBytes().length
                + (registered == null ? 0 : registered.canonicalBytes().length)
                + (preparedKey == null ? 0 : preparedKey.getEncoded().length)
                + (mutationKey == null ? 0 : mutationKey.getEncoded().length));
        return new FrozenControl(held, prepared, registered, preparedKey, mutationKey, mutation, bound);
    }

    private static PublicKey copyKey(PublicKey key) {
        if (key == null) return null; // Proved denial from the original provider; incomplete reads must throw there.
        try { return KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(key.getEncoded())); }
        catch (java.security.GeneralSecurityException invalid) { throw new IllegalStateException("invalid original Control signing key", invalid); }
    }

    private static final class FrozenControl implements ControlTargetRegistrationAuthority {
        private final Held held;
        private final PreparedControlOperation prepared, registered;
        private final PublicKey preparedKey, mutationKey;
        private final SystemMutation mutation;
        private final boolean registryBound;
        FrozenControl(Held held, PreparedControlOperation prepared, PreparedControlOperation registered,
                PublicKey preparedKey, PublicKey mutationKey, SystemMutation mutation, boolean registryBound) {
            this.held = held; this.prepared = prepared; this.registered = registered;
            this.preparedKey = preparedKey; this.mutationKey = mutationKey; this.mutation = mutation; this.registryBound = registryBound;
        }
        PublicKey key(long version, SourcePosition source) {
            held.requireCurrent(); requireSource(held.entry, source);
            if (version == prepared.signingKeyVersion() || version == (int) prepared.signingKeyVersion()) return preparedKey;
            if (version == mutation.signingKeyVersion()) return mutationKey;
            throw new IllegalStateException("Control key not collected for this exact source use");
        }
        @Override public RegistrationResult register(PreparedControlOperation value) {
            throw new IllegalStateException("Source content is a read-only original registration snapshot");
        }
        @Override public Optional<PreparedControlOperation> find(byte[] operation) {
            held.requireCurrent();
            if (!Arrays.equals(operation, prepared.operationId())) throw new IllegalStateException("Control operation not collected");
            return Optional.ofNullable(registered);
        }
        @Override public void validateMutation(PreparedControlOperation value, ControlTargetRef target, SystemMutation actual) {
            held.requireCurrent();
            if (!registryBound || !Arrays.equals(value.canonicalBytes(), prepared.canonicalBytes())
                    || !Arrays.equals(actual.canonicalEnvelope(), mutation.canonicalEnvelope())) {
                throw new IllegalStateException("Control registry snapshot is not this exact mutation");
            }
            ControlTargetMutationBinding.validate(value, target, actual);
        }
    }

    private static void requireSource(SourceReplayEntry entry, SourcePosition source) {
        if (!Arrays.equals(entry.position().canonicalBytes(), source.canonicalBytes())) {
            throw new IllegalStateException("content snapshot changes its original Source point");
        }
    }

    private static final class Held implements TargetSourceAuthorityPreparation.Held {
        private final SourceReplayEntry entry;
        private final HeldInputs original;
        private final byte[] digest;
        private Object control;
        private TargetQuotaCapacitySnapshot.Verified capacity;
        private CheckpointInputFuture.Cleanup pendingCapacityCleanup;
        private long bytes;
        private int commits;
        private boolean originalClosed, capacityClosed;
        Held(SourceReplayEntry entry, HeldInputs original) {
            this.entry = entry; this.original = original; bytes = original.retainedBytes();
            digest = entry instanceof SourceReplayRecord command ? command.command().canonicalFrameDigest()
                    : Bytes.sha256(((SourceReplayMutation) entry).mutation().canonicalEnvelope());
        }
        @Override public SourceReplayEntry entry() { return entry; }
        @Override public Object control() { return control; }
        @Override public long retainedBytes() { return bytes; }
        @Override public synchronized void requireCurrent() {
            if (originalClosed || capacityClosed) throw new IllegalStateException("original content hold is closing");
            original.requireCurrent();
        }
        TargetStoreBackend.CommitAuthority commit(TargetStoreBackend.CommitAuthority physical) {
            return (store, scope, mutation) -> {
                synchronized (this) {
                    requireCurrent(); var stamp = mutation.quota().counters().mutation();
                    stamp.requireSourceApplied(); requireSource(entry, stamp.source());
                    if (!scope.shard().equals(entry.position().shardId()) || !Arrays.equals(digest, stamp.mutationDigest())) {
                        throw new IllegalStateException("content commit changes its original Shard/frame");
                    }
                    commits++;
                }
                final TargetStoreBackend.CommitGuard acquired;
                try { acquired = physical.acquire(store, scope, mutation); }
                catch (RuntimeException | Error failure) { synchronized (this) { commits--; } throw failure; }
                return new TargetStoreBackend.CommitGuard() {
                    private boolean released;
                    @Override public void requireCurrent() { synchronized (Held.this) {
                        if (released) throw new IllegalStateException("content commit guard closed");
                        Held.this.requireCurrent(); acquired.requireCurrent(); Held.this.requireCurrent();
                    } }
                    @Override public void close() { synchronized (Held.this) {
                        if (!released) { acquired.close(); released = true; commits--; }
                    } }
                };
            };
        }
        @Override public synchronized void close() {
            if (commits != 0) throw new IllegalStateException("original content still protects a commit");
            Throwable failure = null;
            if (!capacityClosed) {
                try {
                    if (pendingCapacityCleanup != null && !pendingCapacityCleanup.cleanupComplete()) {
                        var retry = pendingCapacityCleanup.retryCleanup();
                        if (!retry.isDone() || !pendingCapacityCleanup.cleanupComplete()) {
                            throw new IllegalStateException("original failed capacity collection cleanup remains pending");
                        }
                    }
                    if (capacity != null) capacity.close(); capacityClosed = true;
                }
                catch (RuntimeException | Error rejected) { failure = rejected; }
            }
            if (!originalClosed) {
                try { original.close(); originalClosed = true; }
                catch (RuntimeException | Error rejected) { if (failure == null) failure = rejected; else if (failure != rejected) failure.addSuppressed(rejected); }
            }
            if (failure != null) throw new IllegalStateException("original content/capacity release remains pending", failure);
        }
    }
}
