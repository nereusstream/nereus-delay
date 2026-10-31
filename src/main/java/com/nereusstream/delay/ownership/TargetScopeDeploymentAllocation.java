package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetScopeExposureDebit;
import com.nereusstream.delay.protocol.TargetScopeExposurePolicy;
import com.nereusstream.delay.protocol.TargetScopeResourcePolicy;
import com.nereusstream.delay.protocol.TargetScopeResourceRoot;
import com.nereusstream.delay.protocol.TargetSloControlRegisteredInput;
import com.nereusstream.delay.protocol.TargetSloControlScopeRoot;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.runtime.TargetScopeFeeAuthority;
import com.nereusstream.delay.runtime.TrustedUtcClock;
import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Finite composition of the deployment's two original owned allocations. The original persistent
 * Source/Oxia allocation owners are an external deployment prerequisite, not implemented here.
 * Only the trusted composition root supplies Owners; configuration, signatures, connectivity and
 * snapshots cannot be converted into owned handles. Without both actual owners E4 stays unavailable.
 */
public final class TargetScopeDeploymentAllocation {
    private static final byte[] BACKING_DOMAIN = Bytes.utf8("nereus-delay-scope-original-owned-backing-v1\0");
    private static final byte[] PRODUCERS_DOMAIN = Bytes.utf8("nereus-delay-scope-original-producer-admin-domain-v1\0");
    public static final int MAX_DOMAIN_BYTES = 65_536;

    private TargetScopeDeploymentAllocation() {}

    public enum ManagerPurpose {
        INITIALIZE_ROOT,
        RESERVE_CHILD,
        CONFIRM_NATIVE,
        BIRTH_SCOPE,
        SEAL_SCOPE,
        CLOSE_SCOPE_ISSUANCE,
        CLOSE_CHILD_ALLOCATIONS,
        MIGRATION_ACTIVATE,
        MIGRATION_SETTLE,
        MIGRATION_RETIRE_INTENT,
        MIGRATION_RETIRE_CONFIRM,
        PAYLOAD_SUMMARY_INITIALIZE,
        MIGRATION_ARCHIVE
    }

    /**
     * Bounded immutable references to actual already-held facts, not a grant. actualNative and
     * expected are privately minted original holds; their borrowed views introduce no close owner.
     * The complete frame, actual paid Scope, Final/current custody and closure-discharge facts come
     * from actualNative, not caller amounts, balances or a coverageComplete flag. expected carries
     * the exact Root2, original birth marker and backend versions on the actual guarded CAS path.
     */
    public record ClosureRequest(
            TargetSourceApplyRuntime.NativeExposureHold actualNative,
            TargetScopeResourcePolicy resourcePolicy,
            TargetScopeResourceRoot.Child confirmedChild,
            TargetSloControlScopeAuthority.RootHold expected,
            TargetScopeExposureDebit.Origin origin,
            TargetScopeExposureSource.CopyContract sdk,
            TargetScopeExposurePolicy.Kind kind) {
        public ClosureRequest {
            Objects.requireNonNull(actualNative);
            Objects.requireNonNull(resourcePolicy);
            Objects.requireNonNull(confirmedChild);
            Objects.requireNonNull(expected);
            Objects.requireNonNull(origin);
            Objects.requireNonNull(sdk);
            Objects.requireNonNull(kind);
        }
    }

    /**
     * Original Source owner's opaque borrowed local qualification for this one actual CAS and SDK
     * grant. All methods use the same already-held bounded material; they perform no RPC or new
     * allocation and expose no amounts/remaining balances. There is no close/refund ownership.
     */
    public interface ClosureCoverage {
        /** Same-held qualification/revocation check, including the original actual Native facts. */
        void requireCurrent();
        /** Checks complete expected/next/debit binding before that exact Root CAS is submitted. */
        void requireDebitTransition(
                TargetSloControlScopeAuthority.RootHold expected,
                TargetSloControlScopeRoot next,
                TargetScopeExposureDebit debit);
        /**
         * Checks the actual privately minted observed Root/birth/version and exact successful own
         * debit after CAS. Response loss or another debit grants no SDK permission or refund.
         */
        void requireCommittedDebit(
                TargetSloControlScopeAuthority.RootHold expected,
                TargetSloControlScopeAuthority.RootHold observed,
                TargetScopeExposureDebit debit);
    }

    public record ActualBinding(Object processGraph, SourcePosition source, byte[] managedNamespace, String keyPrefix) {
        public ActualBinding {
            Objects.requireNonNull(processGraph, "actual process graph");
            source = TargetSourcePosition.decode(
                    TargetSourcePosition.requireBounded(source).canonicalBytes());
            managedNamespace =
                    TargetScopeResourceConfiguration.assigned(managedNamespace, 32, "actual managed Oxia namespace");
            keyPrefix = TargetScopeResourceConfiguration.text(keyPrefix, 4096, "actual managed Oxia prefix");
            if (keyPrefix.endsWith("/")) {
                throw new IllegalArgumentException("actual managed Oxia prefix has trailing slash");
            }
        }

        @Override
        public byte[] managedNamespace() {
            return managedNamespace.clone();
        }
    }

    /** These are mandatory actual providers, never file/signed-DTO/simulated adapters. */
    public record Owners(SourceOwner source, RegistryOwner registry) {
        public Owners {
            if (source == null || registry == null) {
                throw new ExternalAllocationUnavailable("both original deployment allocation owners are required");
            }
        }
    }

    /**
     * The original provider has already authenticated and durably acquired exclusive fixed shares
     * from its original parent, including all producer/admin identities and old SDK late retries.
     * It must prevent double allocation and isolate unknown restart incarnations persistently;
     * it cannot hand each Process a fresh copy of a common pool. Acquire/read/refresh run on the
     * caller's existing I/O path, outside SDK/handoff locks, under the supplied request/read bounds.
     * Acquire/refresh also retain the complete original input signer, producer-domain and referenced
     * SERVICE-scope materials needed by the local input checks below; missing materials cannot be
     * substituted with policy allowlists, signature digests, actor DTOs or inferred absence.
     * The same original-owner snapshot retains actual Source-position/namespace/producer-admission
     * fee-occurrence facts and positive shared invalid-input payer materials; no second inventory
     * or Root-debit/Native-byte inference substitutes for that original Source qualification.
     * It also retains the original finite promise/failure/retry/concurrency closure contract under
     * the same pre-acquired retained-byte envelope; requireClosureCoverage borrows those facts.
     * A thrown failure retaining native work must implement Cleanup and retain its exact handles
     * and actual in-flight futures until settled. A successful return transfers local close duty.
     */
    @FunctionalInterface
    public interface SourceOwner {
        SourceHandle acquire(
                TargetScopeResourceConfiguration configuration, ActualBinding actualBinding, Runnable guard);
    }

    /**
     * Same original-owner duties as SourceOwner, for the actual managed Oxia namespace and prefix.
     * Its frozen actual charge also covers repeated/failed CAS, response loss, SDK retries, backend
     * WAL/old versions, replication and compaction peaks. A finite successful Root revision count
     * never establishes a physical-space bound. An independent original history owner pays for and
     * protects its own history without omission or a second child C0 debit.
     */
    @FunctionalInterface
    public interface RegistryOwner {
        RegistryHandle acquire(
                TargetScopeResourceConfiguration configuration, ActualBinding actualBinding, Runnable guard);
    }

    public interface Handle extends AutoCloseable {
        /** Pure local check of this actual owner's revocation/admission token; no remote reads. */
        void requireLocalCurrent();
        /**
         * Pure local check of independently held original-owner management authority. Ordinary
         * Source receipts, SDK/fee loans and SERVICE identities alone grant none. The owner pins
         * its independently authenticated grant during acquire; a policy allowlist is insufficient.
         */
        void requireManagementPurpose(ManagerPurpose purpose, AuthorIdentity actor);
        /** Original authenticated independent management grant, never the caller's policy digest. */
        byte[] managementAuthorizationDigest();
        /** Local retained heap/native connection bytes, including outstanding original I/O and cleanup. */
        long retainedBytes();
        /**
         * Actual aggregate provider I/O, tracked through every native exit (not a cancellable
         * dependent result); null only when no original read, refresh or cleanup is outstanding.
         */
        CompletableFuture<?> inFlight();
        /**
         * Idempotently returns local loan/connections. Never refunds persistent physical shares,
         * concludes noFuture, settles UNKNOWN, or retires SDK late writes. Throwing retains duty.
         */
        @Override
        void close();
    }

    public interface SourceHandle extends Handle {
        /**
         * Local continuous qualification of this same actual Source owner. It must retain complete
         * original Source/shard/range/generation facts for every retained prefix and enforce the
         * authenticated frame ceiling at every relevant producer/admin/retry enqueue, including
         * future writers. A snapshot number, largest observed frame or policy allowlist is insufficient.
         * Revocation, Source replacement or loss of any original protection must revoke this check.
         */
        default void requireAllRetainedAndFutureFrames(
                TargetWorkerSourceStartup.FrameOpening opening, SourceSnapshot authenticated) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "original Source has no complete retained-prefix/future-writer frame freeze");
        }
        /** Same pre-acquired SDK/read/codec/shared window, including real JVM layout, field work and native exits. */
        default void requireFrameReadAndCodecWindow(
                TargetWorkerSourceStartup.FrameOpening opening,
                SourceSnapshot authenticated,
                com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.FrameDecodeBounds maximum) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "original Source has no complete prepaid read/codec service window");
        }
        /**
         * Actual Root-selected Native work under the same original IO/SDK/shared owner; computed bytes alone grant
         * none.
         */
        default void requireNativeSourceService(
                TargetWorkerSourceStartup.FrameOpening opening,
                SourceSnapshot authenticated,
                com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.Bounds actual) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "original Source has no paid actual Native service qualification");
        }
        /**
         * Borrows only already acquired local SDK/read/IO/codec/shared capacity, before decoding/copying.
         * The real object/field-work/layout contract and actual Native exits remain with this original
         * owner; mathematical byte-array bounds alone grant no JVM or time qualification. No I/O,
         * consumer/Startup/Store monitor or new acquisition is allowed in this local handoff.
         */
        default TargetWorkerSourceRuntimeFactory.FrameReadLoan borrowFrameReadAndCodec(
                TargetWorkerSourceStartup.FrameOpening opening,
                SourcePosition actual,
                long completeFrameBytes,
                Long generation,
                byte[] attestation,
                com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.FrameDecodeBounds decode) {
            throw new TargetWorkerSourceStartup.FrameCapacityUnavailable(
                    "original Source has no current prepaid SDK/read/codec subloan");
        }
        /**
         * Same complete Source freeze for this exact authenticated old Store/Manifest acquisition; grants no Target
         * writes.
         */
        default void requireAllRetainedAndFutureFrames(
                TargetMigrationProcessDriver.FrameOpening opening, SourceSnapshot authenticated) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "original migration Source has no complete old-domain read frame qualification");
        }
        /** Original old-domain read purpose and paid full codec window; management debit alone is insufficient. */
        default void requireFrameReadAndCodecWindow(
                TargetMigrationProcessDriver.FrameOpening opening,
                SourceSnapshot authenticated,
                com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.FrameDecodeBounds maximum) {
            throw new TargetSourceAuthorityPreparation.InputUnavailable(
                    "original migration Source has no prepaid old-domain read/codec window");
        }
        /** The same original Held pays actual SDK/codec/field work and retains native cleanup; no fresh allocation. */
        default TargetWorkerSourceRuntimeFactory.FrameReadLoan borrowFrameReadAndCodec(
                TargetMigrationProcessDriver.FrameOpening opening,
                SourcePosition actual,
                long completeFrameBytes,
                Long generation,
                byte[] attestation,
                com.nereusstream.delay.runtime.TargetSourceOperationEnvelope.FrameDecodeBounds decode) {
            throw new TargetWorkerSourceStartup.FrameCapacityUnavailable(
                    "original migration Source has no prepaid old-domain SDK/read/codec subloan");
        }
        /** Reads/authenticates the real original allocation on its original I/O path. */
        SourceSnapshot readAuthenticated();
        /** Re-authenticates and updates the original local revocation token, on that same I/O path. */
        SourceSnapshot refresh();
        /**
         * Pure local qualification against complete already-held original input signature, alias,
         * protocol, producer-domain and referenced SERVICE-scope materials. Unknown/unqualified
         * input throws unavailability. This grants neither business Store writes nor management.
         * Never reads a remote Store/Oxia or substitutes a signature digest/actor DTO for authority.
         */
        void requireRegisteredInput(TargetSloControlRegisteredInput input);
        /**
         * Pure local original SERVICE retry obligation from those same held materials. Empty means
         * unavailable/unknown; missing records never prove that no SERVICE obligation exists.
         */
        OptionalLong requiredServiceRetryUntil(TargetSloControlRegisteredInput input);
        /**
         * Pure local occurrence qualification from the original Source owner's already-held
         * authenticated snapshot: actual Source position, namespace and producer admission, plus
         * the original shared invalid-input payer when applicable. Unknown facts throw
         * InputUnavailable. Missing MEMBER, frame debit or NativeWriter never proves INVALID_SHARED.
         * Root debit/Native bytes alone do not authenticate a Source position. The in-repository fee
         * authority separately verifies expected actual Native Scope, complete Root2 frame debit
         * and funding bindings before FUNDED_SCOPE; only this original owner's positive nonbinding
         * qualification permits legacy SharedRoot accounting. This returns no amounts or balances,
         * performs no remote Store/Oxia read, and creates no additional Source inventory.
         */
        TargetScopeFeeAuthority.Payment requireScopeFeeOccurrence(TargetScopeFeeAuthority.Occurrence occurrence);
        /**
         * Mandatory pure local qualification from this actual original Source owner. It binds the
         * same actual Native hold/paid Scope/Final/current custody/closure-discharge facts, confirmed
         * child and resource policy, actual Root2/birth/version, complete frame, Origin, SDK contract
         * and NEW_PROMISE/CLOSURE classification, then the exact expected/next/successful own debit.
         *
         * The original finite contract covers Final/current custody, authorized STOP/CLOSE/FENCE,
         * recovery/bootstrap and existing PENDING/orphan/UNKNOWN/issued-permit/late-I/O obligations;
         * each obligation need not imply an additional Source record. NEW_PROMISE must leave the
         * complete required closure margin after its addition. CLOSURE must be the original required
         * discharge now and cannot consume another debt's protection. Unknown or absent actual-owner
         * qualification throws unavailability; no default grant or caller coverage flag is accepted.
         *
         * Native accounting, Root2 CAS hard budgets and C0/F0/M0 calculations remain in repository;
         * this method neither replaces them with a storage-owner formula nor claims a global closure
         * algorithm/physical implementation here. Returned material is already prepaid within this
         * handle's fixed retainedBytes envelope, with no additional inventory, service, I/O or close
         * ownership. SDK claim only rechecks that same held qualification locally after own commit.
         */
        ClosureCoverage requireClosureCoverage(ClosureRequest request);
        /**
         * Pure local original-provider qualification: each actual native record produces at most
         * one Source position, the exact known SDK copy contract is covered, and retained physical
         * shares cover its retries/late results until explicit retirement. Unknown contracts fail.
         * maximumCopies comes from that SDK (including its value 2); an ACK grants no refund.
         */
        void requireSdkContract(TargetScopeExposureSource.CopyContract contract);
        /** Pure local capture from the original independently qualified UTC clock; never raw wall time. */
        TrustedUtcClock.CustodyTime captureHandoffTime();
    }

    public interface RegistryHandle extends Handle {
        /**
         * Actual authenticated registry allocation. Policy management.maximumHistoryRecords and
         * maximumHistoryWireBytes describe additional application history records, excluding old
         * physical versions of the Root overwritten by CAS. They may be zero only when the actual
         * factory has established that this repository has no extra history writer and the actual
         * original owner still protects any history its own contract requires.
         */
        RegistrySnapshot readAuthenticated();

        RegistrySnapshot refresh();
    }

    /**
     * Original-owner authenticated facts, not authority. Physical units are actual physical bytes:
     * charge freezes worst-case wire expansion/replication and per-record overhead; fixed segment,
     * ledger and allocation overhead is additionally prepaid by fixedSegmentLedgerBytes. No average
     * compression or second C0 debit is permitted. Management prepayment includes Root, first birth,
     * closure and the full finite application history; extra application history is distinct from
     * backend old Root versions/WAL/retries/compaction, which the registry owner's actual charge
     * separately covers. Domain bytes are the original canonical complete inventory
     * of every producer and administrator able to consume or alter these fixed exclusive shares.
     */
    public record Backing(
            TargetScopeResourceConfiguration.ExpectedOwner owner,
            long exclusiveRecords,
            long exclusivePhysicalBytes,
            long fixedSegmentLedgerBytes,
            long prepaidManagementRecords,
            long prepaidManagementPhysicalBytes,
            TargetScopeResourcePolicy.BackingCharge charge,
            byte[] producerAdministratorDomain,
            long retainedUntilEpochMs,
            boolean untilExplicitRetirement) {
        public Backing {
            Objects.requireNonNull(owner);
            Objects.requireNonNull(charge);
            TargetScopeResourceConfiguration.nonnegative(exclusiveRecords, "actual exclusive record units");
            TargetScopeResourceConfiguration.nonnegative(exclusivePhysicalBytes, "actual exclusive physical bytes");
            TargetScopeResourceConfiguration.nonnegative(
                    fixedSegmentLedgerBytes, "fixed segment/ledger physical bytes");
            TargetScopeResourceConfiguration.nonnegative(prepaidManagementRecords, "prepaid management records");
            TargetScopeResourceConfiguration.nonnegative(
                    prepaidManagementPhysicalBytes, "prepaid management physical bytes");
            TargetScopeResourceConfiguration.positive(retainedUntilEpochMs, "original retained-until horizon");
            if (!untilExplicitRetirement
                    || producerAdministratorDomain == null
                    || producerAdministratorDomain.length == 0
                    || producerAdministratorDomain.length > MAX_DOMAIN_BYTES) {
                throw new IllegalArgumentException("original full domain and until-explicit-retirement duty required");
            }
            if (prepaidManagementRecords > exclusiveRecords
                    || prepaidManagementPhysicalBytes > exclusivePhysicalBytes
                    || fixedSegmentLedgerBytes > exclusivePhysicalBytes) {
                throw new IllegalArgumentException("original prepayment exceeds actual exclusive backing");
            }
            producerAdministratorDomain = producerAdministratorDomain.clone();
        }

        @Override
        public byte[] producerAdministratorDomain() {
            return producerAdministratorDomain.clone();
        }
        /** Stable allocation facts; a refreshed retained-until horizon may extend without minting shares. */
        public byte[] canonicalBackingBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, owner.canonicalBytes());
                CanonicalProtobuf.uint64(out, 2, exclusiveRecords);
                CanonicalProtobuf.uint64(out, 3, exclusivePhysicalBytes);
                CanonicalProtobuf.uint64(out, 4, fixedSegmentLedgerBytes);
                CanonicalProtobuf.uint64(out, 5, prepaidManagementRecords);
                CanonicalProtobuf.uint64(out, 6, prepaidManagementPhysicalBytes);
                CanonicalProtobuf.bytes(out, 7, charge.canonicalBytes());
                CanonicalProtobuf.bytes(out, 8, producerAdministratorDomain);
                CanonicalProtobuf.uint32(out, 9, 1); // original until-explicit-retirement responsibility
            });
        }
    }

    public record SourceSnapshot(Backing backing, SourcePosition source, long maximumSourceFrameBytes) {
        public SourceSnapshot {
            Objects.requireNonNull(backing);
            source = TargetSourcePosition.decode(
                    TargetSourcePosition.requireBounded(source).canonicalBytes());
            TargetScopeResourceConfiguration.positive(maximumSourceFrameBytes, "original complete Source frame bound");
            if (maximumSourceFrameBytes >= Integer.MAX_VALUE) {
                throw new IllegalArgumentException("original complete Source frame must fit a finite native frame");
            }
        }

        public byte[] canonicalBackingBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, backing.canonicalBackingBytes());
                CanonicalProtobuf.bytes(out, 2, source.canonicalBytes());
                CanonicalProtobuf.uint64(out, 3, maximumSourceFrameBytes);
            });
        }
    }

    public record RegistrySnapshot(Backing backing, byte[] managedNamespace, String keyPrefix) {
        public RegistrySnapshot {
            Objects.requireNonNull(backing);
            managedNamespace =
                    TargetScopeResourceConfiguration.assigned(managedNamespace, 32, "original managed Oxia namespace");
            keyPrefix = TargetScopeResourceConfiguration.text(keyPrefix, 4096, "original managed Oxia prefix");
            if (keyPrefix.endsWith("/")) {
                throw new IllegalArgumentException("original managed Oxia prefix has trailing slash");
            }
        }

        @Override
        public byte[] managedNamespace() {
            return managedNamespace.clone();
        }

        public byte[] canonicalBackingBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, backing.canonicalBackingBytes());
                CanonicalProtobuf.bytes(out, 2, managedNamespace);
                CanonicalProtobuf.bytes(out, 3, Bytes.utf8(keyPrefix));
            });
        }
    }

    /** Policy.Binding.backingAllocationDigest pins this ordered two-resource combination. */
    public static byte[] backingAllocationDigest(SourceSnapshot source, RegistrySnapshot registry) {
        return Bytes.sha256(BACKING_DOMAIN, pair(source.canonicalBackingBytes(), registry.canonicalBackingBytes()));
    }

    /** Policy.Binding.producerDomainDigest pins both complete original producer/admin domains. */
    public static byte[] producerDomainDigest(SourceSnapshot source, RegistrySnapshot registry) {
        return Bytes.sha256(
                PRODUCERS_DOMAIN,
                pair(
                        source.backing().producerAdministratorDomain(),
                        registry.backing().producerAdministratorDomain()));
    }

    private static byte[] pair(byte[] source, byte[] registry) {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.bytes(out, 1, source);
            CanonicalProtobuf.bytes(out, 2, registry);
        });
    }

    /**
     * No owner means explicit unavailability; there is no requireBacked switch or fallback.
     * guard is the actual process graph's existing local admission/revocation check, without I/O.
     */
    public static Owned acquire(
            TargetScopeResourceConfiguration configuration,
            ActualBinding actualBinding,
            Owners owners,
            Runnable guard) {
        if (owners == null) {
            throw new ExternalAllocationUnavailable("external original Source/Oxia owners are unavailable");
        }
        final var owned = new Owned(configuration, actualBinding, guard);
        try {
            owned.requirePolicyBinding(actualBinding);
            owned.guard.run();
            owned.source.handle = Objects.requireNonNull(
                    owners.source().acquire(configuration, actualBinding, guard), "original Source handle");
            owned.sourceSnapshot = Objects.requireNonNull(((SourceHandle) owned.source.handle).readAuthenticated());
            owned.registry.handle = Objects.requireNonNull(
                    owners.registry().acquire(configuration, actualBinding, guard), "original registry handle");
            owned.registrySnapshot =
                    Objects.requireNonNull(((RegistryHandle) owned.registry.handle).readAuthenticated());
            owned.requireSnapshots(owned.sourceSnapshot, owned.registrySnapshot);
            owned.requireCurrent();
            owned.retainedBytes();
            return owned;
        } catch (RuntimeException failure) {
            owned.acquisitionCleanup = new CleanupWork(failure, null);
            owned.closing = true;
            owned.closeAttempt();
            throw new ExternalAllocationUnavailable(
                    "original deployment allocation acquisition failed", failure, owned);
        }
    }

    public static final class Owned implements AutoCloseable, CheckpointInputFuture.Cleanup {
        private final TargetScopeResourceConfiguration configuration;
        private final ActualBinding actualBinding;
        private final Runnable guard;
        private final CloseSlot source = new CloseSlot(), registry = new CloseSlot();
        private SourceSnapshot sourceSnapshot;
        private RegistrySnapshot registrySnapshot;
        private CleanupWork acquisitionCleanup;
        private boolean closing;
        private boolean refreshing;
        private int localBorrows;

        private Owned(TargetScopeResourceConfiguration configuration, ActualBinding actualBinding, Runnable guard) {
            this.configuration = Objects.requireNonNull(configuration);
            this.actualBinding = Objects.requireNonNull(actualBinding);
            this.guard = Objects.requireNonNull(guard);
        }

        public TargetScopeResourceConfiguration configuration() {
            return configuration;
        }

        public ActualBinding actualBinding() {
            return actualBinding;
        }

        public TargetWorkerSourceRuntimeFactory.SourceFrameContract captureSourceFrames(
                TargetWorkerSourceStartup.FrameOpening opening) {
            final SourceHandle original;
            synchronized (this) {
                requireCurrent();
                original = (SourceHandle) source.handle;
            }
            opening.requireSource(actualBinding.source());
            return TargetWorkerSourceRuntimeFactory.SourceFrameContract.capture(
                    opening, original, this::requireCurrent);
        }

        /** Pure local guard suitable for SDK and Store paths. Remote authentication occurs in refresh. */
        public synchronized void requireCurrent() {
            if (closing || source.handle == null || registry.handle == null) {
                throw new ExternalAllocationUnavailable("original deployment allocation is not locally admitted");
            }
            guard.run();
            source.handle.requireLocalCurrent();
            registry.handle.requireLocalCurrent();
        }

        public synchronized void requireBinding(ActualBinding actual) {
            requireCurrent();
            if (actualBinding.processGraph() != actual.processGraph()
                    || !actualBinding.source().sameSourceIdentity(actual.source())
                    || !Arrays.equals(actualBinding.managedNamespace(), actual.managedNamespace())
                    || !actualBinding.keyPrefix().equals(actual.keyPrefix())) {
                throw new IllegalArgumentException(
                        "owned deployment allocation changed actual process/Source/Oxia binding");
            }
        }

        /** Actual remote I/O: call only on the existing I/O owner outside SDK/handoff locks. */
        public void refresh() {
            final SourceHandle originalSource;
            final RegistryHandle originalRegistry;
            synchronized (this) {
                requireCurrent();
                if (refreshing) {
                    throw new IllegalStateException("original allocation refresh is already outstanding");
                }
                refreshing = true;
                originalSource = (SourceHandle) source.handle;
                originalRegistry = (RegistryHandle) registry.handle;
            }
            try {
                // Never hold the monitor used by local SDK guards while performing remote I/O.
                final var nextSource = Objects.requireNonNull(originalSource.refresh());
                final var nextRegistry = Objects.requireNonNull(originalRegistry.refresh());
                requireSnapshots(nextSource, nextRegistry);
                synchronized (this) {
                    requireCurrent();
                    sourceSnapshot = nextSource;
                    registrySnapshot = nextRegistry;
                    retainedBytes();
                    refreshing = false;
                }
            } catch (RuntimeException failure) {
                synchronized (this) {
                    closing = true;
                    acquisitionCleanup = new CleanupWork(failure, null);
                    refreshing = false;
                }
                throw new ExternalAllocationUnavailable("original deployment allocation refresh failed", failure, this);
            }
        }

        public synchronized void requireSdkContract(TargetScopeExposureSource.CopyContract contract) {
            requireCurrent();
            ((SourceHandle) source.handle).requireSdkContract(Objects.requireNonNull(contract));
            requireCurrent();
        }

        public synchronized void requireRegisteredInput(TargetSloControlRegisteredInput input) {
            requireCurrent();
            ((SourceHandle) source.handle).requireRegisteredInput(Objects.requireNonNull(input));
            requireCurrent();
        }

        public synchronized OptionalLong requiredServiceRetryUntil(TargetSloControlRegisteredInput input) {
            requireCurrent();
            final var until = Objects.requireNonNull(
                    ((SourceHandle) source.handle).requiredServiceRetryUntil(Objects.requireNonNull(input)),
                    "original local SERVICE qualification");
            requireCurrent();
            return until;
        }

        public synchronized TargetScopeFeeAuthority.Payment requireScopeFeeOccurrence(
                TargetScopeFeeAuthority.Occurrence occurrence) {
            requireCurrent();
            final var payment = Objects.requireNonNull(
                    ((SourceHandle) source.handle).requireScopeFeeOccurrence(Objects.requireNonNull(occurrence)),
                    "original local Source fee occurrence qualification");
            requireCurrent();
            return payment;
        }

        public synchronized ClosureCoverage requireClosureCoverage(ClosureRequest request) {
            requireCurrent();
            Objects.requireNonNull(request);
            if (!Arrays.equals(
                    configuration.policy().digest(), request.resourcePolicy().digest())) {
                throw new ExternalAllocationUnavailable("closure qualification changed the original resource policy");
            }
            request.actualNative().requireExposureFrame();
            request.actualNative().requireLocalCurrent();
            final var coverage = Objects.requireNonNull(
                    ((SourceHandle) source.handle).requireClosureCoverage(request),
                    "actual original Source closure qualification");
            requireCurrent();
            return guardedCoverage(request, coverage, this::requireCurrent);
        }

        /** Independent management entry only; ordinary Source and SDK guards never call this method. */
        public synchronized void requireManagementPurpose(ManagerPurpose purpose, AuthorIdentity actor) {
            requireCurrent();
            Objects.requireNonNull(purpose);
            Objects.requireNonNull(actor);
            if (!configuration.policy().authorizedManagers().contains(actor)) {
                throw new ExternalAllocationUnavailable("actor is outside the exact configured manager domain");
            }
            source.handle.requireManagementPurpose(purpose, actor);
            registry.handle.requireManagementPurpose(purpose, actor);
            final byte[] sourceGrant = TargetScopeResourceConfiguration.assigned(
                    source.handle.managementAuthorizationDigest(), 32, "original Source management grant");
            final byte[] registryGrant = TargetScopeResourceConfiguration.assigned(
                    registry.handle.managementAuthorizationDigest(), 32, "original registry management grant");
            if (!Arrays.equals(configuration.policy().managerAuthorizationDigest(), sourceGrant)
                    || !Arrays.equals(configuration.policy().managerAuthorizationDigest(), registryGrant)) {
                throw new ExternalAllocationUnavailable(
                        "original independent management grant differs from the frozen policy");
            }
            requireCurrent();
        }

        public synchronized TrustedUtcClock.CustodyTime captureHandoffTime() {
            requireCurrent();
            final var time = Objects.requireNonNull(((SourceHandle) source.handle).captureHandoffTime());
            if (!time.interval().qualified()
                    || time.interval().latestEpochMs() == Long.MAX_VALUE
                    || sourceSnapshot.backing().retainedUntilEpochMs()
                            < time.interval().latestEpochMs()
                    || registrySnapshot.backing().retainedUntilEpochMs()
                            < time.interval().latestEpochMs()) {
                throw new ExternalAllocationUnavailable("original qualified UTC/retention horizon unavailable");
            }
            requireCurrent();
            return time;
        }

        public synchronized long retainedBytes() {
            long bytes = configuration.canonicalBytes().length;
            if (sourceSnapshot != null) {
                bytes = Math.addExact(bytes, Math.addExact(16, sourceSnapshot.canonicalBackingBytes().length));
            }
            if (registrySnapshot != null) {
                bytes = Math.addExact(bytes, Math.addExact(16, registrySnapshot.canonicalBackingBytes().length));
            }
            bytes = Math.addExact(bytes, source.retainedBytes());
            bytes = Math.addExact(bytes, registry.retainedBytes());
            if (bytes > configuration.limits().maximumRetainedBytes()) {
                throw new ExternalAllocationUnavailable(
                        "original allocation exceeds explicit local retained-byte bound");
            }
            return bytes;
        }

        /** A finite local reference; closing it cannot close or refund either original parent owner. */
        public synchronized Loan borrow() {
            requireCurrent();
            if (localBorrows == configuration.limits().maximumLocalBorrows()) {
                throw new ExternalAllocationUnavailable("original allocation local borrow bound exhausted");
            }
            localBorrows++;
            return new Loan(this);
        }

        private void requirePolicyBinding(ActualBinding actual) {
            final var binding = configuration.policy().binding();
            if (!binding.sourceAnchor().sameSourceIdentity(actual.source())
                    || !Arrays.equals(binding.registryNamespaceBinding(), actual.managedNamespace())
                    || !binding.registryKeyPrefix().equals(actual.keyPrefix())) {
                throw new IllegalArgumentException("policy differs from actual original Source/Oxia binding");
            }
        }

        private void requireSnapshots(SourceSnapshot s, RegistrySnapshot r) {
            final var p = configuration.policy();
            final var limits = configuration.limits();
            final var sb = s.backing();
            final var rb = r.backing();
            final long readBytes = Math.addExact(
                    Math.addExact(16, s.canonicalBackingBytes().length),
                    Math.addExact(16, r.canonicalBackingBytes().length));
            if (readBytes > limits.maximumLocalReadBytes()
                    || !configuration.expectedOwners().source().matches(sb.owner())
                    || !configuration.expectedOwners().registry().matches(rb.owner())
                    || !actualBinding.source().sameSourceIdentity(s.source())
                    || !Arrays.equals(actualBinding.managedNamespace(), r.managedNamespace())
                    || !actualBinding.keyPrefix().equals(r.keyPrefix())
                    || !Arrays.equals(
                            p.backingCharge().canonicalBytes(), sb.charge().canonicalBytes())
                    || !Arrays.equals(
                            p.backingCharge().canonicalBytes(), rb.charge().canonicalBytes())
                    || s.maximumSourceFrameBytes() < limits.maximumSourceFrameBytes()
                    || sb.retainedUntilEpochMs() < limits.minimumRetainedUntilEpochMs()
                    || rb.retainedUntilEpochMs() < limits.minimumRetainedUntilEpochMs()
                    || sb.exclusiveRecords() < p.totalShares().sourceRecords()
                    || rb.exclusiveRecords() < p.totalShares().registryRecords()
                    || sb.exclusivePhysicalBytes()
                            < Math.addExact(
                                    p.backingCharge().sourceBackingBytes(p.totalShares()), sb.fixedSegmentLedgerBytes())
                    || rb.exclusivePhysicalBytes()
                            < Math.addExact(
                                    p.backingCharge().registryBackingBytes(p.totalShares()),
                                    rb.fixedSegmentLedgerBytes())
                    || rb.prepaidManagementRecords() < p.managementShares().registryRecords()
                    || rb.prepaidManagementPhysicalBytes()
                            < p.backingCharge().registryBackingBytes(p.managementShares())
                    || sb.prepaidManagementRecords() < p.managementShares().sourceRecords()
                    || sb.prepaidManagementPhysicalBytes() < p.backingCharge().sourceBackingBytes(p.managementShares())
                    || !Arrays.equals(p.binding().backingAllocationDigest(), backingAllocationDigest(s, r))
                    || !Arrays.equals(p.binding().producerDomainDigest(), producerDomainDigest(s, r))) {
                throw new ExternalAllocationUnavailable(
                        "actual original exclusive allocation does not cover the frozen policy");
            }
        }

        @Override
        public synchronized void close() {
            closing = true;
            closeAttempt();
            if (!cleanupComplete()) {
                throw new ExternalAllocationUnavailable(
                        "original allocation local close retains outstanding duty", null, this);
            }
        }

        private void closeAttempt() {
            if (localBorrows != 0 || refreshing) {
                return;
            }
            if (acquisitionCleanup != null) {
                acquisitionCleanup.retry();
            }
            registry.closeAttempt();
            source.closeAttempt();
        }

        @Override
        public synchronized boolean cleanupComplete() {
            return closing
                    && !refreshing
                    && localBorrows == 0
                    && source.complete()
                    && registry.complete()
                    && (acquisitionCleanup == null || acquisitionCleanup.complete());
        }

        @Override
        public synchronized CompletableFuture<Void> retryCleanup() {
            closing = true;
            closeAttempt();
            return cleanupComplete()
                    ? CompletableFuture.completedFuture(null)
                    : CompletableFuture.failedFuture(new ExternalAllocationUnavailable(
                            "original local cleanup is still outstanding", null, this));
        }
    }

    public static final class Loan implements AutoCloseable {
        private final Owned owned;
        private boolean closed;

        private Loan(Owned owned) {
            this.owned = owned;
        }

        public void requireCurrent() {
            synchronized (owned) {
                if (closed) {
                    throw new IllegalStateException("local allocation loan is closed");
                }
                owned.requireCurrent();
            }
        }

        public void requireRegisteredInput(TargetSloControlRegisteredInput input) {
            synchronized (owned) {
                requireCurrent();
                owned.requireRegisteredInput(input);
                requireCurrent();
            }
        }

        public OptionalLong requiredServiceRetryUntil(TargetSloControlRegisteredInput input) {
            synchronized (owned) {
                requireCurrent();
                final var until = owned.requiredServiceRetryUntil(input);
                requireCurrent();
                return until;
            }
        }

        public TargetScopeFeeAuthority.Payment requireScopeFeeOccurrence(
                TargetScopeFeeAuthority.Occurrence occurrence) {
            synchronized (owned) {
                requireCurrent();
                final var payment = owned.requireScopeFeeOccurrence(occurrence);
                requireCurrent();
                return payment;
            }
        }

        public ClosureCoverage requireClosureCoverage(ClosureRequest request) {
            synchronized (owned) {
                requireCurrent();
                final var coverage = owned.requireClosureCoverage(request);
                requireCurrent();
                return guardedCoverage(request, coverage, this::requireCurrent);
            }
        }

        @Override
        public void close() {
            synchronized (owned) {
                if (!closed) {
                    closed = true;
                    owned.localBorrows--;
                }
            }
        }
    }

    /** Guards only the existing original qualification and its finite local borrow, never mints it. */
    private static ClosureCoverage guardedCoverage(ClosureRequest request, ClosureCoverage original, Runnable guard) {
        return new ClosureCoverage() {
            @Override
            public void requireCurrent() {
                guard.run();
                request.actualNative().requireLocalCurrent();
                original.requireCurrent();
                guard.run();
            }

            @Override
            public void requireDebitTransition(
                    TargetSloControlScopeAuthority.RootHold expected,
                    TargetSloControlScopeRoot next,
                    TargetScopeExposureDebit debit) {
                requireCurrent();
                requireSameExpected(expected);
                original.requireDebitTransition(expected, Objects.requireNonNull(next), Objects.requireNonNull(debit));
                requireCurrent();
            }

            @Override
            public void requireCommittedDebit(
                    TargetSloControlScopeAuthority.RootHold expected,
                    TargetSloControlScopeAuthority.RootHold observed,
                    TargetScopeExposureDebit debit) {
                requireCurrent();
                requireSameExpected(expected);
                original.requireCommittedDebit(
                        expected, Objects.requireNonNull(observed), Objects.requireNonNull(debit));
                requireCurrent();
            }

            private void requireSameExpected(TargetSloControlScopeAuthority.RootHold expected) {
                if (request.expected() != expected) {
                    throw new ExternalAllocationUnavailable(
                            "closure qualification changed its actual guarded Root CAS hold");
                }
            }
        };
    }

    /** Carries the exact retained composition after any partial acquisition/close/refresh failure. */
    public static final class ExternalAllocationUnavailable extends IllegalStateException
            implements CheckpointInputFuture.Cleanup {
        private static final long serialVersionUID = 1L;
        private final transient Owned retained;

        public ExternalAllocationUnavailable(String message) {
            this(message, null, null);
        }

        private ExternalAllocationUnavailable(String message, Throwable cause, Owned retained) {
            super(message, cause);
            this.retained = retained;
        }

        @Override
        public boolean cleanupComplete() {
            return retained == null || retained.cleanupComplete();
        }

        @Override
        public CompletableFuture<Void> retryCleanup() {
            return retained == null ? CompletableFuture.completedFuture(null) : retained.retryCleanup();
        }
    }

    /** At most one retained failure/future per original handle; never replaces unfinished cleanup. */
    private static final class CloseSlot {
        Handle handle;
        boolean closeReturned;
        CleanupWork pending;

        long retainedBytes() {
            if (handle == null) {
                return 0;
            }
            final long bytes = handle.retainedBytes();
            TargetScopeResourceConfiguration.nonnegative(bytes, "original handle retained bytes");
            return bytes;
        }

        void closeAttempt() {
            if (handle == null) {
                return;
            }
            if (pending != null) {
                pending.retry();
                if (!pending.complete()) {
                    return;
                }
                pending = null;
            }
            if (closeReturned) {
                return;
            }
            try {
                CheckpointInputFuture.requestStop(handle.inFlight());
                handle.close();
                closeReturned = true;
                pending = new CleanupWork(null, handle.inFlight());
            } catch (RuntimeException failure) {
                pending = new CleanupWork(failure, handle.inFlight());
            }
        }

        boolean complete() {
            return handle == null || closeReturned && (pending == null || pending.complete());
        }
    }

    private static final class CleanupWork {
        final Throwable failure;
        final CompletableFuture<?> inFlight;
        // Backpressure cleanup retries instead of accumulating exceptional native futures without
        // bound. A slot is reusable only after its actual I/O and failure-owned cleanup have exited.
        final CompletableFuture<?>[] retries = new CompletableFuture<?>[4];

        CleanupWork(Throwable failure, CompletableFuture<?> inFlight) {
            while (failure instanceof CompletionException && failure.getCause() != null) {
                failure = failure.getCause();
            }
            this.failure = failure;
            this.inFlight = inFlight;
        }

        boolean complete() {
            if (failure instanceof CheckpointInputFuture.Cleanup cleanup && !cleanup.cleanupComplete()) {
                return false;
            }
            if (!settled(inFlight)) {
                return false;
            }
            for (var retry : retries) {
                if (!settled(retry)) {
                    return false;
                }
            }
            return true;
        }

        void retry() {
            int free = -1;
            for (int i = 0; i < retries.length; i++) {
                if (settled(retries[i])) {
                    free = i;
                    break;
                }
            }
            if (free < 0) {
                return;
            } // retain every unfinished original future; do not start more work
            try {
                for (int i = retries.length - 1; i >= 0; i--) {
                    if (retries[i] != null && !settled(retries[i])) {
                        retryFuture(retries[i], free);
                        return;
                    }
                }
                if (!settled(inFlight)) {
                    retryFuture(inFlight, free);
                    return;
                }
                if (failure instanceof CheckpointInputFuture.Cleanup cleanup && !cleanup.cleanupComplete()) {
                    retries[free] = Objects.requireNonNull(cleanup.retryCleanup());
                }
            } catch (RuntimeException retryFailure) {
                retries[free] = CompletableFuture.failedFuture(retryFailure);
            }
        }

        private void retryFuture(CompletableFuture<?> future, int free) {
            final var actual = futureFailure(future);
            if (actual instanceof CheckpointInputFuture.Cleanup cleanup && !cleanup.cleanupComplete()) {
                retries[free] = Objects.requireNonNull(cleanup.retryCleanup());
            } else {
                CheckpointInputFuture.requestStop(future);
                CheckpointInputFuture.retryCleanup(future);
            }
        }

        private static boolean settled(CompletableFuture<?> future) {
            if (!CheckpointInputFuture.ioExited(future)) {
                return false;
            }
            final var actual = futureFailure(future);
            return !(actual instanceof CheckpointInputFuture.Cleanup cleanup) || cleanup.cleanupComplete();
        }

        private static Throwable futureFailure(CompletableFuture<?> future) {
            if (future == null || !future.isDone()) {
                return null;
            }
            try {
                future.join();
                return null;
            } catch (RuntimeException exception) {
                Throwable actual = exception;
                while (actual instanceof CompletionException && actual.getCause() != null) {
                    actual = actual.getCause();
                }
                return actual;
            }
        }
    }
}
