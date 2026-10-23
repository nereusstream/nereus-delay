package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.PublishOutcomeBody;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetCloseBody;
import com.nereusstream.delay.protocol.TargetChannelControlBody;
import com.nereusstream.delay.protocol.TargetCloseRequest;
import com.nereusstream.delay.protocol.TargetExpireGenerationBody;
import com.nereusstream.delay.protocol.TargetHeadRef;
import com.nereusstream.delay.protocol.TargetMembershipControlBody;
import com.nereusstream.delay.protocol.TargetNativePolicyControlBody;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetPublishAdmissionBody;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CommandResult;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetClaimStore;
import com.nereusstream.delay.runtime.TargetCloseStore;
import com.nereusstream.delay.runtime.TargetCloseVerifier;
import com.nereusstream.delay.runtime.TargetCommandReplayStore;
import com.nereusstream.delay.runtime.TargetCommandStore;
import com.nereusstream.delay.runtime.TargetExpireGenerationStore;
import com.nereusstream.delay.runtime.TargetExpireGenerationVerifier;
import com.nereusstream.delay.runtime.TargetExpiryDiscoveryStore;
import com.nereusstream.delay.runtime.TargetHeadCostProbe;
import com.nereusstream.delay.runtime.TargetMembershipControlStore;
import com.nereusstream.delay.runtime.TargetMembershipControlVerifier;
import com.nereusstream.delay.runtime.TargetNativePolicyControlStore;
import com.nereusstream.delay.runtime.TargetNativePolicyControlVerifier;
import com.nereusstream.delay.runtime.TargetNativePolicyTrustStore;
import com.nereusstream.delay.runtime.TargetPublishAdmissionStore;
import com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier;
import com.nereusstream.delay.runtime.TargetPublishOutcomeStore;
import com.nereusstream.delay.runtime.TargetPublishOutcomeVerifier;
import com.nereusstream.delay.runtime.TargetPublishRecoveryDiscovery;
import com.nereusstream.delay.runtime.TargetQueueHeadCache;
import com.nereusstream.delay.runtime.TargetQueueSnapshotReader;
import com.nereusstream.delay.runtime.TargetQuotaDelta;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.runtime.TargetQuotaGrantStore;
import com.nereusstream.delay.runtime.TargetReservationControls;
import com.nereusstream.delay.runtime.TargetReservationExpiryStore;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import com.nereusstream.delay.runtime.TargetSystemReplayStore;
import com.nereusstream.delay.runtime.TargetTimeFenceStore;
import com.nereusstream.delay.runtime.TargetTimeFenceVerifier;
import com.nereusstream.delay.scheduler.SchedulerBudget;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.CheckpointManifestLimits;
import com.nereusstream.delay.store.CheckpointUploadIntentAuthority;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.ReadIncompleteException;
import com.nereusstream.delay.store.ShardStore;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.StoreMetadata;
import com.nereusstream.delay.store.TargetCheckpointCandidateWorkClassExecutor;
import com.nereusstream.delay.store.TargetCheckpointRootVerifier;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** Active Target source execution over the existing bounded poll/apply/ACK loop. */
public final class TargetSourceApplyRuntime extends SourceApplyTarget {
    public record Limits(int records, long bytes, long elapsedNanos, int counters, int domains) {
        public Limits {
            if (records <= 0
                    || bytes <= 0
                    || elapsedNanos <= 0
                    || counters < 2
                    || domains < 1
                    || domains > TargetQueueState.MAX_DOMAIN_SLOTS) {
                throw new IllegalArgumentException("Target source runtime requires finite positive limits");
            }
        }
    }

    /** First-application authority only. Immutable duplicates do not resolve current registrations or capacity. */
    public record GrantControl(
            PreparedControlOperation prepared,
            TargetQuotaGrantControlVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public GrantControl {
            Objects.requireNonNull(prepared, "prepared");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface GrantControls {
        GrantControl resolve(SourceReplayMutation entry);
    }

    public record FenceControl(TargetTimeFenceVerifier.Authority authority, TargetStoreBackend.CommitAuthority commit) {
        public FenceControl {
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface Fences {
        FenceControl resolve(SourceReplayMutation entry);
    }

    public record ExpiryControl(
            TargetExpireGenerationVerifier.Authority authority, TargetStoreBackend.CommitAuthority commit) {
        public ExpiryControl {
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface ExpiryControls {
        ExpiryControl resolve(SourceReplayMutation entry);
    }

    public record CloseControl(
            PreparedControlOperation prepared,
            TargetCloseVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public CloseControl {
            Objects.requireNonNull(prepared, "prepared");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface Closes {
        CloseControl resolve(SourceReplayMutation entry);
    }

    public record MembershipControl(
            PreparedControlOperation prepared,
            CanonicalTargetPartition physical,
            TargetMembershipControlVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public MembershipControl {
            Objects.requireNonNull(prepared, "prepared");
            Objects.requireNonNull(physical, "physical");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface MembershipControls {
        MembershipControl resolve(SourceReplayMutation entry);
    }

    public record NativePolicyControl(
            PreparedControlOperation prepared,
            TargetNativePolicyControlVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public NativePolicyControl {
            Objects.requireNonNull(prepared, "prepared");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface NativePolicyControls {
        NativePolicyControl resolve(SourceReplayMutation entry);
    }

    public record AdmissionControl(
            TargetPublishAdmissionVerifier.Authority authority,com.nereusstream.delay.runtime.TargetSloObservationSamples.Authority samples,
            TargetStoreBackend.CommitAuthority commit) {
        public AdmissionControl(TargetPublishAdmissionVerifier.Authority authority,TargetStoreBackend.CommitAuthority commit){
            this(authority,com.nereusstream.delay.runtime.TargetSloObservationSamples.UNAVAILABLE,commit);
        }
        public AdmissionControl {
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(samples,"samples");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface TargetAdmissions {
        AdmissionControl resolve(SourceReplayMutation entry);
    }

    public record OutcomeControl(
            TargetPublishOutcomeVerifier.Authority authority, TargetStoreBackend.CommitAuthority commit) {
        public OutcomeControl {
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface TargetOutcomes {
        OutcomeControl resolve(SourceReplayMutation entry);
    }

    public record RetirementControl(
            com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public RetirementControl { Objects.requireNonNull(authority); Objects.requireNonNull(commit); }
    }

    @FunctionalInterface
    public interface TargetRetirements {
        RetirementControl resolve(SourceReplayMutation entry);
    }
    public record ReclaimControl(com.nereusstream.delay.runtime.TargetResourceReclaimStore.Authority authority,
            TargetStoreBackend.CommitAuthority commit){
        public ReclaimControl{Objects.requireNonNull(authority);Objects.requireNonNull(commit);}
    }
    @FunctionalInterface public interface TargetReclaims{ReclaimControl resolve(SourceReplayMutation entry);}
    public record ProtocolActivationControl(PreparedControlOperation prepared,
            com.nereusstream.delay.runtime.TargetProtocolActivationStore.Authority authority,TargetStoreBackend.CommitAuthority commit){
        public ProtocolActivationControl{Objects.requireNonNull(prepared);Objects.requireNonNull(authority);Objects.requireNonNull(commit);}
    }
    @FunctionalInterface public interface ProtocolActivations{ProtocolActivationControl resolve(SourceReplayMutation entry);}

    public record SloObservationControl(PreparedControlOperation prepared,
            com.nereusstream.delay.runtime.TargetSloObservationVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public SloObservationControl { Objects.requireNonNull(prepared); Objects.requireNonNull(authority); Objects.requireNonNull(commit); }
    }
    @FunctionalInterface public interface SloObservationControls { SloObservationControl resolve(SourceReplayMutation entry); }

    public record ChannelControl(PreparedControlOperation prepared,
            com.nereusstream.delay.runtime.TargetChannelControlVerifier.Authority authority,
            TargetStoreBackend.CommitAuthority commit) {
        public ChannelControl {
            Objects.requireNonNull(prepared, "prepared");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface ChannelControls {
        ChannelControl resolve(SourceReplayMutation entry);
    }

    public record CommandControl(
            TargetCommandStore.Policy policy,
            TargetCommandStore.CancellationControls cancellations,
            com.nereusstream.delay.runtime.TargetReservationControls.Authority reservationControls,
            TargetCommandStore.Schedules schedules,
            TargetCommandStore.PayloadProofControls payloadProofs,
            com.nereusstream.delay.runtime.TargetSloObservationSamples.Authority samples,
            TargetStoreBackend.CommitAuthority commit) {
        public CommandControl(TargetCommandStore.Policy policy, TargetCommandStore.CancellationControls cancellations,
                TargetReservationControls.Authority reservations, TargetCommandStore.Schedules schedules,
                TargetCommandStore.PayloadProofControls payloadProofs, TargetStoreBackend.CommitAuthority commit) {
            this(policy,cancellations,reservations,schedules,payloadProofs,com.nereusstream.delay.runtime.TargetSloObservationSamples.UNAVAILABLE,commit);
        }
        public CommandControl {
            Objects.requireNonNull(policy, "policy");
            Objects.requireNonNull(cancellations, "cancellations");
            Objects.requireNonNull(reservationControls, "reservationControls");
            Objects.requireNonNull(schedules, "schedules");
            Objects.requireNonNull(payloadProofs, "payloadProofs");
            Objects.requireNonNull(samples, "samples");
            Objects.requireNonNull(commit, "commit");
        }
    }

    @FunctionalInterface
    public interface Commands {
        CommandControl resolve(SourceReplayRecord entry);
    }

    /** External resource/Control snapshots remain mandatory; local ownership checks cannot substitute for them. */
    public record Authorities(
            OxiaOwnerLeaseStore leases,
            SourceReplaySuccessor successor,
            GrantControls grants,
            Fences fences,
            ExpiryControls expiries,
            Closes closes,
            MembershipControls membershipControls,
            TargetStoreBackend.CommitAuthority duplicateWrites,
            TargetStoreBackend.ReadAuthority reads,
            Commands commands,
            NativePolicyControls nativePolicyControls,
            TargetAdmissions admissions,
            TargetOutcomes outcomes,
            ChannelControls channelControls,
            TargetRetirements retirements,ProtocolActivations protocolActivations,TargetReclaims reclaims,
            SloObservationControls sloObservations) {
        public Authorities(OxiaOwnerLeaseStore leases, SourceReplaySuccessor successor, GrantControls grants,
                Fences fences, ExpiryControls expiries, Closes closes, MembershipControls memberships,
                TargetStoreBackend.CommitAuthority writes, TargetStoreBackend.ReadAuthority reads, Commands commands,
                NativePolicyControls nativePolicies, TargetAdmissions admissions, TargetOutcomes outcomes,
                ChannelControls channels, TargetRetirements retirements, ProtocolActivations activations, TargetReclaims reclaims) {
            this(leases, successor, grants, fences, expiries, closes, memberships, writes, reads, commands,
                    nativePolicies, admissions, outcomes, channels, retirements, activations, reclaims,
                    entry -> { throw new TargetSourceAuthorityPreparation.Required(); });
        }

        public Authorities(OxiaOwnerLeaseStore leases,SourceReplaySuccessor successor,GrantControls grants,Fences fences,
                ExpiryControls expiries,Closes closes,MembershipControls memberships,TargetStoreBackend.CommitAuthority writes,
                TargetStoreBackend.ReadAuthority reads,Commands commands,NativePolicyControls nativePolicies,TargetAdmissions admissions,
                TargetOutcomes outcomes,ChannelControls channels,TargetRetirements retirements,ProtocolActivations activations){
            this(leases,successor,grants,fences,expiries,closes,memberships,writes,reads,commands,nativePolicies,admissions,outcomes,
                    channels,retirements,activations,entry->{throw new TargetSourceAuthorityPreparation.Required();});
        }
        public Authorities(OxiaOwnerLeaseStore leases,SourceReplaySuccessor successor,GrantControls grants,Fences fences,
                ExpiryControls expiries,Closes closes,MembershipControls memberships,TargetStoreBackend.CommitAuthority writes,
                TargetStoreBackend.ReadAuthority reads,Commands commands,NativePolicyControls nativePolicies,TargetAdmissions admissions,
                TargetOutcomes outcomes,ChannelControls channels,TargetRetirements retirements){
            this(leases,successor,grants,fences,expiries,closes,memberships,writes,reads,commands,nativePolicies,admissions,outcomes,channels,retirements,
                    entry->{throw new IllegalStateException("Target protocol activation authorities are not configured");});
        }
        public Authorities(OxiaOwnerLeaseStore leases, SourceReplaySuccessor successor, GrantControls grants,
                Fences fences, ExpiryControls expiries, Closes closes, MembershipControls membershipControls,
                TargetStoreBackend.CommitAuthority duplicateWrites, TargetStoreBackend.ReadAuthority reads,
                Commands commands, NativePolicyControls nativePolicyControls, TargetAdmissions admissions,
                TargetOutcomes outcomes, ChannelControls channelControls) {
            this(leases, successor, grants, fences, expiries, closes, membershipControls, duplicateWrites, reads,
                    commands, nativePolicyControls, admissions, outcomes, channelControls, entry -> {
                        throw new IllegalStateException("Target evidence retirement authorities are not configured");
                    });
        }
        public Authorities(OxiaOwnerLeaseStore leases, SourceReplaySuccessor successor, GrantControls grants,
                Fences fences, ExpiryControls expiries, Closes closes, MembershipControls membershipControls,
                TargetStoreBackend.CommitAuthority duplicateWrites, TargetStoreBackend.ReadAuthority reads,
                Commands commands, NativePolicyControls nativePolicyControls, TargetAdmissions admissions,
                TargetOutcomes outcomes) {
            this(leases, successor, grants, fences, expiries, closes, membershipControls, duplicateWrites, reads,
                    commands, nativePolicyControls, admissions, outcomes, entry -> {
                        throw new IllegalStateException("Target channel lifecycle authorities are not configured");
                    });
        }

        public Authorities withChannelControls(final ChannelControls selected) {
            return new Authorities(leases, successor, grants, fences, expiries, closes, membershipControls,
                    duplicateWrites, reads, commands, nativePolicyControls, admissions, outcomes,
                    Objects.requireNonNull(selected, "channelControls"), retirements,protocolActivations,reclaims,sloObservations);
        }

        public Authorities withRetirements(final TargetRetirements selected) {
            return new Authorities(leases, successor, grants, fences, expiries, closes, membershipControls,
                    duplicateWrites, reads, commands, nativePolicyControls, admissions, outcomes,
                    channelControls, Objects.requireNonNull(selected),protocolActivations,reclaims,sloObservations);
        }
        public Authorities withProtocolActivations(ProtocolActivations selected){
            return new Authorities(leases,successor,grants,fences,expiries,closes,membershipControls,duplicateWrites,reads,commands,
                    nativePolicyControls,admissions,outcomes,channelControls,retirements,Objects.requireNonNull(selected),reclaims,sloObservations);
        }

        public Authorities withSuccessor(final SourceReplaySuccessor selected) {
            return new Authorities(leases, Objects.requireNonNull(selected), grants, fences, expiries, closes,
                    membershipControls, duplicateWrites, reads, commands, nativePolicyControls, admissions,
                    outcomes, channelControls, retirements,protocolActivations,reclaims,sloObservations);
        }
        public Authorities withSloObservations(SloObservationControls selected) {
            return new Authorities(leases, successor, grants, fences, expiries, closes, membershipControls,
                    duplicateWrites, reads, commands, nativePolicyControls, admissions, outcomes, channelControls,
                    retirements, protocolActivations, reclaims, Objects.requireNonNull(selected));
        }
        public Authorities(
                final OxiaOwnerLeaseStore leases,
                final SourceReplaySuccessor successor,
                final GrantControls grants,
                final Fences fences,
                final ExpiryControls expiries,
                final Closes closes,
                final MembershipControls membershipControls,
                final TargetStoreBackend.CommitAuthority duplicateWrites,
                final TargetStoreBackend.ReadAuthority reads,
                final Commands commands) {
            this(
                    leases,
                    successor,
                    grants,
                    fences,
                    expiries,
                    closes,
                    membershipControls,
                    duplicateWrites,
                    reads,
                    commands,
                    entry -> {
                        throw new IllegalStateException("Target Native policy Control authorities are not configured");
                    },
                    entry -> {
                        throw new IllegalStateException("Target Publish Admission authorities are not configured");
                    },
                    entry -> {
                        throw new IllegalStateException("Target Publish Outcome authorities are not configured");
                    });
        }

        public Authorities(
                final OxiaOwnerLeaseStore leases,
                final SourceReplaySuccessor successor,
                final GrantControls grants,
                final Fences fences,
                final ExpiryControls expiries,
                final Closes closes,
                final MembershipControls membershipControls,
                final TargetStoreBackend.CommitAuthority duplicateWrites,
                final TargetStoreBackend.ReadAuthority reads,
                final Commands commands,
                final NativePolicyControls nativePolicyControls) {
            this(
                    leases,
                    successor,
                    grants,
                    fences,
                    expiries,
                    closes,
                    membershipControls,
                    duplicateWrites,
                    reads,
                    commands,
                    nativePolicyControls,
                    entry -> {
                        throw new IllegalStateException("Target Publish Admission authorities are not configured");
                    },
                    entry -> {
                        throw new IllegalStateException("Target Publish Outcome authorities are not configured");
                    });
        }

        public Authorities(
                final OxiaOwnerLeaseStore leases,
                final SourceReplaySuccessor successor,
                final GrantControls grants,
                final Fences fences,
                final ExpiryControls expiries,
                final Closes closes,
                final MembershipControls membershipControls,
                final TargetStoreBackend.CommitAuthority duplicateWrites,
                final TargetStoreBackend.ReadAuthority reads,
                final Commands commands,
                final NativePolicyControls nativePolicyControls,
                final TargetAdmissions admissions) {
            this(
                    leases,
                    successor,
                    grants,
                    fences,
                    expiries,
                    closes,
                    membershipControls,
                    duplicateWrites,
                    reads,
                    commands,
                    nativePolicyControls,
                    admissions,
                    entry -> {
                        throw new IllegalStateException("Target Publish Outcome authorities are not configured");
                    });
        }

        public Authorities {
            Objects.requireNonNull(protocolActivations,"protocolActivations");
            Objects.requireNonNull(leases, "leases");
            Objects.requireNonNull(successor, "successor");
            Objects.requireNonNull(grants, "grants");
            Objects.requireNonNull(fences, "fences");
            Objects.requireNonNull(expiries, "expiries");
            Objects.requireNonNull(closes, "closes");
            Objects.requireNonNull(membershipControls, "membershipControls");
            Objects.requireNonNull(duplicateWrites, "duplicateWrites");
            Objects.requireNonNull(reads, "reads");
            Objects.requireNonNull(commands, "commands");
            Objects.requireNonNull(nativePolicyControls, "nativePolicyControls");
            Objects.requireNonNull(admissions, "admissions");
            Objects.requireNonNull(outcomes, "outcomes");
            Objects.requireNonNull(channelControls, "channelControls");
            Objects.requireNonNull(retirements, "retirements");
            Objects.requireNonNull(protocolActivations,"protocolActivations");
            Objects.requireNonNull(reclaims,"reclaims");
            Objects.requireNonNull(sloObservations, "sloObservations");
        }
    }

    private final ShardStore store;
    private final TargetStoreBackend backend;
    private final StoreMetadata metadata;
    private final TargetQuotaScope scope;
    private final byte[] lineage;
    private final TargetQuotaGrantStore grants;
    private final TargetPublishAdmissionStore targetAdmissions;
    private final TargetPublishRecoveryDiscovery publishRecoveryDiscovery;
    private final com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery reclaimDiscovery;
    private final TargetPublishOutcomeStore targetOutcomes;
    private final TargetTimeFenceStore fences;
    private final TargetExpiryDiscoveryStore expiryDiscovery;
    private final TargetExpireGenerationStore expiries;
    private final TargetCloseStore closes;
    private final TargetMembershipControlStore membershipControls;
    private final TargetNativePolicyControlStore nativePolicyControls;
    private final com.nereusstream.delay.runtime.TargetChannelControlStore channelControls;
    private final TargetSystemReplayStore replay;
    private final TargetCommandReplayStore commandReplay;
    private final TargetCommandStore commands;
    private final TargetQueueHeadCache targetQueueHeadCache = new TargetQueueHeadCache();
    private final SourceAssignment assignment;
    private final Authorities authorities;
    private final Limits limits;
    private final LongSupplier monotonicClock;
    private final boolean recoveryOnly;
    private final Runnable recoveryProtection;
    // Store-read guards revalidate ownership while holding the Store monitor; keep this lock free of Store access.
    private final Object ownerStateLock = new Object();
    private WorkClassExecutionRegistry workClasses;
    private TargetReservationGcRuntime maintenanceRuntime;
    private volatile TargetResourceReclaimMaintenance resourceReclaimMaintenance;
    void registerResourceReclaimMaintenance(TargetResourceReclaimMaintenance original){
        if(resourceReclaimMaintenance!=null)throw new IllegalStateException("Source already has its original Native GC owner");
        resourceReclaimMaintenance=Objects.requireNonNull(original);
    }
    private volatile OwnerLease lease;
    private volatile boolean fenced;
    private long lastOwnerTime = -1;
    private volatile TargetSourceAuthorityPreparation authorityPreparation;
    private boolean authorityRefresh;
    private SourceReplaySuccessor activeSourceSuccessor;
    private boolean sourceSubmissionStarted;
    private TargetWorkerRecoverySource recoveryReader;
    private SourcePosition recoveryReaderCut;

    synchronized void bindRecoveryReader(TargetWorkerRecoverySource source, SourcePosition cut) {
        if (!recoveryOnly || fenced || sourceSubmissionStarted || recoveryReader != null) {
            throw new IllegalStateException("independent reader binds only before original Target recovery work");
        }
        source.requireReaderBinding(assignment, cut);
        recoveryReader = source;
        recoveryReaderCut = Objects.requireNonNull(cut);
    }

    synchronized void bindActiveSourceSuccessor(java.util.function.Supplier<SourceReplaySuccessor> actual) {
        if (recoveryOnly || fenced || sourceSubmissionStarted || activeSourceSuccessor != null) {
            throw new IllegalStateException("active Source successor must bind once before original Source work");
        }
        activeSourceSuccessor = Objects.requireNonNull(Objects.requireNonNull(actual).get());
    }

    /** Installs the root's complete content/history graph before this runtime can submit Source work. */
    public synchronized void configureAuthorityPreparation(TargetSourceAuthorityPreparation selected, LongSupplier clock) {
        configureAuthorityPreparation(selected,clock,null);
    }
    private volatile com.nereusstream.delay.store.RecoveryCatalogAuthority originalEvidenceCatalog;
    public synchronized void configureAuthorityPreparation(TargetSourceAuthorityPreparation selected,LongSupplier clock,
            com.nereusstream.delay.store.RecoveryCatalogAuthority catalog){
        if (authorityPreparation != null || fenced) throw new IllegalStateException("Source preparation is already configured or fenced");
        Objects.requireNonNull(clock, "ownerClock");
        Objects.requireNonNull(selected).bind(authorities, () -> {
            if (recoveryOnly) recoveryProtection.run();
            synchronized (ownerStateLock) {
                final long now = clock.getAsLong();
                final boolean wrongState = recoveryOnly
                        ? lease.state() != ShardLifecycleState.ACQUIRING
                            && lease.state() != ShardLifecycleState.CATCHING_UP
                            && lease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                        : lease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS;
                if (fenced || store.isCloseStarted() || now < lastOwnerTime || !lease.validAt(now)
                        || store.runtimeMetadata().lastOpenedOwnerEpoch() != lease.ownerEpoch() || wrongState) {
                    throw new IllegalStateException("Source preparation lost its original local Owner/Store");
                }
            }
        },this,clock);
        originalEvidenceCatalog=catalog;
        authorityPreparation = selected;
    }
    synchronized void bindOriginalEvidenceCatalog(com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend catalog){
        if(originalEvidenceCatalog!=null&&originalEvidenceCatalog!=catalog){
            if(!(originalEvidenceCatalog instanceof com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend previous))
                throw new IllegalStateException("original Source recovery publication authority is not managed");
            previous.requireSameOriginalEvidenceAuthority(catalog);
        }else originalEvidenceCatalog=Objects.requireNonNull(catalog);
    }
    ConfirmationRead confirmationRead(SourceReplayEntry entry,LongSupplier clock){
        return entry instanceof SourceReplayMutation mutation&&mutation.mutation().type()==SystemMutationType.RESOURCE_DELETE_CONFIRMED
                ?new ConfirmationRead(this,mutation,Objects.requireNonNull(clock)):null;
    }
    ReclaimRead reclaimRead(SourceReplayEntry entry,LongSupplier clock){
        return entry instanceof SourceReplayMutation mutation&&mutation.mutation().type()==SystemMutationType.TARGET_RESOURCE_RECLAIM
                ?new ReclaimRead(this,mutation,Objects.requireNonNull(clock)):null;
    }
    /** Privately minted actual Source/Store/Owner context, including cold replay under a successor Owner. */
    public static final class ReclaimRead {
        private final TargetSourceApplyRuntime runtime;
        private final SourceReplayMutation entry;
        private final LongSupplier clock;
        private ReclaimRead(TargetSourceApplyRuntime runtime,SourceReplayMutation entry,LongSupplier clock){this.runtime=runtime;this.entry=entry;this.clock=clock;}
        public SourceReplayMutation entry(){requireCurrent();return entry;}
        public void requireBackend(TargetStoreBackend backend){requireCurrent();if(runtime.backend!=backend)throw new IllegalArgumentException("reclaim changed original Source Native Store");}
        public com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend catalog(){
            requireCurrent();
            if(!(runtime.originalEvidenceCatalog instanceof com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend actual))
                throw new TargetSourceAuthorityPreparation.InputUnavailable("original Native reclaim publication Catalog is unavailable");
            return actual;
        }
        public void requireCurrent(){
            runtime.authorityPreparation.requireOriginalEntry(entry);
            synchronized(runtime.ownerStateLock){
                final long now=clock.getAsLong();final var lease=runtime.lease;
                if(runtime.fenced||runtime.store.isCloseStarted()||now<runtime.lastOwnerTime||!lease.validAt(now)
                        ||runtime.store.runtimeMetadata().lastOpenedOwnerEpoch()!=lease.ownerEpoch()
                        ||lease.state()!=(runtime.recoveryOnly?ShardLifecycleState.CATCHING_UP:ShardLifecycleState.ACTIVE_FOR_COMMANDS))
                    throw new IllegalStateException("original reclaim Source Owner/Store is no longer held");
            }
        }
        public com.nereusstream.delay.runtime.TargetResourceReclaimStore.Protection protectUnused(
                java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,long maximumBytes,Runnable originalHeld){
            requireCurrent();
            if(com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(entry.mutation().canonicalBody()).inlinePayloadRelease()
                    ||com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(entry.mutation().canonicalBody()).nativeRetainedReclaim())
                throw new IllegalArgumentException("unused protection entry cannot select inline capability");
            return protectReclaim(images,maximumBytes,originalHeld);
        }
        public com.nereusstream.delay.runtime.TargetResourceReclaimStore.Protection protectReclaim(
                java.util.List<com.nereusstream.delay.store.TargetCheckpointRootVerifier.AuthenticatedImage> images,long maximumBytes,Runnable originalHeld){
            requireCurrent();
            return com.nereusstream.delay.runtime.TargetResourceReclaimStore.Protection.collect(this,images,
                    new BoundedReadBudget(runtime.limits.records(),runtime.limits.bytes(),runtime.limits.elapsedNanos(),runtime.monotonicClock),maximumBytes,originalHeld);
        }
        public boolean matchesExpected(){
            return readNative(reader->com.nereusstream.delay.runtime.TargetResourceReclaimStore.matchesExpected(reader,
                    com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(entry.mutation().canonicalBody())));
        }
        /** The original Native prefix must have replayed a newer Floor that still precedes this selected event. */
        public void requireReplayedFloor(com.nereusstream.delay.protocol.RecoveryFloorRef floor){
            requireCurrent();
            if(!Arrays.equals(floor.recoveryLineageId(),runtime.lineage)||!floor.appliedSourcePosition().shardId().equals(runtime.scope.shard())
                    ||floor.appliedSourcePosition().compareTo(entry.position())>=0)throw new IllegalArgumentException("replay Floor is not before the original Source event");
            readNative(reader->{
                if(reader.source()==null||floor.appliedSourcePosition().compareTo(reader.source())>0
                        ||floor.appliedSourcePosition().compareTo(reader.source())==0&&!floor.appliedSourcePosition().equals(reader.source())
                        ||Long.compareUnsigned(floor.includedMutationSequence(),reader.sourceSequence())>0)
                    throw new TargetSourceAuthorityPreparation.InputUnavailable("original Source prefix has not replayed the advanced Floor");
                return Boolean.TRUE;
            });
        }
        public void requireClosedUnused(com.nereusstream.delay.runtime.TargetResourceReclaimStore.Protection protection){
            readNative(reader->{
                com.nereusstream.delay.runtime.TargetResourceReclaimStore.requireClosedUnused(reader,runtime.scope,runtime.lineage,entry.mutation(),entry.position(),protection);
                return Boolean.TRUE;
            });
        }
        public void requireClosedInline(com.nereusstream.delay.runtime.TargetResourceReclaimStore.Protection protection){
            readNative(reader->{com.nereusstream.delay.runtime.TargetResourceReclaimStore.requireClosedInline(reader,runtime.scope,runtime.lineage,entry.mutation(),entry.position(),protection);return Boolean.TRUE;});
        }
        public java.util.Optional<com.nereusstream.delay.adapter.PayloadUncommittedDelete.Observation> uncommittedDeletion(){
            requireCurrent();final var original=runtime.resourceReclaimMaintenance;
            return original==null?java.util.Optional.empty():original.uncommittedDeletionFor(entry.mutation());
        }
        public java.util.Optional<com.nereusstream.delay.adapter.PayloadObjectDelete.Observation> objectDeletion(){
            requireCurrent();final var original=runtime.resourceReclaimMaintenance;
            return original==null?java.util.Optional.empty():original.objectDeletionFor(entry.mutation());
        }
        public void requireClosedObject(com.nereusstream.delay.runtime.TargetResourceReclaimStore.Protection protection){
            readNative(reader->{com.nereusstream.delay.runtime.TargetResourceReclaimStore.requireClosedObject(reader,runtime.scope,runtime.lineage,entry.mutation(),entry.position(),protection);return Boolean.TRUE;});
        }
        public void requireClosedNative(com.nereusstream.delay.runtime.TargetResourceReclaimStore.Protection protection){
            readNative(reader->{com.nereusstream.delay.runtime.TargetResourceReclaimStore.requireClosedNative(reader,runtime.scope,runtime.lineage,entry.mutation(),entry.position(),protection);return Boolean.TRUE;});
        }
        private <T> T readNative(java.util.function.Function<TargetStoreBackend.Reader,T> read){
            synchronized(runtime){
                requireCurrent();runtime.requireOwner(clock);
                final TargetStoreBackend.ReadAuthority authority=(metadata,scope)->{
                    requireCurrent();final var original=runtime.authorities.reads().acquire(metadata,scope);
                    return new TargetStoreBackend.CommitGuard(){
                        public void requireCurrent(){ReclaimRead.this.requireCurrent();original.requireCurrent();}
                        public void close(){original.close();}
                    };
                };
                final var result=runtime.backend.guardedRead(new BoundedReadBudget(runtime.limits.records(),runtime.limits.bytes(),runtime.limits.elapsedNanos(),runtime.monotonicClock),read,authority);
                runtime.requireOwner(clock);requireCurrent();return result;
            }
        }
    }
    /** Actual selected Source/recovery input, minted only by its runtime and owning preparation task. */
    public static final class ConfirmationRead {
        private final TargetSourceApplyRuntime runtime;
        private final SourceReplayMutation entry;
        private final LongSupplier clock;
        private ConfirmationRead(TargetSourceApplyRuntime runtime,SourceReplayMutation entry,LongSupplier clock){this.runtime=runtime;this.entry=entry;this.clock=clock;}
        public SourceReplayMutation entry(){requireCurrent();return entry;}
        public void requireResources(com.nereusstream.delay.store.SharedRocksDbResources resources){
            requireCurrent();if(runtime.store.sharedResources()!=resources)throw new IllegalArgumentException("historical confirmation uses another original process Store");
        }
        public com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend catalog(){
            requireCurrent();final var actual=runtime.originalEvidenceCatalog;
            if(!(actual instanceof com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend managed))
                throw new TargetSourceAuthorityPreparation.InputUnavailable("original Source checkpoint publication configuration is unavailable");
            return managed;
        }
        /** Local held checks only; safe at Native commit. Actual lease I/O is confined to readIntent. */
        public void requireCurrent(){
            runtime.authorityPreparation.requireOriginalEntry(entry);
            synchronized(runtime.ownerStateLock){
                final long now=clock.getAsLong();final var lease=runtime.lease;
                if(runtime.fenced||runtime.store.isCloseStarted()||now<runtime.lastOwnerTime||!lease.validAt(now)
                        ||runtime.store.runtimeMetadata().lastOpenedOwnerEpoch()!=lease.ownerEpoch()
                        ||lease.state()!=(runtime.recoveryOnly?ShardLifecycleState.CATCHING_UP:ShardLifecycleState.ACTIVE_FOR_COMMANDS))
                    throw new IllegalStateException("original historical Source Owner/Store is no longer held");
            }
        }
        public com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent readIntent(BoundedReadBudget reads,
                com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.SourceHistory history,TargetRetirementAuthorizationHistory.Grant grant){
            synchronized(runtime){return readOriginalIntent(reads,history,grant);}
        }
        private com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent readOriginalIntent(BoundedReadBudget reads,
                com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.SourceHistory history,TargetRetirementAuthorizationHistory.Grant grant){
            requireCurrent();runtime.requireOwner(clock);
            final TargetStoreBackend.ReadAuthority authority=(actual,scope)->{
                requireCurrent();final var original=runtime.authorities.reads().acquire(actual,scope);
                return new TargetStoreBackend.CommitGuard(){
                    public void requireCurrent(){ConfirmationRead.this.requireCurrent();original.requireCurrent();}
                    public void close(){original.close();}
                };
            };
            final var result=com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.readHistoricalConfirmationIntent(
                    runtime.backend,reads,runtime.scope,runtime.lineage,entry.mutation(),entry.position(),history,grant,authority);
            runtime.requireOwner(clock);requireCurrent();return result;
        }
    }

    boolean hasAuthorityPreparation() { return authorityPreparation != null; }

    void stopAuthorityPreparation() { final var selected = authorityPreparation; if (selected != null) selected.requestStop(); }
    void retryAuthorityCleanup() { final var selected = authorityPreparation; if (selected != null) selected.retryCleanup(); }
    boolean authorityIoExited() { final var selected = authorityPreparation; return selected == null || selected.ioExited(); }
    Throwable authorityIoFailure() { final var selected = authorityPreparation; return selected == null ? null : selected.pendingIoFailure(); }

    @Override synchronized boolean prepareAuthorities(SourceReplayEntry entry, LongSupplier clock, boolean recovery) {
        final var selected = authorityPreparation;
        if (selected == null) return true;
        requireSubmission(entry, recovery); requireOwner(clock);
        if (authorityRefresh) {
            if (!selected.complete(entry)) return false;
            authorityRefresh = false;
        }
        final var budget = new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock);
        try {
            if (entry instanceof SourceReplayRecord command) {
                if (commandReplay.prepareReplayOrExpired(budget, command.command(), entry.position()).isPresent()) return true;
            } else {
                final var mutation = ((SourceReplayMutation) entry).mutation();
                if (replay.prepareIfPresent(budget, mutation, entry.position()).isPresent()) return true;
                if(mutation.type()==SystemMutationType.TARGET_RESOURCE_RECLAIM
                        &&!new com.nereusstream.delay.runtime.TargetResourceReclaimStore(backend,scope,lineage,limits.counters(),limits.domains())
                                .requiresInputs(budget,mutation,entry.position()))return true;
                if (mutation.type() == SystemMutationType.RESOURCE_DELETE_CONFIRMED
                        && !new com.nereusstream.delay.runtime.TargetEvidenceRetirementStore(
                                backend, scope, lineage, limits.counters(), limits.domains())
                                .requiresConfirmationInputs(budget, mutation, entry.position())) return true;
            }
        } catch (ReadIncompleteException incomplete) { throw new ReadYield(incomplete); }
        return selected.prepare(entry, contentFamily(entry));
    }

    @Override boolean completeAuthorities(SourceReplayEntry entry) {
        final var selected = authorityPreparation;
        return selected == null || selected.complete(entry);
    }

    @Override boolean isAuthorityPreparationRequired(Throwable failure) {
        return failure instanceof TargetSourceAuthorityPreparation.Required
                || failure instanceof com.nereusstream.delay.runtime.TargetQuotaCapacitySnapshot.ViewChanged;
    }

    private static TargetSourceAuthorityPreparation.Family contentFamily(SourceReplayEntry entry) {
        if (entry instanceof SourceReplayRecord) return TargetSourceAuthorityPreparation.Family.COMMAND;
        final var mutation = ((SourceReplayMutation) entry).mutation();
        if (mutation.type() == SystemMutationType.TIME_FENCE) return TargetSourceAuthorityPreparation.Family.FENCE;
        if (mutation.type() == SystemMutationType.EXPIRE_GENERATION) return TargetSourceAuthorityPreparation.Family.EXPIRY;
        if (mutation.type() == SystemMutationType.TARGET_PUBLISH_ADMISSION) return TargetSourceAuthorityPreparation.Family.ADMISSION;
        if (mutation.type() == SystemMutationType.PUBLISH_OUTCOME || mutation.type() == SystemMutationType.EVIDENCE_RESOLUTION) return TargetSourceAuthorityPreparation.Family.OUTCOME;
        if (mutation.type() == SystemMutationType.RESOURCE_RETIRE_INTENT||mutation.type()==SystemMutationType.RESOURCE_DELETE_CONFIRMED) return TargetSourceAuthorityPreparation.Family.RETIREMENT;
        if(mutation.type()==SystemMutationType.TARGET_RESOURCE_RECLAIM)return TargetSourceAuthorityPreparation.Family.RECLAIM;
        if (isProtocolActivation(mutation)) return TargetSourceAuthorityPreparation.Family.ACTIVATION;
        if (isSloObservationControl(mutation)) return TargetSourceAuthorityPreparation.Family.SLO_MODE;
        if (isChannelControl(mutation)) return TargetSourceAuthorityPreparation.Family.CHANNEL;
        if (isTargetClose(mutation)) return TargetSourceAuthorityPreparation.Family.CLOSE;
        if (isMembershipControl(mutation)) return TargetSourceAuthorityPreparation.Family.MEMBERSHIP;
        if (isNativePolicyControl(mutation)) return TargetSourceAuthorityPreparation.Family.NATIVE_POLICY;
        return TargetSourceAuthorityPreparation.Family.GRANT;
    }

    /** Requires prior explicit activation, including persisted Owner epoch and the reached source barrier. */
    public TargetSourceApplyRuntime(
            final TargetStoreBootstrap.Initialized initialized,
            final ShardStore store,
            final SourceAssignment assignment,
            final OwnerLease lease,
            final Authorities authorities,
            final Limits limits,
            final LongSupplier monotonicClock) {
        this(
                Objects.requireNonNull(initialized, "initialized").backend(),
                initialized.root(),
                store,
                assignment,
                lease,
                authorities,
                limits,
                monotonicClock, false, () -> {});
    }

    /** Recovery composition uses only the root reconstructed from the persisted fixed Shard anchor. */
    public TargetSourceApplyRuntime(
            final TargetStoreBootstrap.Reopened reopened,
            final ShardStore store,
            final SourceAssignment assignment,
            final OwnerLease lease,
            final Authorities authorities,
            final Limits limits,
            final LongSupplier monotonicClock) {
        this(
                Objects.requireNonNull(reopened, "reopened").backend(),
                reopened.root(),
                store,
                assignment,
                lease,
                authorities,
                limits,
                monotonicClock, false, () -> {});
    }

    /** Internal replay target; never exposed as an active Worker, Publisher, GC or ACK source runtime. */
    static TargetSourceApplyRuntime recovery(TargetStoreBootstrap.Reopened reopened, ShardStore store,
            SourceAssignment assignment, OwnerLease lease, Authorities authorities, Limits limits,
            LongSupplier monotonicClock, Runnable protection) {
        Objects.requireNonNull(reopened, "reopened");
        return new TargetSourceApplyRuntime(reopened.backend(), reopened.root(), store, assignment, lease,
                authorities, limits, monotonicClock, true, Objects.requireNonNull(protection, "protection"));
    }

    private TargetSourceApplyRuntime(
            final TargetStoreBackend openedBackend,
            final TargetQuotaIncarnation root,
            final ShardStore store,
            final SourceAssignment assignment,
            final OwnerLease lease,
            final Authorities authorities,
            final Limits limits,
            final LongSupplier monotonicClock,
            final boolean recoveryOnly,
            final Runnable recoveryProtection) {
        this.store = Objects.requireNonNull(store, "store");
        backend = Objects.requireNonNull(openedBackend, "backend");
        backend.requireStore(store);
        metadata = store.metadata();
        final var exactRoot = Objects.requireNonNull(root, "root");
        scope = exactRoot.scope();
        this.assignment = Objects.requireNonNull(assignment, "assignment");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.authorities = Objects.requireNonNull(authorities, "authorities");
        this.limits = Objects.requireNonNull(limits, "limits");
        this.monotonicClock = Objects.requireNonNull(monotonicClock, "monotonicClock");
        this.recoveryOnly = recoveryOnly;
        this.recoveryProtection = recoveryProtection;
        final var install = store.recoveryMetadata().installState();
        if (!recoveryOnly && install != null
                && (install.phase() == com.nereusstream.delay.protocol.RecoveryInstallPhase.INSTALLED
                        || install.phase() == com.nereusstream.delay.protocol.RecoveryInstallPhase.STAGED)) {
            throw new IllegalStateException("Target active source requires completed checkpoint recovery");
        }
        if (!scope.shard().equals(lease.shardId())
                || !scope.shard().equals(assignment.shardId())
                || lease.context() == null
                || !Arrays.equals(lease.sourceAssignmentId(), assignment.assignmentId())
                || lease.sourceAssignmentEpoch() != assignment.assignmentEpoch()
                || lease.state() != (recoveryOnly ? ShardLifecycleState.CATCHING_UP
                        : ShardLifecycleState.ACTIVE_FOR_COMMANDS)
                || store.runtimeMetadata().lastOpenedOwnerEpoch() != lease.ownerEpoch()
                || !recoveryOnly && !assignment.activationBarrier().reachedBy(store.appliedShardLogPosition())) {
            throw new IllegalArgumentException("Target source runtime lacks exact active Owner/assignment/Store state");
        }
        if (!recoveryOnly) {
            // ACTIVE lease alone cannot expose Claims left behind by a previous process or restore.
            final var claimCheck = backend.prepareRead(new BoundedReadBudget(limits.records(), limits.bytes(),
                    limits.elapsedNanos(), monotonicClock), reader -> {
                final var first = reader.first(ColumnFamily.INFLIGHT,
                        new byte[] {TargetKeyCodec.CLAIM_TAG, TargetKeyCodec.KEY_FORMAT},
                        new byte[] {TargetKeyCodec.CLAIM_TAG + 1}, java.util.List.of());
                if (first != null) {
                    throw new IllegalStateException("Target startup must finish protected reversible Claim recovery before exposure");
                }
                reader.requireWithinElapsedBudget();
                return Boolean.TRUE;
            });
            backend.completeRead(claimCheck, (actual, actualScope) -> new TargetStoreBackend.CommitGuard() {
                @Override public void requireCurrent() {
                    if (store.runtimeMetadata().lastOpenedOwnerEpoch() != lease.ownerEpoch()) {
                        throw new IllegalStateException("Target startup Owner epoch changed during Claim gate");
                    }
                }
                @Override public void close() {}
            });
        }
        lineage = exactRoot.recoveryLineage();
        grants = new TargetQuotaGrantStore(backend, scope, lineage, limits.counters(), limits.domains());
        targetAdmissions = new TargetPublishAdmissionStore(
                backend, scope, lineage, limits.counters(), 1, limits.domains());
        publishRecoveryDiscovery = new TargetPublishRecoveryDiscovery(backend, scope, lineage);
        reclaimDiscovery=new com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery(backend,scope,lineage);
        targetOutcomes = new TargetPublishOutcomeStore(
                backend, scope, lineage, limits.counters(), 1, limits.domains());
        fences = new TargetTimeFenceStore(backend, scope, lineage, limits.counters(), limits.domains());
        expiryDiscovery = new TargetExpiryDiscoveryStore(backend, scope);
        expiries = new TargetExpireGenerationStore(backend, scope, lineage, limits.counters(), limits.domains());
        closes = new TargetCloseStore(backend, scope, lineage, limits.counters(), limits.domains());
        membershipControls =
                new TargetMembershipControlStore(backend, scope, lineage, limits.counters(), limits.domains());
        nativePolicyControls =
                new TargetNativePolicyControlStore(backend, scope, lineage, limits.counters(), limits.domains());
        channelControls = new com.nereusstream.delay.runtime.TargetChannelControlStore(
                backend, scope, lineage, limits.counters(), limits.domains());
        replay = new TargetSystemReplayStore(backend, scope, lineage, limits.counters(), limits.domains());
        commandReplay = new TargetCommandReplayStore(backend, scope, lineage, limits.counters(), limits.domains());
        commands = new TargetCommandStore(backend, scope, lineage, limits.counters(), limits.domains());
        backend.bindTargetQueueHeadCache(targetQueueHeadCache);
    }

    /** Requires a broker factory's accepted assignment to match this activated Target runtime exactly. */
    public void requireAcceptedAssignment(final SourceAssignment acceptedAssignment) {
        if (!assignment.sameIdentity(Objects.requireNonNull(acceptedAssignment, "acceptedAssignment"))) {
            throw new IllegalArgumentException("Target source runtime uses a different accepted assignment");
        }
    }

    /** Exact assignment whose activation barrier this runtime has validated against its Store frontier. */
    public SourceAssignment acceptedAssignment() {
        return assignment;
    }

    /** Additional local material-use check; existing authoritative Owner/Source guards remain responsible for I/O. */
    void requireLocalSourceTransportOwner(SourceAssignment expected, com.nereusstream.delay.protocol.OwnerIdentity owner, long now) {
        requireAcceptedAssignment(expected);
        synchronized (ownerStateLock) {
            if (fenced || recoveryOnly || lease.state() != ShardLifecycleState.ACTIVE_FOR_COMMANDS
                    || lease.ownerEpoch() != owner.ownerEpoch()
                    || !Bytes.constantTimeEquals(lease.leaseToken(), owner.leaseFencingDigest())
                    || now < lastOwnerTime || !lease.validAt(now)) {
                throw new IllegalStateException("Source material use lost its original active Owner");
            }
        }
    }

    synchronized void bindTargetQueueChangeSignal(final TargetStoreBackend.TargetQueueChangeSignal signal) {
        backend.bindTargetQueueChangeSignal(signal);
    }

    synchronized void unbindTargetQueueChangeSignal(final TargetStoreBackend.TargetQueueChangeSignal signal) {
        backend.unbindTargetQueueChangeSignal(signal);
    }

    synchronized void configureTargetQueueHeadCache(final int maximumEntries) {
        targetQueueHeadCache.configure(maximumEntries);
    }

    @Override
    synchronized void bind(final WorkClassExecutionRegistry registry) {
        if (workClasses != null && workClasses != registry) {
            throw new IllegalStateException("Target source runtime is bound to another work-class registry");
        }
        store.sharedResources().bindWorkClassExecutionRegistry(registry);
        workClasses = registry;
    }

    /** Builds Close GC on this exact active Owner, Store and shared WorkClass graph. */
    public synchronized TargetReservationClosureWorkClassExecutor newCloseGcExecutor(
            final WorkClassExecutionRegistry registry,
            final TargetReservationControls.Authority controls,
            final TargetReservationClosureWorkClassExecutor.Limits gcLimits,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final TargetQuotaDelta.ReservationClosureAuthority closureQuota,
            final TargetQuotaDelta.ReservationExpiryAuthority expiryQuota,
            final TargetQuotaDelta.CloseCursorAuthority cursorQuota,
            final LongSupplier ownerClock) {
        if (workClasses == null || workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalStateException("Close GC requires the bound Target source WorkClass graph");
        }
        Objects.requireNonNull(physicalWrites, "physicalWrites");
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetReservationClosureWorkClassExecutor(
                registry,
                backend,
                scope,
                lineage,
                limits.domains(),
                controls,
                gcLimits,
                () -> requireGcOwner(clock),
                (actual, actualScope) -> {
                    requireGcOwner(clock);
                    return gcGuard(authorities.reads().acquire(actual, actualScope), actual, actualScope, clock);
                },
                (actual, actualScope, mutation) -> {
                    final var stamp = mutation.quota().counters().mutation();
                    if (!stamp.reservationClosure() && !stamp.reservationExpiry() && !stamp.reservationCloseCursor()) {
                        throw new IllegalStateException("Close GC cannot commit another mutation kind");
                    }
                    requireGcOwner(clock);
                    return gcGuard(physicalWrites.acquire(actual, actualScope, mutation), actual, actualScope, clock);
                },
                closureQuota,
                expiryQuota,
                cursorQuota,
                monotonicClock);
    }

    /** Builds ordinary reservation expiry on the same exact Owner, Store and WorkClass graph. */
    public synchronized TargetReservationExpiryWorkClassExecutor newExpiryGcExecutor(
            final WorkClassExecutionRegistry registry,
            final TargetReservationControls.Authority controls,
            final TargetReservationExpiryWorkClassExecutor.Limits gcLimits,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final TargetQuotaDelta.ReservationExpiryAuthority expiryQuota,
            final LongSupplier ownerClock) {
        if (workClasses == null || workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalStateException("expiry GC requires the bound Target source WorkClass graph");
        }
        Objects.requireNonNull(physicalWrites, "physicalWrites");
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetReservationExpiryWorkClassExecutor(
                registry,
                new TargetReservationExpiryStore(backend, scope, lineage, limits.domains(), controls),
                gcLimits,
                () -> requireGcOwner(clock),
                (actual, actualScope) -> {
                    requireGcOwner(clock);
                    return gcGuard(authorities.reads().acquire(actual, actualScope), actual, actualScope, clock);
                },
                (actual, actualScope, mutation) -> {
                    if (!mutation.quota().counters().mutation().reservationExpiry()) {
                        throw new IllegalStateException("expiry GC cannot commit another mutation kind");
                    }
                    requireGcOwner(clock);
                    return gcGuard(physicalWrites.acquire(actual, actualScope, mutation), actual, actualScope, clock);
                },
                expiryQuota,
                monotonicClock);
    }

    /** Provides one bounded alternating Close/expiry action per caller-driven maintenance turn. */
    public synchronized TargetReservationGcRuntime newReservationGcRuntime(
            final WorkClassExecutionRegistry registry,
            final TargetReservationControls.Authority controls,
            final TargetReservationClosureWorkClassExecutor.Limits closeLimits,
            final TargetReservationExpiryWorkClassExecutor.Limits expiryLimits,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final TargetQuotaDelta.ReservationClosureAuthority closureQuota,
            final TargetQuotaDelta.ReservationExpiryAuthority expiryQuota,
            final TargetQuotaDelta.CloseCursorAuthority cursorQuota,
            final LongSupplier ownerClock) {
        if (maintenanceRuntime != null) {
            throw new IllegalStateException("Target reservation GC runtime is already bound to this Owner");
        }
        final var closeGc = newCloseGcExecutor(
                registry, controls, closeLimits, physicalWrites, closureQuota, expiryQuota, cursorQuota, ownerClock);
        final var expiryGc =
                newExpiryGcExecutor(registry, controls, expiryLimits, physicalWrites, expiryQuota, ownerClock);
        maintenanceRuntime =
                new TargetReservationGcRuntime(registry, scope.shard(), lease.ownerEpoch(), closeGc, expiryGc);
        return maintenanceRuntime;
    }

    synchronized void requireWorkerStore(final ShardStore expectedStore, final SharedRocksDbResources resources) {
        if (store != Objects.requireNonNull(expectedStore, "store")
                || store.sharedResources() != Objects.requireNonNull(resources, "resources")) {
            throw new IllegalArgumentException("Target Worker requires the exact source Store resource graph");
        }
    }

    synchronized void requireRuntimeLimits(final Limits expected) {
        if (!limits.equals(Objects.requireNonNull(expected, "expected"))) {
            throw new IllegalArgumentException("Target Source limits differ from the Worker configuration");
        }
    }

    synchronized TargetSourceBacklog.Cut captureSourceBacklogCut(final LongSupplier ownerClock) {
        requireGcOwner(ownerClock);
        final var applied = Objects.requireNonNull(store.appliedShardLogPosition(), "initialized Target Source");
        final var cut = new TargetSourceBacklog.Cut(applied, () -> {
            requireGcOwner(ownerClock);
            if (!Arrays.equals(applied.canonicalBytes(),
                    Objects.requireNonNull(store.appliedShardLogPosition(), "initialized Target Source")
                            .canonicalBytes())) {
                throw new IllegalStateException("Target Source backlog cut advanced during observation");
            }
            requireGcOwner(ownerClock);
        });
        cut.requireCurrent();
        return cut;
    }

    /**
     * Commits one selected Target head under the active Owner and Store graph. The supplied write
     * authority must also protect current time, policy, permits and the complete Owner identity.
     */
    synchronized TargetClaimRecord claim(
            final BoundedReadBudget budget,
            final TargetHeadRef selected,
            final OwnerIdentity owner,
            final long nowEpochMs,
            final long deadlineEpochMs,
            final long executionBytes,
            final byte[] operationDigest,
            final TargetQuotaDelta.LocalClaimAuthority quota,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final LongSupplier ownerClock) {
        return claim(budget, selected, owner, nowEpochMs, deadlineEpochMs, executionBytes, operationDigest,
                quota, physicalWrites, ownerClock, null);
    }

    synchronized TargetClaimRecord claim(
            final BoundedReadBudget budget,
            final TargetHeadRef selected,
            final OwnerIdentity owner,
            final long nowEpochMs,
            final long deadlineEpochMs,
            final long executionBytes,
            final byte[] operationDigest,
            final TargetQuotaDelta.LocalClaimAuthority quota,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final LongSupplier ownerClock,
            final com.nereusstream.delay.protocol.HandoffPolicyHeadRef policyHead) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var exactOwner = Objects.requireNonNull(owner, "owner");
        if (exactOwner.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(exactOwner.leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalStateException("Target Claim Owner identity differs from the active lease");
        }
        final var claims =
                new TargetClaimStore(backend, scope, lineage, limits.domains(), Objects.requireNonNull(quota, "quota"));
        final var prepared = claims.prepareClaim(
                budget, selected, exactOwner, nowEpochMs, deadlineEpochMs, executionBytes, operationDigest, policyHead);
        claims.commit(prepared, (actual, actualScope, mutation) -> {
            if (!mutation.quota().counters().mutation().isLocalClaim()) {
                throw new IllegalStateException("Target Claim cannot commit another mutation kind");
            }
            requireGcOwner(clock);
            return gcGuard(
                    Objects.requireNonNull(physicalWrites, "physicalWrites").acquire(actual, actualScope, mutation),
                    actual,
                    actualScope,
                    clock);
        });
        return prepared.claim();
    }

    /** Restores only an unadmitted, exact Claim under the same active Owner and Store guards. */
    synchronized boolean claimStillCurrent(final BoundedReadBudget budget, final TargetClaimRecord claim,
            final TargetQuotaDelta.LocalClaimAuthority quota, final LongSupplier clock) {
        requireGcOwner(clock);
        return new TargetClaimStore(backend, scope, lineage, limits.domains(), quota)
                .isCurrent(budget, claim, workerReads(clock));
    }

    synchronized void revokeClaim(
            final BoundedReadBudget budget,
            final TargetClaimRecord expected,
            final byte[] operationDigest,
            final TargetQuotaDelta.LocalClaimAuthority quota,
            final TargetStoreBackend.CommitAuthority physicalWrites,
            final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var exact = Objects.requireNonNull(expected, "expected");
        if (exact.owner().ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(exact.owner().leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalStateException("Target revoke Owner identity differs from the active lease");
        }
        final var claims =
                new TargetClaimStore(backend, scope, lineage, limits.domains(), Objects.requireNonNull(quota, "quota"));
        final var prepared = claims.prepareRevoke(budget, exact, exact.owner(), operationDigest);
        claims.commit(prepared, (actual, actualScope, mutation) -> {
            if (!mutation.quota().counters().mutation().isLocalClaim()) {
                throw new IllegalStateException("Target revoke cannot commit another mutation kind");
            }
            requireGcOwner(clock);
            return gcGuard(
                    Objects.requireNonNull(physicalWrites, "physicalWrites").acquire(actual, actualScope, mutation),
                    actual,
                    actualScope,
                    clock);
        });
    }

    /** Rebuilds bounded Target head summaries without granting Claim or Producer authority. */
    synchronized TargetQueueSnapshotReader.Page scanTargetQueues(
            final BoundedReadBudget budget,
            final TargetPartitionId after,
            final int maximumTargets,
            final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetQueueSnapshotReader(backend, limits.domains(), targetQueueHeadCache)
                .scan(budget, after, maximumTargets, workerReads(clock));
    }

    synchronized Optional<TargetQueueSnapshotReader.Entry> readTargetQueue(
            final BoundedReadBudget budget, final TargetPartitionId target, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetQueueSnapshotReader(backend, limits.domains(), targetQueueHeadCache)
                .readTarget(budget, target, workerReads(clock));
    }

    synchronized TargetPublishAdmissionBody preparePublishAdmission(
            final BoundedReadBudget budget, final TargetClaimRecord claim,
            final com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            final com.nereusstream.delay.protocol.ProfileRef capability, final byte[] artifacts,
            final long retryUntilEpochMs, final com.nereusstream.delay.protocol.CapacityVector commitment,
            final com.nereusstream.delay.protocol.CapacityVector allocated,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time,
            final com.nereusstream.delay.semantic.TargetNativePolicyAuthority policies, final LongSupplier clock) {
        return preparePublishAdmission(budget, claim, channel, capability, artifacts, retryUntilEpochMs,
                commitment, allocated, time, policies, null, clock);
    }

    synchronized TargetPublishAdmissionBody preparePublishAdmission(
            final BoundedReadBudget budget, final TargetClaimRecord claim,
            final com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            final com.nereusstream.delay.protocol.ProfileRef capability, final byte[] artifacts,
            final long retryUntilEpochMs, final com.nereusstream.delay.protocol.CapacityVector commitment,
            final com.nereusstream.delay.protocol.CapacityVector allocated,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time,
            final com.nereusstream.delay.semantic.TargetNativePolicyAuthority policies,
            final com.nereusstream.delay.protocol.TargetKafkaReceiptBinding kafkaReceipt, final LongSupplier clock) {
        requireGcOwner(clock);
        if (claim.owner().ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(claim.owner().leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalStateException("Admission Claim differs from the current Owner lease");
        }
        final var body = targetAdmissions.prepareBody(budget, claim, channel, capability, artifacts,
                retryUntilEpochMs, commitment, allocated, time, policies, kafkaReceipt, workerReads(clock));
        requireGcOwner(clock);
        return body;
    }

    synchronized void requireNativeOwnership(final BoundedReadBudget budget,
            final TargetPublishAdmissionStore.Applied applied,
            final com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence time, final LongSupplier clock) {
        requireGcOwner(clock);
        final var owner = applied.body().owner();
        if (owner.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(owner.leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalStateException("Native first ownership belongs to another Owner lease");
        }
        targetAdmissions.requireNativeOwnership(budget, applied, time, workerReads(clock));
        requireGcOwner(clock);
    }

    synchronized TargetPublishAdmissionStore.Applied readAppliedAdmission(
            final BoundedReadBudget budget, final SystemMutation image, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var applied = targetAdmissions.readApplied(budget, image, workerReads(clock));
        final var owner = applied.body().owner();
        if (owner.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(owner.leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalStateException("Target first-send Admission belongs to another Owner");
        }
        requireGcOwner(clock);
        return applied;
    }

    synchronized com.nereusstream.delay.runtime.TargetPublishOutcomeStore.NegativeOutcome readNegativeOutcome(
            final BoundedReadBudget budget, final byte[] mappingId, final SystemMutation admission,
            final SystemMutation outcome, final com.nereusstream.delay.protocol.PulsarRecordTemplate template,
            final LongSupplier clock) {
        requireGcOwner(clock);
        final var proof = targetOutcomes.readNegativeOutcome(budget, mappingId, admission, outcome, template, workerReads(clock));
        requireGcOwner(clock);
        return proof;
    }

    synchronized TargetPublishAdmissionStore.Recovery readRecoveryAdmission(
            final BoundedReadBudget budget, final SystemMutation image, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var recovery = targetAdmissions.readRecovery(budget, image, workerReads(clock));
        requireGcOwner(clock);
        return recovery;
    }

    synchronized com.nereusstream.delay.runtime.TargetPublishOutcomeStore.KafkaUnknown readKafkaUnknown(
            BoundedReadBudget budget, SystemMutation admission, LongSupplier clock) {
        requireGcOwner(clock);
        final var result = targetOutcomes.readKafkaUnknown(budget, admission, workerReads(clock));
        requireGcOwner(clock);
        return result;
    }

    synchronized TargetPublishOutcomeStore.KafkaRecoveryPending readKafkaRecoveryPending(BoundedReadBudget budget,
            SystemMutation admission, com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping, LongSupplier clock) {
        requireGcOwner(clock);
        final var result = targetOutcomes.readKafkaRecoveryPending(budget, admission, mapping, workerReads(clock));
        requireGcOwner(clock); return result;
    }

    synchronized boolean kafkaMappingSettled(BoundedReadBudget budget, SystemMutation admission,
            com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping, LongSupplier clock) {
        requireGcOwner(clock);
        final boolean result = targetOutcomes.kafkaMappingSettled(budget, admission, mapping, workerReads(clock));
        requireGcOwner(clock); return result;
    }

    synchronized TargetPublishRecoveryDiscovery.Page discoverPublishRecovery(
            final BoundedReadBudget budget, final TargetPublishRecoveryDiscovery.Cursor continuation,
            final int maximumRows, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var page = publishRecoveryDiscovery.scan(budget, continuation, maximumRows, workerReads(clock));
        requireGcOwner(clock);
        return page;
    }

    synchronized TargetPublishRecoveryDiscovery.Page discoverKafkaUnknown(
            BoundedReadBudget reads, TargetPublishRecoveryDiscovery.Cursor continuation, int maximumRows, LongSupplier clock) {
        requireGcOwner(clock);
        final var page = publishRecoveryDiscovery.scanKafkaUnknown(reads, continuation, maximumRows, workerReads(clock));
        requireGcOwner(clock);
        return page;
    }

    synchronized boolean publishRecoveryStillPending(BoundedReadBudget reads,
            TargetPublishRecoveryDiscovery.Reference reference, LongSupplier clock) {
        requireGcOwner(clock);
        final boolean current = publishRecoveryDiscovery.stillPending(reads, reference, workerReads(clock));
        requireGcOwner(clock);
        return current;
    }

    synchronized void requirePublishRecoveryOwner(final LongSupplier ownerClock) {
        requireGcOwner(Objects.requireNonNull(ownerClock, "ownerClock"));
    }

    synchronized TargetWorkerChannelInventory newChannelInventory(TargetWorkerChannelInventory.Limits limits,
            LongSupplier clock, LongSupplier nanos) {
        requireGcOwner(clock); return new TargetWorkerChannelInventory(this, lineage, limits, clock, nanos);
    }

    synchronized <T> T readChannelInventory(BoundedReadBudget budget,
            java.util.function.Function<TargetStoreBackend.Reader, T> reader, LongSupplier clock) {
        requireGcOwner(clock);
        final T value = backend.guardedRead(budget, reader, workerReads(clock));
        requireGcOwner(clock); return value;
    }

    synchronized com.nereusstream.delay.protocol.CanonicalTargetPartition requirePublicationChannel(
            com.nereusstream.delay.protocol.TargetChannelIdentity channel, boolean newWork, BoundedReadBudget reads, LongSupplier clock) {
        return readPublicationChannel(channel,newWork,false,reads,clock).physical();
    }
    synchronized com.nereusstream.delay.protocol.CanonicalTargetPartition requireRetainedPublicationChannel(
            com.nereusstream.delay.protocol.TargetChannelIdentity channel,BoundedReadBudget reads,LongSupplier clock) {
        return readPublicationChannel(channel,false,true,reads,clock).physical();
    }
    synchronized boolean publicationChannelOpen(com.nereusstream.delay.protocol.TargetChannelIdentity channel,
            BoundedReadBudget reads, LongSupplier clock) {
        return readPublicationChannel(channel,false,true,reads,clock).currentOpen();
    }
    private record PublicationChannelState(com.nereusstream.delay.protocol.CanonicalTargetPartition physical, boolean currentOpen) {}
    private PublicationChannelState readPublicationChannel(
            com.nereusstream.delay.protocol.TargetChannelIdentity channel,boolean newWork,boolean retained,
            BoundedReadBudget reads,LongSupplier clock) {
        requireGcOwner(clock);
        final var actual=backend.guardedRead(reads,reader->{
            final var slot=com.nereusstream.delay.runtime.TargetChannelControlVerifier.readSlot(reader,channel,lineage);
            if(slot==null)throw new IllegalStateException("publication startup lacks its actual APPLIED channel lifecycle");
            if(newWork)slot.requireNewWork(channel);
            else if(!retained)slot.channel().requireExactFrozenIdentity(channel);
            else {
                if(!java.util.Arrays.equals(slot.channel().context().producerIdentity(),channel.context().producerIdentity())
                        ||Long.compareUnsigned(slot.channel().context().channelGeneration(),channel.context().channelGeneration())<0)
                    throw new IllegalStateException("retained channel changed its Source stable producer scope");
                final byte[] old=reader.get(com.nereusstream.delay.store.ColumnFamily.META,channel.encodedKey());
                if(old==null||!java.util.Arrays.equals(com.nereusstream.delay.store.TargetValueEnvelope.decode(old,
                        com.nereusstream.delay.protocol.TargetChannelIdentity.VALUE_TYPE).payload(),channel.canonicalBytes()))
                    throw new IllegalStateException("retained channel lacks its exact immutable Source identity");
            }
            final byte[] key=com.nereusstream.delay.store.TargetKeyCodec.identity(channel.context().target());
            final byte[] raw=reader.get(com.nereusstream.delay.store.ColumnFamily.META,key);
            if(raw==null)throw new IllegalStateException("publication channel lacks its actual Target identity");
            final var physical=com.nereusstream.delay.protocol.CanonicalTargetPartition.decodeForStore(key,
                    com.nereusstream.delay.store.TargetValueEnvelope.decode(raw,com.nereusstream.delay.protocol.CanonicalTargetPartition.VALUE_TYPE).payload());
            reader.requireWithinElapsedBudget();return new PublicationChannelState(physical,
                    slot.phase()==com.nereusstream.delay.protocol.TargetChannelSlot.Phase.OPEN
                            &&java.util.Arrays.equals(slot.channel().canonicalBytes(),channel.canonicalBytes()));
        },workerReads(clock));
        requireGcOwner(clock);return actual;
    }

    synchronized com.nereusstream.delay.protocol.TargetQuotaAccounting readPublicationAccounting(
            BoundedReadBudget reads,com.nereusstream.delay.protocol.TargetChannelIdentity channel,LongSupplier clock) {
        requireGcOwner(clock);
        final var result=backend.guardedRead(reads,reader->{
            final var id=new com.nereusstream.delay.protocol.TargetQuotaIdentity(com.nereusstream.delay.protocol.TargetQuotaIdentity.Kind.TARGET,
                    scope.shard(),channel.context().accountingIncarnation(),channel.context().target(),null);
            final byte[] key=id.key();key[0]=com.nereusstream.delay.store.TargetKeyCodec.QUOTA_INCARNATION_TAG;
            final byte[] raw=reader.get(com.nereusstream.delay.store.ColumnFamily.META,key);
            if(raw==null)throw new IllegalStateException("publication has no frozen Target accounting descriptor");
            final var owner=com.nereusstream.delay.protocol.TargetQuotaIncarnation.decodeForStore(key,
                    com.nereusstream.delay.store.TargetValueEnvelope.decode(raw,com.nereusstream.delay.protocol.TargetQuotaIncarnation.VALUE_TYPE).payload(),
                    scope.shard(),scope.tenantScope());
            if(!java.util.Arrays.equals(owner.recoveryLineage(),lineage))throw new IllegalStateException("publication accounting lineage changed");
            owner.latestMutation().requireAtOrBefore(reader.aggregate().mutation());reader.requireWithinElapsedBudget();return owner.accounting();
        },workerReads(clock));requireGcOwner(clock);return result;
    }

    synchronized OwnerIdentity requirePublicationOwner(OwnerIdentity expected, LongSupplier clock) {
        requireGcOwner(clock);
        if (Objects.requireNonNull(expected, "configured publication Owner").ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(expected.leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalStateException("receipt provider belongs to another actual Source/Owner lease");
        }
        return expected;
    }

    synchronized com.nereusstream.delay.runtime.TargetExternalEvidenceStore.Adoption readExternalEvidenceAdoption(
            BoundedReadBudget reads, com.nereusstream.delay.protocol.TargetKafkaReceiptMapping mapping, LongSupplier clock) {
        requireGcOwner(clock);
        final var proof = com.nereusstream.delay.runtime.TargetExternalEvidenceStore.readAdoption(backend, reads, mapping,
                scope, lineage, workerReads(clock));
        requireGcOwner(clock);
        return proof;
    }
    synchronized com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption readRecoveryEvidenceAdoption(
            BoundedReadBudget reads,com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.PreparedRecovery snapshot,
            SystemMutation mutation,LongSupplier clock){
        requireGcOwner(clock);
        final var proof=com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.readAdoption(backend,reads,scope,lineage,snapshot,mutation,workerReads(clock));
        requireGcOwner(clock);return proof;
    }
    synchronized com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.ClosedUses readClosedRecoveryUses(
            BoundedReadBudget reads,com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
            com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.Adoption applied,LongSupplier clock){
        requireGcOwner(clock);
        final var actual=com.nereusstream.delay.runtime.TargetRecoveryEvidenceStore.readClosedUses(
                backend,reads,scope,lineage,closure,applied,workerReads(clock));
        requireGcOwner(clock);return actual;
    }

    synchronized com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ControlClosure readControlReferenceClosure(
            BoundedReadBudget reads,com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ControlReferenceSnapshot snapshot,
            LongSupplier clock){
        requireGcOwner(clock);
        final var actual=com.nereusstream.delay.runtime.TargetExternalEvidenceStore.readControlClosure(
                backend,reads,scope,lineage,snapshot,workerReads(clock));
        requireGcOwner(clock);return actual;
    }

    synchronized com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ClosedUses readClosedControlUses(
            BoundedReadBudget reads,com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
            com.nereusstream.delay.runtime.TargetExternalEvidenceStore.Adoption applied,int kind,LongSupplier clock){
        requireGcOwner(clock);
        final var actual=com.nereusstream.delay.runtime.TargetExternalEvidenceStore.readClosedControlUses(
                backend,reads,scope,lineage,closure,applied,kind,workerReads(clock));
        requireGcOwner(clock);return actual;
    }

    synchronized com.nereusstream.delay.runtime.TargetStagingEvidenceStore.ClosedUses readClosedStagingUses(
            BoundedReadBudget reads,com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
            LongSupplier clock){
        requireGcOwner(clock);
        final var actual=com.nereusstream.delay.runtime.TargetStagingEvidenceStore.readClosedUses(
                backend,reads,scope,lineage,closure,workerReads(clock));
        requireGcOwner(clock);return actual;
    }
    synchronized com.nereusstream.delay.runtime.TargetExternalEvidenceStore.ClosedUses readClosedControlUses(
            BoundedReadBudget reads,com.nereusstream.delay.runtime.OxiaSyncTargetKafkaReceiptProtectionCatalog.ClosedObject closure,
            com.nereusstream.delay.protocol.TargetExternalEvidenceOwner selected,LongSupplier clock){
        requireGcOwner(clock);
        final var actual=com.nereusstream.delay.runtime.TargetExternalEvidenceStore.readClosedControlUses(
                backend,reads,scope,lineage,closure,selected,workerReads(clock));
        requireGcOwner(clock);return actual;
    }

    synchronized com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent readAppliedRetirementIntent(
            BoundedReadBudget reads,SystemMutation selected,LongSupplier clock){
        requireGcOwner(clock);
        final var actual=com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.readAppliedIntent(
                backend,reads,scope,lineage,selected,workerReads(clock));
        requireGcOwner(clock);return actual;
    }
    synchronized com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedConfirmation readAppliedRetirementConfirmation(
            BoundedReadBudget reads,SystemMutation selected,LongSupplier clock){
        requireGcOwner(clock);
        final var actual=com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.readAppliedConfirmation(
                backend,reads,scope,lineage,selected,workerReads(clock));
        requireGcOwner(clock);return actual;
    }

    synchronized TargetPublishRecoveryDiscovery.Page discoverCheckpointInputs(
            final BoundedReadBudget budget, final TargetPublishRecoveryDiscovery.Cursor continuation,
            final int maximumRows, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var page = publishRecoveryDiscovery.scanRetainedInputs(
                budget, continuation, maximumRows, workerReads(clock));
        requireGcOwner(clock);
        return page;
    }

    synchronized boolean publishRecoveryStillAdmitted(final BoundedReadBudget budget,
            final TargetPublishRecoveryDiscovery.Reference reference, final LongSupplier ownerClock) {
        requireGcOwner(ownerClock);
        final boolean admitted = publishRecoveryDiscovery.stillAdmitted(budget, reference, workerReads(ownerClock));
        requireGcOwner(ownerClock);
        return admitted;
    }

    synchronized void requireCheckpointInputsCurrent(final BoundedReadBudget budget,
            final TargetPublishRecoveryDiscovery.Cursor cursor, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        publishRecoveryDiscovery.requireRetainedCursorCurrent(budget, cursor, workerReads(clock));
        requireGcOwner(clock);
    }

    synchronized TargetCheckpointControlInputs.Cut captureCheckpointControlInputs(final BoundedReadBudget budget,
            final com.nereusstream.delay.store.TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final LongSupplier ownerClock) {
        requireGcOwner(ownerClock);
        final var captured = backend.guardedRead(budget, reader -> {
            final var bookkeeping = com.nereusstream.delay.protocol.TargetQuotaBookkeeping.decode(
                    TargetValueEnvelope.decode(reader.get(ColumnFamily.META,
                            com.nereusstream.delay.protocol.TargetQuotaBookkeeping.keyFor(scope.shard())),
                            com.nereusstream.delay.protocol.TargetQuotaBookkeeping.VALUE_TYPE).payload());
            final byte[] rootKey = bookkeeping.owner().key();
            rootKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
            final var root = TargetQuotaIncarnation.decodeForStore(rootKey,
                    TargetValueEnvelope.decode(reader.get(ColumnFamily.META, rootKey),
                            TargetQuotaIncarnation.VALUE_TYPE).payload(), scope.shard(), scope.tenantScope());
            final var aggregate = reader.aggregate();
            if (!bookkeeping.owner().equals(root.identity())
                    || !Arrays.equals(bookkeeping.tenantScope(), scope.tenantScope())
                    || !Arrays.equals(bookkeeping.accounting().canonicalBytes(), root.accounting().canonicalBytes())
                    || !Arrays.equals(root.recoveryLineage(), lineage)
                    || aggregate.mutation() == null) {
                throw new IllegalStateException("Control input cut has inconsistent root/bookkeeping/lineage");
            }
            root.latestMutation().requireAtOrBefore(aggregate.mutation());
            bookkeeping.mutation().requireAtOrBefore(aggregate.mutation());
            final var proof = new com.nereusstream.delay.store.TargetCheckpointRootVerifier.RootProof(reader.metadata(),
                    aggregate.mutation().source(), aggregate.mutation().sequence(), root, bookkeeping, aggregate);
            final var dependencies = com.nereusstream.delay.store.TargetCheckpointRootVerifier
                    .auditLiveStoreDependencies(store, proof, ledgerLimits, budget);
            reader.requireWithinElapsedBudget();
            return new TargetCheckpointControlInputs.Cut(this,
                    new TargetQueueSnapshotReader.Cut(reader.metadata().storeIncarnation(), reader.nativeSequence()),
                    dependencies);
        }, workerReads(ownerClock));
        requireGcOwner(ownerClock);
        return captured;
    }

    synchronized void requireCheckpointControlInputsCurrent(final BoundedReadBudget budget,
            final TargetCheckpointControlInputs.Cut cut, final LongSupplier ownerClock) {
        requireGcOwner(ownerClock);
        if (cut.owner != this) { throw new IllegalArgumentException("Control history cut belongs to another Worker"); }
        backend.guardedRead(budget, reader -> {
            if (!cut.view.equals(new TargetQueueSnapshotReader.Cut(
                    reader.metadata().storeIncarnation(), reader.nativeSequence()))) {
                throw new IllegalStateException("Control history cut changed its physical Store view");
            }
            reader.requireWithinElapsedBudget();
            return Boolean.TRUE;
        }, workerReads(ownerClock));
        requireGcOwner(ownerClock);
    }

    synchronized void requireOutcomeWriter(final SystemMutation mutation, final LongSupplier clock) {
        requireGcOwner(clock);
        final var author = AuthorIdentity.decode(mutation.authorIdentity());
        if (!mutation.shardId().equals(scope.shard())) {
            throw new IllegalStateException("Target Outcome writer differs from current Source Shard");
        }
        if (mutation.type() == SystemMutationType.EVIDENCE_RESOLUTION) {
            if (author.kind() != AuthorIdentity.Kind.SERVICE) {
                throw new IllegalStateException("Target resolution requires its service writer");
            }
            return;
        }
        if (mutation.type() != SystemMutationType.PUBLISH_OUTCOME || author.kind() != AuthorIdentity.Kind.OWNER
                || author.asOwnerIdentity().ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(author.asOwnerIdentity().leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalStateException("Target Outcome writer differs from current Source/Owner");
        }
    }

    synchronized void requireAdmissionWriter(final SystemMutation mutation, final LongSupplier clock) {
        requireGcOwner(clock);
        final var author = AuthorIdentity.decode(mutation.authorIdentity());
        if (mutation.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                || !mutation.shardId().equals(scope.shard())
                || author.kind() != AuthorIdentity.Kind.OWNER
                || author.asOwnerIdentity().ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(author.asOwnerIdentity().leaseFencingDigest(), lease.leaseToken())) {
            throw new IllegalStateException("Target Admission writer differs from current Source/Owner");
        }
        targetAdmissions.requireCurrentSubmission(
                new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock),
                mutation, workerReads(clock));
        requireGcOwner(clock);
    }

    synchronized Optional<SystemMutationResult> admissionMutationResult(
            final SystemMutation mutation, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        if (mutation.type() != SystemMutationType.TARGET_PUBLISH_ADMISSION
                || !mutation.shardId().equals(scope.shard())) {
            throw new IllegalArgumentException("Target Admission reconciliation requires its exact Source mutation");
        }
        return replay.appliedResult(
                new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock),
                mutation, workerReads(clock));
    }

    synchronized void requireTargetControlWriter(final SystemMutation mutation, final LongSupplier clock) {
        requireGcOwner(clock);
        if (mutation.type() != SystemMutationType.APPLY_SHARD_CONTROL
                || !scope.shard().equals(mutation.shardId())
                || AuthorIdentity.decode(mutation.authorIdentity()).kind() != AuthorIdentity.Kind.CONTROL) {
            throw new IllegalArgumentException("Target Control writer belongs to another author/type/Shard");
        }
        if (isSloObservationControl(mutation)) {
            final var body = com.nereusstream.delay.protocol.TargetSloObservationControlBody.decode(mutation.canonicalBody());
            if (!scope.equals(body.request().scope()) || !Arrays.equals(lineage, body.request().lineage())) {
                throw new IllegalArgumentException("Target certification writer changed its frozen scope/lineage");
            }
            final var tuple = com.nereusstream.delay.protocol.ProtocolTuple.targetSloObservation();
            final var compatible = store.controlSnapshot();
            if (compatible == null || !compatible.protocolTuples().contains(tuple)) {
                throw new IllegalStateException("Target certification writer lacks independently admitted readers");
            }
            backend.guardedRead(new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock), reader -> {
                final var activation = com.nereusstream.delay.runtime.TargetProtocolActivationStore.applied(reader, scope, lineage, tuple);
                if (activation == null || activation.body().payload().descriptor().schemaVersion() < 15) {
                    throw new IllegalStateException("Target certification writer lacks its original APPLIED activation");
                }
                return Boolean.TRUE;
            }, workerReads(clock));
        } else if (isChannelControl(mutation)) {
            final var body = TargetChannelControlBody.decode(mutation.canonicalBody());
            final var owner = body.request().owner();
            if (owner.ownerEpoch() != lease.ownerEpoch()
                    || !Bytes.constantTimeEquals(owner.leaseFencingDigest(), lease.leaseToken())) {
                throw new IllegalStateException("channel lifecycle writer requested another active Owner");
            }
        } else {
            throw new IllegalArgumentException("Target shared Control slot does not accept this operation");
        }
        requireGcOwner(clock);
    }

    synchronized Optional<SystemMutationResult> targetControlMutationResult(
            final SystemMutation mutation, final LongSupplier clock) {
        requireGcOwner(clock);
        if (mutation.type() != SystemMutationType.APPLY_SHARD_CONTROL
                || (!isChannelControl(mutation) && !isSloObservationControl(mutation))
                || !scope.shard().equals(mutation.shardId())) {
            throw new IllegalArgumentException("Target Control result lookup belongs to another operation/Shard");
        }
        return replay.appliedResult(
                new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock),
                mutation, workerReads(clock));
    }

    synchronized Optional<SystemMutationResult> outcomeMutationResult(
            final SystemMutation mutation, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        if ((mutation.type() != SystemMutationType.PUBLISH_OUTCOME
                && mutation.type() != SystemMutationType.EVIDENCE_RESOLUTION)
                || !mutation.shardId().equals(scope.shard())) {
            throw new IllegalArgumentException("Target Outcome reconciliation requires its exact Source mutation");
        }
        return replay.appliedResult(
                new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock),
                mutation, workerReads(clock));
    }

    synchronized com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Page discoverResourceReclaim(BoundedReadBudget reads,
            com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Cursor cursor,int maximumRows,LongSupplier clock){
        requireGcOwner(clock);
        if(maximumRows<1||maximumRows>com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OPERATIONS||limits.counters()<4)
            throw new IllegalArgumentException("reclaim discovery requires a bounded page and Source root/owner counter window");
        // A page may contain one distinct frozen owner per row, in addition to the Source root and all mirrors.
        final int serviceableRows=Math.min(maximumRows,(limits.counters()-2)/2);
        final boolean unused=resourceReclaimProtocolActive(com.nereusstream.delay.protocol.ProtocolTuple.targetResourceReclaim(),clock);
        final boolean inline=resourceReclaimProtocolActive(com.nereusstream.delay.protocol.ProtocolTuple.targetSelfContainedInlinePayloadReclaim(),clock);
        final boolean nativeRetained=resourceReclaimProtocolActive(com.nereusstream.delay.protocol.ProtocolTuple.targetNativeRetainedReclaim(),clock);
        final boolean object=resourceReclaimProtocolActive(com.nereusstream.delay.protocol.ProtocolTuple.targetObjectPayloadReclaim(),clock);
        final boolean uncommitted=resourceReclaimProtocolActive(com.nereusstream.delay.protocol.ProtocolTuple.targetUncommittedObjectReclaim(),clock);
        final boolean anchorAllocation=resourceReclaimProtocolActive(com.nereusstream.delay.protocol.ProtocolTuple.targetAnchorAllocationReclaim(),clock);
        final boolean incarnation=resourceReclaimProtocolActive(com.nereusstream.delay.protocol.ProtocolTuple.targetIncarnationReclaim(),clock);
        final boolean duplicatePosition=resourceReclaimProtocolActive(com.nereusstream.delay.protocol.ProtocolTuple.targetDuplicatePositionReclaim(),clock);
        return reclaimDiscovery.scan(reads,cursor,serviceableRows,workerReads(clock),unused,inline,nativeRetained,object,uncommitted,anchorAllocation,incarnation,duplicatePosition);
    }
    synchronized ReclaimPreparationRead reclaimPreparationRead(
            java.util.List<com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate> candidates,LongSupplier clock){
        if(candidates.isEmpty()||candidates.size()>com.nereusstream.delay.protocol.TargetResourceReclaimBody.MAX_OPERATIONS)
            throw new IllegalArgumentException("bounded original Native candidates required");
        candidates.forEach(reclaimDiscovery::requireCandidate);
        final var tuple=candidates.getFirst().requiredTuple();
        if(candidates.stream().anyMatch(c->!c.requiredTuple().equals(tuple)))throw new IllegalArgumentException("reclaim preparation cannot mix body generations");
        requireResourceReclaimProtocolActive(tuple,clock);
        final long primaryOwners=candidates.stream().map(com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate::primaryIdentity).distinct().count();
        // Source root and each frozen Target owner need their independent tenant mirrors.
        if(2+2*primaryOwners>limits.counters())throw new IllegalArgumentException("candidate group exceeds original Source counter window");
        return new ReclaimPreparationRead(this,java.util.List.copyOf(candidates),clock);
    }
    /** Actual active GC context. It cannot stand in for the selected Source apply's separate ReclaimRead. */
    public static final class ReclaimPreparationRead {
        private final TargetSourceApplyRuntime runtime;
        private final java.util.List<com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate> candidates;
        private final LongSupplier clock;
        private ReclaimPreparationRead(TargetSourceApplyRuntime runtime,
                java.util.List<com.nereusstream.delay.runtime.TargetResourceReclaimDiscovery.Candidate> candidates,LongSupplier clock){
            this.runtime=runtime;this.candidates=candidates;this.clock=clock;
        }
        void requireRuntime(TargetSourceApplyRuntime actual){if(runtime!=actual)throw new IllegalArgumentException("prepared reclaim belongs to another Worker Source");}
        public void requireCurrent(){
            synchronized(runtime.ownerStateLock){
                final long now=clock.getAsLong();final var lease=runtime.lease;
                if(runtime.recoveryOnly||runtime.fenced||runtime.store.isCloseStarted()||now<runtime.lastOwnerTime||!lease.validAt(now)
                        ||lease.state()!=ShardLifecycleState.ACTIVE_FOR_COMMANDS||runtime.store.runtimeMetadata().lastOpenedOwnerEpoch()!=lease.ownerEpoch())
                    throw new IllegalStateException("original GC Source Owner/Store is no longer active");
            }
        }
        public com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend catalog(){
            requireCurrent();
            if(!(runtime.originalEvidenceCatalog instanceof com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend actual))
                throw new TargetSourceAuthorityPreparation.InputUnavailable("original GC managed publication Catalog is unavailable");
            return actual;
        }
        com.nereusstream.delay.protocol.TargetResourceReclaimBody selectBody(byte[] root,long retryUntil,BoundedReadBudget reads,BoundedReadBudget nativeReads,Runnable held){
            requireCurrent();held.run();
            final var operations=new java.util.ArrayList<com.nereusstream.delay.protocol.TargetResourceReclaimBody.Operation>();
            for(var candidate:candidates){
                if(candidate.needsObjectProof()){
                    synchronized(runtime){
                        runtime.requireGcOwner(clock);
                        final var origin=runtime.backend.guardedRead(nativeReads,reader->{
                            final var expected=candidate.objectSelection().expected();final byte[] raw=reader.get(ColumnFamily.META,expected.key());
                            if(raw==null||!Arrays.equals(TargetValueEnvelope.decode(raw,com.nereusstream.delay.protocol.TargetQuotaPayloadOwner.VALUE_TYPE).payload(),expected.canonicalBytes()))throw new IllegalStateException("OBJECT selected owner changed before original Native acceptance read");
                            final var value=com.nereusstream.delay.runtime.TargetObjectPayloadAcceptance.collect(reader,expected);held.run();requireCurrent();return value;
                        },runtime.workerReads(clock));
                        operations.add(candidate.withNativeOrigin(origin).operation());
                    }
                }else operations.add(candidate.operation());
            }
            return catalog().selectNativeReclaimOperations(runtime.scope,runtime.lineage,root,retryUntil,operations,reads,()->{requireCurrent();held.run();});
        }
        void requirePendingObjectProvider(com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord pending,
                com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery,BoundedReadBudget reads,Runnable held){
            requireCurrent();held.run();recovery.requirePendingObject(pending);
            synchronized(runtime){
                runtime.requireGcOwner(clock);
                runtime.backend.guardedRead(reads,reader->{
                    final var body=pending.body();final byte[] raw=reader.get(ColumnFamily.META,pending.key());
                    if(raw==null||!Arrays.equals(TargetValueEnvelope.decode(raw,com.nereusstream.delay.protocol.TargetPayloadObjectRetiringRecord.VALUE_TYPE).payload(),pending.canonicalBytes()))
                        throw new IllegalStateException("live Native OBJECT intent changed before provider operation");
                    com.nereusstream.delay.runtime.TargetObjectPayloadReclaim.requirePending(reader,java.util.List.of(),pending,reader.aggregate().mutation());
                    final var applied=com.nereusstream.delay.runtime.TargetObjectPayloadReclaim.requireClosed(reader.auditLedger(),pending.intent(),recovery.objectPolicies(),true,reader::requireWithinElapsedBudget);
                    if(!pending.appliedAt().equals(applied))throw new IllegalStateException("live OBJECT lacks original APPLIED pending decision");
                    held.run();requireCurrent();return Boolean.TRUE;
                },runtime.workerReads(clock));
            }
        }
        void requireClosedDraft(SystemMutation input,com.nereusstream.delay.store.OxiaSyncCheckpointPublicationBackend.NativeRecoveryProtection recovery,
                BoundedReadBudget reads,Runnable held){
            requireCurrent();held.run();recovery.requireHeldFor(catalog(),input);
            synchronized(runtime){
                runtime.requireGcOwner(clock);
                runtime.backend.guardedRead(reads,reader->{
                    if(com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(input.canonicalBody()).objectPayloadReclaim())
                        com.nereusstream.delay.runtime.TargetResourceReclaimStore.requireClosedObjectDraft(reader,runtime.scope,runtime.lineage,input,recovery);
                    else if(com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(input.canonicalBody()).nativeRetainedReclaim())
                        com.nereusstream.delay.runtime.TargetResourceReclaimStore.requireClosedNativeDraft(reader,runtime.scope,runtime.lineage,input,recovery);
                    else if(com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(input.canonicalBody()).inlinePayloadRelease())
                        com.nereusstream.delay.runtime.TargetResourceReclaimStore.requireClosedInlineDraft(reader,runtime.scope,runtime.lineage,input,recovery);
                    else com.nereusstream.delay.runtime.TargetResourceReclaimStore.requireClosedUnusedDraft(reader,runtime.scope,runtime.lineage,input,recovery);
                    return Boolean.TRUE;
                },runtime.workerReads(clock));
                runtime.requireGcOwner(clock);
            }
            recovery.requireHeldFor(catalog(),input);held.run();requireCurrent();
        }
    }

    synchronized Optional<SystemMutationResult> retirementMutationResult(SystemMutation mutation,LongSupplier clock){
        requireGcOwner(clock);
        if((mutation.type()!=SystemMutationType.RESOURCE_RETIRE_INTENT&&mutation.type()!=SystemMutationType.RESOURCE_DELETE_CONFIRMED
                &&mutation.type()!=SystemMutationType.TARGET_RESOURCE_RECLAIM)||!scope.shard().equals(mutation.shardId()))
            throw new IllegalArgumentException("retirement result requires its exact Source operation/Shard");
        if(mutation.type()==SystemMutationType.RESOURCE_RETIRE_INTENT)com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.decode(mutation.canonicalBody());
        else if(mutation.type()==SystemMutationType.RESOURCE_DELETE_CONFIRMED)com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.decode(mutation.canonicalBody());
        else com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(mutation.canonicalBody());
        return replay.appliedResult(new BoundedReadBudget(limits.records(),limits.bytes(),limits.elapsedNanos(),monotonicClock),
                mutation,workerReads(clock));
    }
    synchronized void requireRetirementWriter(SystemMutation mutation,LongSupplier clock){
        requireGcOwner(clock);
        if((mutation.type()!=SystemMutationType.RESOURCE_RETIRE_INTENT&&mutation.type()!=SystemMutationType.RESOURCE_DELETE_CONFIRMED
                &&mutation.type()!=SystemMutationType.TARGET_RESOURCE_RECLAIM)||!scope.shard().equals(mutation.shardId())
                ||AuthorIdentity.decode(mutation.authorIdentity()).kind()!=AuthorIdentity.Kind.SERVICE)
            throw new IllegalArgumentException("retirement writer changed Source/operation/SERVICE actor");
        if(mutation.type()==SystemMutationType.TARGET_RESOURCE_RECLAIM){
            final var body=com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(mutation.canonicalBody());
            if(!body.scope().equals(scope)||!Arrays.equals(body.lineage(),lineage))throw new IllegalArgumentException("reclaim writer changed Source scope/lineage");
            requireResourceReclaimProtocolActive(body.requiredTuple(),clock);
        }else if(mutation.type()==SystemMutationType.RESOURCE_RETIRE_INTENT){
            final var body=com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.decode(mutation.canonicalBody());requireRetirementProtocolActive(body.resource(),clock);
        }else{
            final var body=com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.decode(mutation.canonicalBody());requireRetirementProtocolActive(body.resource(),body.markerRetained(),clock);
        }
    }
    synchronized void requireResourceReclaimProtocolActive(LongSupplier clock){
        requireResourceReclaimProtocolActive(com.nereusstream.delay.protocol.ProtocolTuple.targetResourceReclaim(),clock);
    }
    private void requireResourceReclaimProtocolActive(com.nereusstream.delay.protocol.ProtocolTuple tuple,LongSupplier clock){
        if(!resourceReclaimProtocolActive(tuple,clock))throw new IllegalStateException("reclaim writer requires its exact independently admitted/APPLIED capability "+tuple.bodyVersion());
    }
    private boolean resourceReclaimProtocolActive(com.nereusstream.delay.protocol.ProtocolTuple tuple,LongSupplier clock){
        requireGcOwner(clock);
        final var compatible=store.controlSnapshot();
        if(compatible==null||!compatible.protocolTuples().contains(tuple))return false;
        return backend.guardedRead(new BoundedReadBudget(limits.records(),limits.bytes(),limits.elapsedNanos(),monotonicClock),reader->{
            final var activation=com.nereusstream.delay.runtime.TargetProtocolActivationStore.applied(reader,scope,lineage,tuple);
            if(activation==null||!activation.body().payload().descriptor().supportsResourceReclaim()
                    ||tuple.equals(com.nereusstream.delay.protocol.ProtocolTuple.targetInlinePayloadReclaim())
                        &&!activation.body().payload().descriptor().supportsInlinePayloadReclaim()
                    ||tuple.equals(com.nereusstream.delay.protocol.ProtocolTuple.targetSelfContainedInlinePayloadReclaim())
                        &&!activation.body().payload().descriptor().supportsSelfContainedInlinePayloadReclaim()
                    ||tuple.equals(com.nereusstream.delay.protocol.ProtocolTuple.targetDuplicatePositionReclaim())
                        &&!activation.body().payload().descriptor().supportsDuplicatePositionReclaim()
                    ||tuple.equals(com.nereusstream.delay.protocol.ProtocolTuple.targetIncarnationReclaim())
                        &&!activation.body().payload().descriptor().supportsIncarnationReclaim()
                    ||tuple.equals(com.nereusstream.delay.protocol.ProtocolTuple.targetAnchorAllocationReclaim())
                        &&!activation.body().payload().descriptor().supportsAnchorAllocationReclaim()
                    ||tuple.equals(com.nereusstream.delay.protocol.ProtocolTuple.targetUncommittedObjectReclaim())
                        &&!activation.body().payload().descriptor().supportsUncommittedObjectPayloadReclaim()
                    ||tuple.equals(com.nereusstream.delay.protocol.ProtocolTuple.targetObjectPayloadReclaim())
                        &&!activation.body().payload().descriptor().supportsObjectPayloadReclaim()
                    ||tuple.equals(com.nereusstream.delay.protocol.ProtocolTuple.targetNativeRetainedReclaim())
                        &&!activation.body().payload().descriptor().supportsNativeRetainedReclaim())
                return Boolean.FALSE;
            return Boolean.TRUE;
        },workerReads(clock));
    }
    synchronized void requireRetirementProtocolActive(LongSupplier clock){
        requireRetirementProtocolActive(null,clock);
    }
    synchronized void requireRetirementProtocolActive(
            com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource resource,LongSupplier clock){
        requireRetirementProtocolActive(resource,false,clock);
    }
    private void requireRetirementProtocolActive(com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource resource,boolean marker,LongSupplier clock){
        requireGcOwner(clock);final var tuple=com.nereusstream.delay.protocol.ProtocolTuple.targetEvidenceRetirement();
        if(resource!=null&&(!scope.shard().equals(resource.shard())||!Arrays.equals(scope.tenantScope(),resource.tenantScope())
                ||!Arrays.equals(lineage,resource.recoveryLineage())))
            throw new IllegalArgumentException("retirement writer changed actual Source SHARD/tenant/lineage");
        final var compatible=store.controlSnapshot();
        if(compatible==null||!compatible.protocolTuples().contains(tuple))throw new IllegalStateException("retirement writer lacks independently admitted compatible tuple");
        backend.guardedRead(new BoundedReadBudget(limits.records(),limits.bytes(),limits.elapsedNanos(),monotonicClock),reader->{
            final var activation=com.nereusstream.delay.runtime.TargetProtocolActivationStore.applied(reader,scope,lineage,tuple);
            if(activation==null||!activation.body().payload().descriptor().supportsAcceptedHistoryTip())
                throw new IllegalStateException("retirement writer requires actual Source APPLIED descriptor3/Native55 capability");
            if(resource!=null&&resource.provenance() instanceof com.nereusstream.delay.protocol.TargetExternalEvidenceObjectResource.ReservedStaging
                    &&!activation.body().payload().descriptor().supportsCapacityOwnerStaging())
                throw new IllegalStateException("staging writer requires actual Source APPLIED descriptor4 capacity-root capability");
            if(marker&&!activation.body().payload().descriptor().supportsRetainedMarkerConfirmation())
                throw new IllegalStateException("marker confirmation writer requires actual APPLIED descriptor5 capability");
            return Boolean.TRUE;
        },workerReads(clock));
    }

    synchronized OptionalLong readTargetActiveMessages(
            final BoundedReadBudget budget, final TargetPartitionId target, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetQueueSnapshotReader(backend, limits.domains())
                .readActiveMessages(budget, target, workerReads(clock));
    }
    synchronized java.util.Optional<com.nereusstream.delay.runtime.TargetMessageMetadataStore.Snapshot> readMessageMetadata(
            BoundedReadBudget budget,com.nereusstream.delay.protocol.DelayMessageId id,LongSupplier ownerClock){
        requireGcOwner(ownerClock);
        return new com.nereusstream.delay.runtime.TargetMessageMetadataStore(backend,scope,lineage).read(budget,id,workerReads(ownerClock));
    }

    synchronized TargetQueueSnapshotReader.Cut readTargetQueueCut(
            final BoundedReadBudget budget, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetQueueSnapshotReader(backend, limits.domains()).readCut(budget, workerReads(clock));
    }

    synchronized TargetExpiryDiscoveryStore.Discovery discoverMessageExpiry(
            final BoundedReadBudget budget,
            final TargetExpiryDiscoveryStore.Cursor cursor,
            final TrustedUtcIntervalEvidence evidence,
            final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return expiryDiscovery.discover(budget, cursor, evidence, workerReads(clock));
    }

    synchronized void requireExpirySubmission(
            final TargetExpiryDiscoveryStore.Candidate candidate,
            final TrustedUtcIntervalEvidence evidence,
            final OwnerIdentity owner,
            final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var work = Objects.requireNonNull(candidate, "candidate");
        final var proof = Objects.requireNonNull(evidence, "evidence");
        final var exactOwner = Objects.requireNonNull(owner, "owner");
        if (!scope.shard().equals(work.locator().messageId().routingId().shardId())
                || exactOwner.ownerEpoch() != lease.ownerEpoch()
                || !Bytes.constantTimeEquals(
                        exactOwner.leaseFencingDigest(), Bytes.sha256(lease.leaseToken()))) {
            throw new IllegalArgumentException("Target expiry candidate or Owner differs from the active Shard");
        }
        proof.requireEarliestAtLeast(work.expireAtEpochMs());
    }

    synchronized void requireExpiryAuthoritativelyStrict(
            final TargetExpiryDiscoveryStore.Candidate candidate,
            final TrustedUtcIntervalEvidence evidence,
            final OwnerIdentity owner,
            final LongSupplier ownerClock) {
        requireExpirySubmission(candidate, evidence, owner, ownerClock);
    }

    synchronized void requireCurrentExpiryLogPosition(
            final SourcePosition position,
            final Long sourceConnectionGeneration,
            final byte[] guardAttestationDigest,
            final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        try {
            requireGcOwner(clock);
            final var persisted = Objects.requireNonNull(position, "persisted Source Position");
            if (!scope.shard().equals(persisted.shardId())) {
                throw new IllegalStateException("Target expiry appender returned a foreign Shard position");
            }
            assignment.activationBarrier().validatePosition(persisted);
            if (persisted instanceof PulsarSourcePosition) {
                if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier barrier)
                        || sourceConnectionGeneration == null
                        || guardAttestationDigest == null) {
                    throw new IllegalStateException("Target expiry append lacks a Pulsar source connection proof");
                }
                barrier.validateSourceConnection(sourceConnectionGeneration, guardAttestationDigest);
            } else if (sourceConnectionGeneration != null || guardAttestationDigest != null) {
                throw new IllegalStateException("Kafka Target expiry append cannot carry a Pulsar connection proof");
            }
            final var applied =
                    Objects.requireNonNull(store.appliedShardLogPosition(), "initialized Target source position");
            if (!applied.sameSourceIdentity(persisted)) {
                throw new IllegalStateException("Target expiry position belongs to another assigned source");
            }
        } catch (RuntimeException | Error failure) {
            fence();
            throw failure;
        }
    }

    synchronized boolean expiryAppendApplied(final SourcePosition position, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var current = Objects.requireNonNull(store.appliedShardLogPosition(), "initialized Target source");
        return current.compareTo(Objects.requireNonNull(position, "expiry source position")) >= 0;
    }

    synchronized Optional<SystemMutationResult> expiryMutationResult(
            final com.nereusstream.delay.protocol.SystemMutation mutation, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        final var exact = Objects.requireNonNull(mutation, "mutation");
        if (exact.type() != SystemMutationType.EXPIRE_GENERATION) {
            throw new IllegalArgumentException("Target expiry reconciliation requires EXPIRE_GENERATION");
        }
        return replay.appliedResult(
                new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock),
                exact,
                workerReads(clock));
    }

    /** Validates one current head and its frozen byte cost only when the Worker selects it. */
    synchronized TargetHeadCostProbe.Cost probeSelectedHead(
            final BoundedReadBudget budget, final TargetHeadRef selected, final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetHeadCostProbe(backend, scope, limits.domains()).probe(budget, selected, workerReads(clock));
    }

    /** Builds historical Native trust from this Owner's exact source-applied Store view. */
    public synchronized TargetNativePolicyTrustStore nativePolicyTrustStore(final LongSupplier ownerClock) {
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        requireGcOwner(clock);
        return new TargetNativePolicyTrustStore(
                backend,
                scope,
                lineage,
                limits.records(),
                limits.bytes(),
                limits.elapsedNanos(),
                monotonicClock,
                nativePolicyReads(clock));
    }

    private TargetStoreBackend.ReadAuthority workerReads(final LongSupplier clock) {
        return (actual, actualScope) -> {
            requireGcOwner(clock);
            return gcGuard(authorities.reads().acquire(actual, actualScope), actual, actualScope, clock);
        };
    }

    private TargetStoreBackend.ReadAuthority nativePolicyReads(final LongSupplier clock) {
        return (actual, actualScope) -> {
            requireGcOwner(clock);
            if (!scope.equals(actualScope) || !Arrays.equals(metadata.encode(), actual.encode())) {
                throw new IllegalStateException("Target Native trust belongs to another Store/scope");
            }
            final byte[] sourceFrontier = Objects.requireNonNull(
                            store.appliedShardLogPosition(), "initialized Target source")
                    .canonicalBytes();
            final var external = Objects.requireNonNull(
                    authorities.reads().acquire(actual, actualScope), "Target Native read guard");
            return new TargetStoreBackend.CommitGuard() {
                @Override
                public void requireCurrent() {
                    if (!scope.equals(actualScope) || !Arrays.equals(metadata.encode(), actual.encode())) {
                        throw new IllegalStateException("Target Native trust guard belongs to another Store/scope");
                    }
                    requireGcOwner(clock);
                    external.requireCurrent();
                    requireGcOwner(clock);
                    final var current =
                            Objects.requireNonNull(store.appliedShardLogPosition(), "initialized Target source");
                    if (!Arrays.equals(sourceFrontier, current.canonicalBytes())) {
                        throw new IllegalStateException("Target Native trust source frontier changed during read");
                    }
                }

                @Override
                public void close() {
                    external.close();
                }
            };
        };
    }

    /** Queues an unpublished candidate on this Owner's exact source/Store WorkClass graph. */
    public synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitLocalCheckpointCandidate(
            final WorkClassExecutionRegistry registry,
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Path checkpointPath,
            final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits) {
        return submitLocalCheckpointCandidate(
                registry,
                intents,
                ownerClock,
                checkpointPath,
                pending,
                physicalLimits,
                quotaLimits,
                ledgerLimits,
                () -> {});
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitLocalCheckpointCandidate(
            final WorkClassExecutionRegistry registry,
            final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock,
            final Path checkpointPath,
            final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final Runnable sourceCutGuard) {
        if (workClasses == null || workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalStateException("Target checkpoint requires the bound source WorkClass graph");
        }
        final LongSupplier clock = Objects.requireNonNull(ownerClock, "ownerClock");
        final Runnable cut = Objects.requireNonNull(sourceCutGuard, "sourceCutGuard");
        requireGcOwner(clock);
        cut.run();
        final var executor = new TargetCheckpointCandidateWorkClassExecutor(
                registry, store, authorities.leases(), intents, clock, () -> {
                    requireGcOwner(clock);
                    cut.run();
                });
        return executor.submit(new TargetCheckpointCandidateWorkClassExecutor.Request(
                checkpointPath, pending, lease, physicalLimits, quotaLimits, ledgerLimits));
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitSemanticCheckpointCandidate(
            final WorkClassExecutionRegistry registry, final CheckpointUploadIntentAuthority intents,
            final LongSupplier ownerClock, final Path checkpointPath, final CheckpointUploadIntent pending,
            final CheckpointManifestLimits physicalLimits,
            final TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            final TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            final com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
            final com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            final Runnable sourceCutGuard, final Runnable protectionGuard) {
        return submitSemanticCheckpointCandidate(registry, intents, ownerClock, checkpointPath, pending,
                physicalLimits, quotaLimits, ledgerLimits, snapshot, semanticLimits, sourceCutGuard,
                protectionGuard, null, null);
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitSemanticCheckpointCandidate(
            WorkClassExecutionRegistry registry, CheckpointUploadIntentAuthority intents, LongSupplier ownerClock,
            Path checkpointPath, CheckpointUploadIntent pending, CheckpointManifestLimits physicalLimits,
            TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot snapshot,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            Runnable sourceCutGuard, Runnable protectionGuard,
            TargetCheckpointCandidateWorkClassExecutor.PublicationPlan publication, SchedulerBudget settlementBudget) {
        if (workClasses == null || workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalStateException("Target semantic candidate requires the bound source WorkClass graph");
        }
        final var clock = Objects.requireNonNull(ownerClock, "ownerClock");
        final var cut = Objects.requireNonNull(sourceCutGuard, "sourceCutGuard");
        final var protection = Objects.requireNonNull(protectionGuard, "protectionGuard");
        final Runnable guard = () -> {
            requireGcOwner(clock);
            cut.run();
            protection.run();
        };
        guard.run();
        final var executor = new TargetCheckpointCandidateWorkClassExecutor(
                registry, store, authorities.leases(), intents, clock, guard, settlementBudget);
        final var request = new TargetCheckpointCandidateWorkClassExecutor.SemanticRequest(
                new TargetCheckpointCandidateWorkClassExecutor.Request(checkpointPath, pending, lease,
                        physicalLimits, quotaLimits, ledgerLimits), snapshot, semanticLimits, guard);
        return publication == null ? executor.submitSemantic(request)
                : executor.submitSemanticPublication(request, publication);
    }

    synchronized TargetCheckpointCandidateWorkClassExecutor.Submission submitCollectedSemanticCheckpointCandidate(
            WorkClassExecutionRegistry registry, CheckpointUploadIntentAuthority intents, LongSupplier ownerClock,
            Path checkpointPath, CheckpointUploadIntent pending, CheckpointManifestLimits physicalLimits,
            TargetCheckpointRootVerifier.QuotaAuditLimits quotaLimits,
            TargetCheckpointRootVerifier.LedgerAuditLimits ledgerLimits,
            Supplier<Optional<com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot>> inputs,
            com.nereusstream.delay.store.TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            Runnable sourceCutGuard, Runnable protectionGuard,
            TargetCheckpointCandidateWorkClassExecutor.PublicationPlan publication, SchedulerBudget settlementBudget,
            TargetCheckpointCandidateWorkClassExecutor.IntentRegistration registration) {
        if (workClasses == null || workClasses != Objects.requireNonNull(registry, "registry")) {
            throw new IllegalStateException("Target semantic collection requires the bound source WorkClass graph");
        }
        final Runnable guard = () -> {
            requireGcOwner(ownerClock);
            sourceCutGuard.run();
            protectionGuard.run();
        };
        guard.run();
        final var executor = new TargetCheckpointCandidateWorkClassExecutor(
                registry, store, authorities.leases(), intents, ownerClock, guard, settlementBudget);
        return executor.submitCollectedSemantic(new TargetCheckpointCandidateWorkClassExecutor.Request(
                checkpointPath, pending, lease, physicalLimits, quotaLimits, ledgerLimits), inputs, semanticLimits,
                guard, publication, registration);
    }

    OxiaOwnerLeaseStore drainAuthority() {
        return authorities.leases();
    }

    synchronized OwnerLease requireActiveDrainLease(final LongSupplier clock) {
        requireGcOwner(Objects.requireNonNull(clock, "clock"));
        return lease;
    }

    synchronized OwnerLease snapshotDrainLease() {
        return lease;
    }

    private TargetStoreBackend.CommitGuard gcGuard(
            final TargetStoreBackend.CommitGuard external,
            final StoreMetadata actual,
            final TargetQuotaScope actualScope,
            final LongSupplier clock) {
        final var delegate = Objects.requireNonNull(external, "Close GC authority guard");
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                if (!scope.equals(actualScope) || !Arrays.equals(metadata.encode(), actual.encode())) {
                    throw new IllegalStateException("Close GC guard belongs to another Store/scope");
                }
                requireGcOwner(clock);
                delegate.requireCurrent();
                requireGcOwner(clock);
            }

            @Override
            public void close() {
                delegate.close();
            }
        };
    }

    private void requireGcOwner(final LongSupplier clock) {
        try {
            if (recoveryOnly) { throw new IllegalStateException("Target recovery cannot perform active maintenance"); }
            requireOwner(clock);
        } catch (RuntimeException | Error failure) {
            fence();
            throw failure;
        }
    }

    @Override
    synchronized void requireSubmission(final SourceReplayEntry entry, final boolean recovery) {
        if (fenced || recovery != recoveryOnly) {
            throw new IllegalStateException("Target source lifecycle does not match its apply submission");
        }
        validateEntry(entry);
        sourceSubmissionStarted = true;
    }

    @Override
    synchronized SourceReplayOutcome apply(
            final SourceReplayEntry entry, final LongSupplier clock, final boolean recovery) {
        try {
            requireSubmission(entry, recovery);
            requireOwner(clock);
            if (entry instanceof SourceReplayRecord command) {
                return applyCommand(command, clock);
            }
            final var mutation = (SourceReplayMutation) entry;
            final var budget =
                    new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock);
            final TargetSystemReplayStore.Prepared duplicate;
            // Only backend preparation yields are retryable. External resolution and commit failures must fence.
            try {
                duplicate = replay.prepareIfPresent(budget, mutation.mutation(), entry.position())
                        .orElse(null);
            } catch (ReadIncompleteException incomplete) {
                throw new ReadYield(incomplete);
            }
            final SystemMutationResult result;
            if (duplicate != null) {
                result = replay.commit(
                        duplicate, writes(authorities.duplicateWrites(), entry, clock), reads(entry, clock));
            } else if (mutation.mutation().type() == SystemMutationType.TIME_FENCE) {
                final var control = Objects.requireNonNull(authorities.fences().resolve(mutation), "fence control");
                final TargetTimeFenceStore.Prepared first;
                try {
                    first = fences.prepareFirst(budget, mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = fences.commit(first, writes(control.commit(), entry, clock));
            } else if (mutation.mutation().type() == SystemMutationType.EXPIRE_GENERATION) {
                final var control =
                        Objects.requireNonNull(authorities.expiries().resolve(mutation), "Target expiry control");
                final TargetExpireGenerationStore.Prepared first;
                try {
                    first = expiries.prepareFirst(budget, mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = expiries.commit(first, writes(control.commit(), entry, clock));
            } else if (mutation.mutation().type() == SystemMutationType.TARGET_PUBLISH_ADMISSION) {
                final var control = Objects.requireNonNull(
                        authorities.admissions().resolve(mutation), "Target Publish Admission authority");
                final TargetPublishAdmissionStore.Prepared first;
                try {
                    first = targetAdmissions.prepareFirst(
                            budget, mutation.mutation(), entry.position(), control.authority(),control.samples());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = targetAdmissions.commit(first, writes(control.commit(), entry, clock));
            } else if (mutation.mutation().type() == SystemMutationType.PUBLISH_OUTCOME
                    || mutation.mutation().type() == SystemMutationType.EVIDENCE_RESOLUTION) {
                final var control = Objects.requireNonNull(
                        authorities.outcomes().resolve(mutation), "Target Publish Outcome authority");
                final TargetPublishOutcomeStore.Prepared first;
                try {
                    first = targetOutcomes.prepareFirst(
                            budget, mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = targetOutcomes.commit(first, writes(control.commit(), entry, clock));
            } else if(mutation.mutation().type()==SystemMutationType.TARGET_RESOURCE_RECLAIM){
                final var reclaim=new com.nereusstream.delay.runtime.TargetResourceReclaimStore(backend,scope,lineage,limits.counters(),limits.domains());
                final ReclaimControl control;
                final com.nereusstream.delay.runtime.TargetResourceReclaimStore.Prepared first;
                try{
                    control=!reclaim.requiresInputs(budget,mutation.mutation(),entry.position())
                            ?new ReclaimControl(new com.nereusstream.delay.runtime.TargetResourceReclaimStore.Authority(){
                                public com.nereusstream.delay.runtime.TargetResourceReclaimStore.SourceHistory history(TargetQuotaScope actual,SystemMutation input,SourcePosition source){
                                    throw new TargetSourceAuthorityPreparation.Required();
                                }
                                public com.nereusstream.delay.runtime.TargetResourceReclaimStore.Protection protect(TargetQuotaScope actual,SystemMutation input,SourcePosition source,
                                        com.nereusstream.delay.protocol.TargetRetirementAuthorizationHistory.Grant grant){throw new TargetSourceAuthorityPreparation.Required();}
                            },authorities.duplicateWrites())
                            :Objects.requireNonNull(authorities.reclaims().resolve(mutation),"original Native reclaim authority");
                    first=reclaim.prepareFirst(budget,mutation.mutation(),entry.position(),control.authority());
                }catch(ReadIncompleteException incomplete){throw new ReadYield(incomplete);}
                result=reclaim.commit(first,writes(control.commit(),entry,clock));
            } else if (mutation.mutation().type() == SystemMutationType.RESOURCE_RETIRE_INTENT||mutation.mutation().type()==SystemMutationType.RESOURCE_DELETE_CONFIRMED) {
                // This operation needs four counters; existing smaller runtime limits retain their old scope.
                final var retirement = new com.nereusstream.delay.runtime.TargetEvidenceRetirementStore(
                        backend, scope, lineage, limits.counters(), limits.domains());
                final RetirementControl control;
                try {
                    control = mutation.mutation().type() == SystemMutationType.RESOURCE_DELETE_CONFIRMED
                                    && !retirement.requiresConfirmationInputs(budget, mutation.mutation(), entry.position())
                            ? new RetirementControl(new com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.Authority() {
                                public com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.Approval resolve(
                                        TargetQuotaScope actual, AuthorIdentity actor, SystemMutation input, SourcePosition position) {
                                    throw new TargetSourceAuthorityPreparation.Required();
                                }
                                public com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.SourceHistory history(
                                        TargetQuotaScope actual, AuthorIdentity actor, SystemMutation input, SourcePosition position) {
                                    throw new TargetSourceAuthorityPreparation.Required();
                                }
                            }, authorities.duplicateWrites())
                            : Objects.requireNonNull(authorities.retirements().resolve(mutation), "Target evidence retirement authority");
                } catch (ReadIncompleteException incomplete) { throw new ReadYield(incomplete); }
                final com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.Prepared first;
                try {
                    first = mutation.mutation().type()==SystemMutationType.RESOURCE_RETIRE_INTENT
                            ?retirement.prepareFirst(budget, mutation.mutation(), entry.position(), control.authority())
                            :retirement.prepareConfirmationFirst(budget,mutation.mutation(),entry.position(),control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = retirement.commit(first, writes(control.commit(), entry, clock));
            } else if (isProtocolActivation(mutation.mutation())) {
                final var control=Objects.requireNonNull(authorities.protocolActivations().resolve(mutation),"Target protocol activation authority");
                final var activations=new com.nereusstream.delay.runtime.TargetProtocolActivationStore(backend,scope,lineage,limits.counters(),limits.domains());
                final com.nereusstream.delay.runtime.TargetProtocolActivationStore.Prepared first;
                try{first=activations.prepareFirst(budget,control.prepared(),mutation.mutation(),entry.position(),control.authority());}
                catch(ReadIncompleteException incomplete){throw new ReadYield(incomplete);}
                result=activations.commit(first,writes(control.commit(),entry,clock));
            } else if (isSloObservationControl(mutation.mutation())) {
                final var control = Objects.requireNonNull(authorities.sloObservations().resolve(mutation), "original Target SLO Control inputs");
                final var mode = new com.nereusstream.delay.runtime.TargetSloObservationStore(backend, scope, lineage, limits.counters(), limits.domains());
                final com.nereusstream.delay.runtime.TargetSloObservationStore.Prepared first;
                try { first = mode.prepareFirst(budget, control.prepared(), mutation.mutation(), entry.position(), control.authority()); }
                catch (ReadIncompleteException incomplete) { throw new ReadYield(incomplete); }
                result = mode.commit(first, writes(control.commit(), entry, clock));
            } else if (isChannelControl(mutation.mutation())) {
                final var control = Objects.requireNonNull(authorities.channelControls().resolve(mutation),
                        "Target channel lifecycle authority");
                final var actualOwner = control.authority().currentOwner();
                if (actualOwner.ownerEpoch() != lease.ownerEpoch()
                        || !Bytes.constantTimeEquals(actualOwner.leaseFencingDigest(), lease.leaseToken())) {
                    throw new IllegalStateException("channel authority belongs to another active Owner");
                }
                final com.nereusstream.delay.runtime.TargetChannelControlStore.Prepared first;
                try {
                    first = channelControls.prepareFirst(budget, control.prepared(), mutation.mutation(),
                            entry.position(), control.authority(), actualOwner);
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = channelControls.commit(first, writes(control.commit(), entry, clock));
            } else if (isTargetClose(mutation.mutation())) {
                final var control =
                        Objects.requireNonNull(authorities.closes().resolve(mutation), "Target Close control");
                final TargetCloseStore.Prepared first;
                try {
                    first = closes.prepareFirst(
                            budget, control.prepared(), mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = closes.commit(first, writes(control.commit(), entry, clock));
            } else if (isMembershipControl(mutation.mutation())) {
                final var control = Objects.requireNonNull(
                        authorities.membershipControls().resolve(mutation), "membership control");
                final TargetMembershipControlStore.Prepared first;
                try {
                    first = membershipControls.prepareFirst(
                            budget,
                            control.prepared(),
                            mutation.mutation(),
                            entry.position(),
                            control.physical(),
                            control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = membershipControls.commit(first, writes(control.commit(), entry, clock));
            } else if (isNativePolicyControl(mutation.mutation())) {
                final var control = Objects.requireNonNull(
                        authorities.nativePolicyControls().resolve(mutation), "Target Native policy control");
                final TargetNativePolicyControlStore.Prepared first;
                try {
                    first = nativePolicyControls.prepareFirst(
                            budget, control.prepared(), mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = nativePolicyControls.commit(first, writes(control.commit(), entry, clock));
            } else {
                final var control = Objects.requireNonNull(authorities.grants().resolve(mutation), "grant control");
                final TargetQuotaGrantStore.Prepared first;
                try {
                    first = grants.prepareFirst(
                            budget, control.prepared(), mutation.mutation(), entry.position(), control.authority());
                } catch (ReadIncompleteException incomplete) {
                    throw new ReadYield(incomplete);
                }
                result = grants.commit(first, writes(control.commit(), entry, clock));
            }
            // Broker ACK carries this physical record's anchor, while the Store retains the first logical result.
            return SourceReplayOutcome.systemMutation(
                    entry.position(),
                    new SystemMutationResult(
                            result.mutationId(),
                            result.mutationHash(),
                            result.mutationType(),
                            result.retryUntilEpochMs(),
                            result.authorIdentity(),
                            result.applyStatus(),
                            result.stableCode(),
                            entry.position().canonicalBytes()));
        } catch (ReadYield incomplete) {
            throw incomplete;
        } catch (TargetSourceAuthorityPreparation.Required | com.nereusstream.delay.runtime.TargetQuotaCapacitySnapshot.ViewChanged refresh) {
            authorityRefresh = true;
            throw refresh;
        } catch (RuntimeException | Error failure) {
            fence();
            throw failure;
        }
    }

    private SourceReplayOutcome applyCommand(final SourceReplayRecord entry, final LongSupplier clock) {
        final var budget =
                new BoundedReadBudget(limits.records(), limits.bytes(), limits.elapsedNanos(), monotonicClock);
        final TargetCommandReplayStore.Prepared prepared;
        try {
            prepared = commandReplay
                    .prepareReplayOrExpired(budget, entry.command(), entry.position())
                    .orElse(null);
        } catch (ReadIncompleteException incomplete) {
            throw new ReadYield(incomplete);
        }
        final CommandResult result;
        if (prepared != null) {
            result = commandReplay.commit(
                    prepared, writes(authorities.duplicateWrites(), entry, clock), reads(entry, clock));
        } else {
            // Resolve external snapshots outside the zero-write backend retry classifier.
            final var control = Objects.requireNonNull(authorities.commands().resolve(entry), "command control");
            final TargetCommandStore.Prepared first;
            try {
                first = commands.prepareFirst(
                        budget,
                        entry.command(),
                        entry.position(),
                        control.policy(),
                        (reader, binding, source) -> {
                            try {
                                return control.cancellations().closed(reader, binding, source);
                            } catch (ReadIncompleteException external) {
                                throw new IllegalStateException(
                                        "external cancellation authority did not complete", external);
                            }
                        },
                        control.reservationControls(),
                        control.schedules(),
                        control.payloadProofs(),
                        control.samples());
            } catch (ReadIncompleteException incomplete) {
                throw new ReadYield(incomplete);
            }
            result = commands.commit(first, writes(control.commit(), entry, clock));
        }
        return SourceReplayOutcome.command(
                entry.position(),
                new CommandResult(
                        result.applyStatus(),
                        result.stableCode(),
                        result.generation(),
                        result.stateVersion(),
                        result.messageStatus(),
                        entry.position().canonicalBytes()));
    }

    private static byte[] sourceDigest(final SourceReplayEntry entry) {
        if (entry instanceof SourceReplayRecord command) {
            return command.command().canonicalFrameDigest();
        }
        return Bytes.sha256(((SourceReplayMutation) entry).mutation().canonicalEnvelope());
    }

    private TargetStoreBackend.CommitAuthority writes(
            final TargetStoreBackend.CommitAuthority external,
            final SourceReplayEntry entry,
            final LongSupplier clock) {
        return (actual, actualScope, mutation) -> {
            final var stamp = mutation.quota().counters().mutation();
            if (stamp.isLocalMutation()
                    || !Arrays.equals(
                            stamp.source().canonicalBytes(), entry.position().canonicalBytes())
                    || !Arrays.equals(stamp.mutationDigest(), sourceDigest(entry))) {
                throw new IllegalStateException("Target source commit changed its exact source/envelope stamp");
            }
            requireOwner(clock);
            return guard(external.acquire(actual, actualScope, mutation), actual, actualScope, entry, clock);
        };
    }

    private TargetStoreBackend.ReadAuthority reads(final SourceReplayEntry entry, final LongSupplier clock) {
        return (actual, actualScope) -> {
            requireOwner(clock);
            return guard(authorities.reads().acquire(actual, actualScope), actual, actualScope, entry, clock);
        };
    }

    private TargetStoreBackend.CommitGuard guard(
            final TargetStoreBackend.CommitGuard external,
            final StoreMetadata actual,
            final TargetQuotaScope actualScope,
            final SourceReplayEntry entry,
            final LongSupplier clock) {
        Objects.requireNonNull(external, "external guard");
        return new TargetStoreBackend.CommitGuard() {
            @Override
            public void requireCurrent() {
                if (!scope.equals(actualScope) || !Arrays.equals(metadata.encode(), actual.encode())) {
                    throw new IllegalStateException("Target source guard belongs to another Store/scope");
                }
                validateEntry(entry);
                final SourcePosition prior =
                        Objects.requireNonNull(store.appliedShardLogPosition(), "initialized source");
                (activeSourceSuccessor == null ? authorities.successor() : activeSourceSuccessor).validate(prior, entry.position());
                external.requireCurrent();
                requireOwner(clock);
            }

            @Override
            public void close() {
                external.close();
            }
        };
    }

    @Override
    synchronized void beforeAcknowledgement(
            final SourceReplayEntry entry, final SourceReplayOutcome outcome, final LongSupplier clock) {
        try (var guard = reads(entry, clock).acquire(store.metadata(), scope)) {
            guard.requireCurrent();
            if (!Arrays.equals(
                    store.appliedShardLogPosition().canonicalBytes(),
                    entry.position().canonicalBytes())) {
                throw new IllegalStateException("Target ACK no longer names the current durable source");
            }
        } catch (RuntimeException | Error failure) {
            fence();
            throw failure;
        }
    }

    private void requireOwner(final LongSupplier clock) {
        if (recoveryOnly) { recoveryProtection.run(); }
        final OwnerLease initialLease = lease;
        if (fenced || store.runtimeMetadata().lastOpenedOwnerEpoch() != initialLease.ownerEpoch()) {
            throw new IllegalStateException("Target source Owner/Store is fenced");
        }
        final LongSupplier ownerClock = Objects.requireNonNull(clock, "ownerClock");
        final long started = monotonicClock.getAsLong();
        for (int attempt = 0; attempt < 4; attempt++) {
            requireOwnerReadDeadline(started);
            final OwnerLease observedLease = lease;
            final OwnerLease current = authorities.leases().current(scope.shard()).orElse(null);
            requireOwnerReadDeadline(started);
            synchronized (ownerStateLock) {
                final OwnerLease activeLease = lease;
                if (fenced || !activeLease.sameIdentity(initialLease)) {
                    throw new IllegalStateException("Target source Owner/Store is fenced");
                }
                if (current == null
                        || !activeLease.sameIdentity(current)
                        || current.state() != (recoveryOnly ? ShardLifecycleState.CATCHING_UP
                                : ShardLifecycleState.ACTIVE_FOR_COMMANDS)) {
                    throw new IllegalStateException("Target source authoritative lease/time changed");
                }
                if (current.expiresAtEpochMs() < activeLease.expiresAtEpochMs()) {
                    if (activeLease != observedLease) {
                        continue;
                    }
                    throw new IllegalStateException("Target source authoritative lease/time changed");
                }
                final long now = ownerClock.getAsLong();
                if (now < 0 || now < lastOwnerTime || !current.validAt(now)) {
                    throw new IllegalStateException("Target source authoritative lease/time changed");
                }
                lastOwnerTime = now;
                if (current.expiresAtEpochMs() > activeLease.expiresAtEpochMs()) {
                    lease = current;
                }
                return;
            }
        }
        throw new IllegalStateException("Target Source Owner renewal raced beyond its finite reread budget");
    }

    private void requireOwnerReadDeadline(long started) {
        final long elapsed = monotonicClock.getAsLong() - started;
        if (elapsed < 0 || elapsed >= limits.elapsedNanos()) {
            throw new IllegalStateException("Target Source authoritative Owner check exceeded its finite operation deadline");
        }
    }

    synchronized void requireRecoveryOwner(final LongSupplier clock) {
        if (!recoveryOnly) { throw new IllegalStateException("Target runtime is not a recovery target"); }
        requireOwner(clock);
    }

    private void validateEntry(final SourceReplayEntry entry) {
        Objects.requireNonNull(entry, "entry");
        assignment.activationBarrier().validatePosition(entry.position());
        if (entry.position() instanceof PulsarSourcePosition) {
            if (!(assignment.activationBarrier() instanceof PulsarActivationBarrier barrier)
                    || entry.sourceConnectionGeneration() == null
                    || entry.guardAttestationDigest() == null) {
                throw new IllegalArgumentException("Target source lacks a Pulsar connection proof");
            }
            if (recoveryReader == null) {
                barrier.validateSourceConnection(entry.sourceConnectionGeneration(), entry.guardAttestationDigest());
            } else {
                recoveryReader.requireReaderEntry(entry, assignment, recoveryReaderCut);
            }
        } else if (entry.sourceConnectionGeneration() != null || entry.guardAttestationDigest() != null) {
            throw new IllegalArgumentException("Kafka source cannot carry Pulsar connection proof");
        }
        if (entry instanceof SourceReplayRecord) {
            return;
        }
        if (entry instanceof SourceReplayMutation fence && fence.mutation().type() == SystemMutationType.TIME_FENCE) {
            return;
        }
        if (entry instanceof SourceReplayMutation expiry
                && expiry.mutation().type() == SystemMutationType.EXPIRE_GENERATION) {
            TargetExpireGenerationBody.decode(expiry.mutation().canonicalBody());
            return;
        }
        if (entry instanceof SourceReplayMutation admission
                && admission.mutation().type() == SystemMutationType.TARGET_PUBLISH_ADMISSION) {
            TargetPublishAdmissionBody.decode(admission.mutation().canonicalBody());
            return;
        }
        if (entry instanceof SourceReplayMutation outcome
                && (outcome.mutation().type() == SystemMutationType.PUBLISH_OUTCOME
                        || outcome.mutation().type() == SystemMutationType.EVIDENCE_RESOLUTION)) {
            if (outcome.mutation().type() == SystemMutationType.EVIDENCE_RESOLUTION) {
                PublishOutcomeBody.decodeEvidenceResolution(outcome.mutation().canonicalBody());
            } else {
                PublishOutcomeBody.decode(outcome.mutation().canonicalBody());
            }
            return;
        }
        if (entry instanceof SourceReplayMutation retirement
                && retirement.mutation().type() == SystemMutationType.RESOURCE_RETIRE_INTENT) {
            com.nereusstream.delay.protocol.TargetEvidenceRetireIntentBody.decode(retirement.mutation().canonicalBody());
            return;
        }
        if(entry instanceof SourceReplayMutation confirmation&&confirmation.mutation().type()==SystemMutationType.RESOURCE_DELETE_CONFIRMED){
            com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.decode(confirmation.mutation().canonicalBody());return;
        }
        if(entry instanceof SourceReplayMutation reclaim&&reclaim.mutation().type()==SystemMutationType.TARGET_RESOURCE_RECLAIM){
            com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(reclaim.mutation().canonicalBody());return;
        }
        if (!(entry instanceof SourceReplayMutation mutation)
                || mutation.mutation().type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            throw new IllegalArgumentException(
                    "Target source business branch is not wired yet; source must remain pending");
        }
        if(isProtocolActivation(mutation.mutation())){
            com.nereusstream.delay.protocol.TargetProtocolActivationBody.decode(mutation.mutation().canonicalBody());
        }else if (isSloObservationControl(mutation.mutation())) {
            com.nereusstream.delay.protocol.TargetSloObservationControlBody.decode(mutation.mutation().canonicalBody());
        }else if (isChannelControl(mutation.mutation())) {
            TargetChannelControlBody.decode(mutation.mutation().canonicalBody());
        } else if (isTargetClose(mutation.mutation())) {
            TargetCloseBody.decode(mutation.mutation().canonicalBody());
        } else if (isMembershipControl(mutation.mutation())) {
            TargetMembershipControlBody.decode(mutation.mutation().canonicalBody());
        } else if (isNativePolicyControl(mutation.mutation())) {
            TargetNativePolicyControlBody.decode(mutation.mutation().canonicalBody());
        } else {
            TargetQuotaGrantControlBody.decode(mutation.mutation().canonicalBody());
        }
    }

    /** Bounded structural discriminator only; the selected full decoder must still validate every field. */
    private static boolean isTargetClose(com.nereusstream.delay.protocol.SystemMutation mutation) {
        return targetControlKind(mutation) == TargetCloseRequest.CONTROL_KIND;
    }

    private static boolean isMembershipControl(com.nereusstream.delay.protocol.SystemMutation mutation) {
        final int kind = targetControlKind(mutation);
        return kind == 15 || kind == 16;
    }

    private static boolean isProtocolActivation(com.nereusstream.delay.protocol.SystemMutation mutation){
        return mutation.type()==SystemMutationType.APPLY_SHARD_CONTROL&&targetControlKind(mutation)==1
                &&com.nereusstream.delay.protocol.TargetProtocolActivationBody.hasTargetPayload(mutation.canonicalBody());
    }
    private static boolean isNativePolicyControl(com.nereusstream.delay.protocol.SystemMutation mutation) {
        final int kind = targetControlKind(mutation);
        return kind >= 19 && kind <= 23;
    }

    private static boolean isSloObservationControl(final SystemMutation mutation) {
        if (mutation.type() != SystemMutationType.APPLY_SHARD_CONTROL) return false;
        final int kind = targetControlKind(mutation);
        return kind == com.nereusstream.delay.protocol.TargetSloObservationControlRequest.ENABLE_CONTROL_KIND
                || kind == com.nereusstream.delay.protocol.TargetSloObservationControlRequest.STOP_CONTROL_KIND;
    }

    private static boolean isChannelControl(final SystemMutation mutation) {
        final int kind = targetControlKind(mutation);
        return kind >= com.nereusstream.delay.protocol.TargetChannelControlRequest.CREATE_CONTROL_KIND
                && kind <= com.nereusstream.delay.protocol.TargetChannelControlRequest.STOP_CONTROL_KIND;
    }

    private static int targetControlKind(com.nereusstream.delay.protocol.SystemMutation mutation) {
        final byte[] body = mutation.canonicalBody();
        if (body.length
                > Math.max(com.nereusstream.delay.protocol.TargetSloObservationControlBody.MAX_CANONICAL_BYTES, Math.max(com.nereusstream.delay.protocol.TargetProtocolActivationBody.MAX_BYTES,Math.max(
                        TargetNativePolicyControlBody.MAX_CANONICAL_BYTES,
                        Math.max(
                                TargetMembershipControlBody.MAX_CANONICAL_BYTES,
                                Math.max(
                                        TargetCloseBody.MAX_CANONICAL_BYTES,
                                        TargetQuotaGrantControlBody.MAX_CANONICAL_BYTES)))))) {
            throw new IllegalArgumentException("Target control body exceeds activated codec bounds");
        }
        final var reader = new CanonicalProtobuf.Reader(body);
        for (int field = 0; field < 5 && reader.hasRemaining(); field++) {
            final var value = reader.next();
            if (value.number() == 11) {
                if (value.wireType() != 0) {
                    throw new IllegalArgumentException("Target control kind has a non-varint wire type");
                }
                return Math.toIntExact(value.unsignedValue());
            }
        }
        return -1;
    }

    @Override
    public synchronized void fence() {
        fenced = true;
        stopAuthorityPreparation();
    }

    public synchronized boolean fenced() {
        return fenced;
    }

    @Override
    boolean isReadIncomplete(final Throwable failure) {
        return failure instanceof ReadYield;
    }

    com.nereusstream.delay.store.BoundedReadBudget.Exhaustion sourceReadIncompleteReason(Throwable failure) {
        return failure instanceof ReadYield yield ? ((ReadIncompleteException) yield.getCause()).reason()
                : failure instanceof com.nereusstream.delay.runtime.HeadReadIncompleteException head ? head.reason() : null;
    }

    private static final class ReadYield extends RuntimeException {
        private static final long serialVersionUID = 1L;

        private ReadYield(final ReadIncompleteException cause) {
            super("Target source preparation yielded before commit", cause);
        }
    }
}
