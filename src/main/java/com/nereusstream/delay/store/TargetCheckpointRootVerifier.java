package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.RecoveryFloorRef;
import com.nereusstream.delay.protocol.CheckpointUploadIntent;
import com.nereusstream.delay.protocol.CheckpointUploadState;
import com.nereusstream.delay.protocol.RecoveryCandidateRef;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SourcePositionCodec;
import com.nereusstream.delay.protocol.TargetPartitionId;
import com.nereusstream.delay.protocol.TargetQuotaAggregate;
import com.nereusstream.delay.protocol.TargetQuotaBookkeeping;
import com.nereusstream.delay.protocol.TargetQuotaCounter;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaTotal;
import com.nereusstream.delay.protocol.TargetQuotaUsage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Read-only physical check of the format-2 root, bookkeeping and source frontier.
 *
 * <p>The optional quota and business-ledger scans establish only local projection consistency.
 * This does not authenticate the manifest control digest or make an image publishable or recoverable.</p>
 */
public final class TargetCheckpointRootVerifier {
    private static final int FIXED_VALUE_TYPE = 1;

    private TargetCheckpointRootVerifier() {}

    public record RootProof(
            StoreMetadata metadata,
            SourcePosition source,
            long mutationSequence,
            TargetQuotaIncarnation root,
            TargetQuotaBookkeeping bookkeeping,
            TargetQuotaAggregate aggregate) {
        public RootProof {
            Objects.requireNonNull(metadata, "metadata");
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(root, "root");
            Objects.requireNonNull(bookkeeping, "bookkeeping");
            Objects.requireNonNull(aggregate, "aggregate");
        }
    }

    /** Maximum decoded accounting records and aggregate key/value bytes for a recovery-only scan. */
    public record QuotaAuditLimits(int maxRecords, long maxKeyValueBytes) {
        public QuotaAuditLimits {
            if (maxRecords <= 0
                    || maxRecords == Integer.MAX_VALUE
                    || maxKeyValueBytes <= 0
                    || maxKeyValueBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Target quota audit requires finite positive limits");
            }
        }
    }

    /** Finite full-scan and dependency point-read limits for a recovery-only business ledger fold. */
    public record LedgerAuditLimits(int maxRecords, long maxKeyValueBytes, int maxPointReads, long maxPointReadBytes) {
        public LedgerAuditLimits {
            if (maxRecords <= 0
                    || maxRecords == Integer.MAX_VALUE
                    || maxKeyValueBytes <= 0
                    || maxKeyValueBytes == Long.MAX_VALUE
                    || maxPointReads <= 0
                    || maxPointReads == Integer.MAX_VALUE
                    || maxPointReadBytes <= 0
                    || maxPointReadBytes == Long.MAX_VALUE) {
                throw new IllegalArgumentException("Target ledger audit requires finite positive limits");
            }
        }
    }

    /** Reads an immutable RocksDB image under finite physical limits without changing its markers. */
    public static RootProof validate(
            final Path image, final ShardId expectedShard, final CheckpointManifestLimits limits) {
        return validateImage(image, expectedShard, limits, null, null, null, null).root();
    }

    /** Audits the bounded quota projection graph; independent work ledgers still require a separate fold. */
    public static RootProof auditQuotaProjections(
            final Path image,
            final ShardId expectedShard,
            final CheckpointManifestLimits physicalLimits,
            final QuotaAuditLimits quotaLimits) {
        return validateImage(
                image, expectedShard, physicalLimits, null, Objects.requireNonNull(quotaLimits), null, null).root();
    }

    /** Rebuilds quota usage from bounded actual business records after the quota projection audit. */
    public static RootProof auditIndependentLedger(
            final Path image,
            final ShardId expectedShard,
            final CheckpointManifestLimits physicalLimits,
            final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits) {
        return validateImage(
                image,
                expectedShard,
                physicalLimits,
                null,
                Objects.requireNonNull(quotaLimits, "quotaLimits"),
                Objects.requireNonNull(ledgerLimits, "ledgerLimits"),
                null).root();
    }

    /** Folds the live Store ledger inside the exact bounded read plan used to reconstruct its root. */
    public static void auditLiveStoreLedger(final ShardStore store, final RootProof proof,
            final LedgerAuditLimits limits, final BoundedReadBudget budget) {
        auditLiveStoreDependencies(store, proof, limits, budget);
    }

    /** Captures stored dependencies in that same complete live ledger fold. */
    public static TargetCheckpointDependencies auditLiveStoreDependencies(
            final ShardStore store,
            final RootProof proof,
            final LedgerAuditLimits limits,
            final BoundedReadBudget budget) {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(proof, "proof");
        Objects.requireNonNull(limits, "limits");
        Objects.requireNonNull(budget, "budget");
        if (!Thread.holdsLock(store)) {
            throw new IllegalStateException("Target live ledger audit requires the exact Store read plan");
        }
        store.requireActiveReadBudget(budget);
        final SourcePosition storeSource = store.appliedShardLogPosition();
        if (proof.metadata().storeFormatVersion() != store.metadata().storeFormatVersion()
                || !proof.metadata().shardId().equals(store.shardId())
                || !Bytes.constantTimeEquals(
                        proof.metadata().storeIncarnation(), store.metadata().storeIncarnation())
                || storeSource == null
                || !Arrays.equals(proof.source().canonicalBytes(), storeSource.canonicalBytes())
                || proof.mutationSequence() != store.shardMutationSequence()) {
            throw new IllegalArgumentException("Target live ledger proof does not match the active Store frontier");
        }
        return TargetCheckpointLedgerAudit.auditLive(store, budget, proof, limits);
    }

    /** Binds a complete local candidate image to the exact live Store cut without granting publication authority. */
    static RootProof auditLocalCandidate(
            final Path image,
            final ShardStore store,
            final byte[] expectedLineage,
            final byte[] checkpointId,
            final CheckpointManifestLimits physicalLimits,
            final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits) {
        return auditLocalCandidate(image, store, expectedLineage, checkpointId, physicalLimits,
                quotaLimits, ledgerLimits, null, null);
    }

    static RootProof auditLocalCandidate(final Path image, final ShardStore store, final byte[] expectedLineage,
            final byte[] checkpointId, final CheckpointManifestLimits physicalLimits,
            final QuotaAuditLimits quotaLimits, final LedgerAuditLimits ledgerLimits,
            final TargetCheckpointSemanticSnapshot snapshot,
            final TargetCheckpointSemanticSnapshot.Limits semanticLimits) {
        Objects.requireNonNull(store, "store");
        Bytes.requireLength(expectedLineage, 16, "expectedLineage");
        Bytes.requireLength(checkpointId, 16, "checkpointId");
        if (!Thread.holdsLock(store)) {
            throw new IllegalStateException("Target candidate audit requires the exact Store monitor");
        }
        final var runtime = store.runtimeMetadata();
        if (!Bytes.constantTimeEquals(runtime.lastCheckpointId(), checkpointId)
                || runtime.lastOpenedOwnerEpoch() == 0) {
            throw new IllegalStateException("Target candidate has no matching local checkpoint or opened Owner");
        }
        final var expected = new CandidateIdentity(
                store.metadata(),
                Bytes.copy(expectedLineage),
                store.appliedShardLogPosition(),
                store.shardMutationSequence(),
                store.latestSequenceNumber(),
                runtime);
        final var proof = validateImage(
                image,
                store.shardId(),
                physicalLimits,
                null,
                Objects.requireNonNull(quotaLimits, "quotaLimits"),
                Objects.requireNonNull(ledgerLimits, "ledgerLimits"),
                expected);
        if (snapshot != null) { snapshot.validateAgainst(proof.dependencies(), semanticLimits); }
        return proof.root();
    }

    /**
     * Binds the physical files and fixed image identity to a format-2 manifest. The manifest's
     * control-state and semantic-version digests still require separate authenticated validation.
     */
    public static RootProof validateManifestImageIdentity(
            final Path image, final CheckpointManifest manifest, final CheckpointManifestLimits limits) {
        requireFormat2Manifest(manifest, limits);
        return validateImage(image, manifest.shardId(), limits, manifest, null, null, null).root();
    }

    /** Binds Manifest identity and both local audits in one immutable image read. */
    public static RootProof auditManifestImageLedger(
            final Path image,
            final CheckpointManifest manifest,
            final CheckpointManifestLimits physicalLimits,
            final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits) {
        requireFormat2Manifest(manifest, physicalLimits);
        return validateImage(
                image,
                manifest.shardId(),
                physicalLimits,
                manifest,
                Objects.requireNonNull(quotaLimits, "quotaLimits"),
                Objects.requireNonNull(ledgerLimits, "ledgerLimits"),
                null).root();
    }

    /** A complete local ledger and its stored dependencies; neither proves external authentication or pins. */
    public record LedgerProof(RootProof root, TargetCheckpointDependencies dependencies) {}

    /** Inventories dependencies in the same immutable image read and finite complete ledger fold. */
    public static LedgerProof auditImageDependencies(final Path image, final ShardId expectedShard,
            final CheckpointManifestLimits physicalLimits, final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits) {
        return validateImage(image, expectedShard, physicalLimits, null,
                Objects.requireNonNull(quotaLimits, "quotaLimits"),
                Objects.requireNonNull(ledgerLimits, "ledgerLimits"), null);
    }

    /** Validates closed semantic contents against the exact fully audited immutable physical image. */
    public static LedgerProof auditImageSemanticSnapshot(final Path image, final ShardId expectedShard,
            final CheckpointManifestLimits physicalLimits, final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits, final TargetCheckpointSemanticSnapshot snapshot,
            final TargetCheckpointSemanticSnapshot.Limits semanticLimits) {
        final var proof = auditImageDependencies(image, expectedShard, physicalLimits, quotaLimits, ledgerLimits);
        Objects.requireNonNull(snapshot, "snapshot").validateAgainst(proof.dependencies(), semanticLimits);
        return proof;
    }

    /** Reads the durable companion and compares it with the complete immutable physical image ledger. */
    public static LedgerProof auditPersistedSemanticSnapshot(final Path image, final ShardId expectedShard,
            final CheckpointManifestLimits physicalLimits, final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits, final TargetCheckpointSemanticSnapshot.Limits semanticLimits) {
        final var snapshot = TargetCheckpointSemanticSnapshotFile.read(image, semanticLimits);
        return auditImageSemanticSnapshot(image, expectedShard, physicalLimits, quotaLimits, ledgerLimits,
                snapshot, semanticLimits);
    }

    /**
     * Binds a complete format-2 Manifest to the persisted schema-2 companion and physical ledger. The control-state
     * digest is the complete companion digest; the semantic-version digest is its Target dependency inventory.
     * This is a bounded read of an immutable image, not original-Source authentication or publication/install rights.
     */
    public static LedgerProof auditManifestSemanticSnapshot(final Path image, final CheckpointManifest manifest,
            final CheckpointManifestLimits physicalLimits, final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits, final TargetCheckpointSemanticSnapshot.Limits semanticLimits) {
        return auditManifestCompanion(image, manifest, physicalLimits, quotaLimits, ledgerLimits, semanticLimits)
                .ledger();
    }

    /** Original Control/initial reader authentication after the same immutable Manifest/companion image audit. */
    public static LedgerProof auditManifestControlAuthentication(final Path image, final CheckpointManifest manifest,
            final CheckpointManifestLimits physicalLimits, final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits, final TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            final TargetCheckpointControlAuthority controls,
            final com.nereusstream.delay.runtime.TargetStoreBootstrap.InitialControlAuthority initialAuthority) {
        Objects.requireNonNull(controls, "controls");
        Objects.requireNonNull(initialAuthority, "initialAuthority");
        final var verified = auditManifestCompanion(image, manifest, physicalLimits, quotaLimits, ledgerLimits,
                semanticLimits);
        controls.authenticateSnapshot(verified.ledger().dependencies(), verified.snapshot(), semanticLimits,
                initialAuthority);
        return verified.ledger();
    }

    /** Original materialized Admission authentication after the same immutable Manifest/companion image audit. */
    public static LedgerProof auditManifestAdmissionAuthentication(final Path image, final CheckpointManifest manifest,
            final CheckpointManifestLimits physicalLimits, final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits, final TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            final BoundedReadBudget authenticationBudget,
            final TargetCheckpointDependencies.AdmissionHistoryAuthority history,
            final com.nereusstream.delay.runtime.TargetPublishAdmissionVerifier.Authority authority,
            final Runnable protectionGuard) {
        Objects.requireNonNull(authenticationBudget, "authenticationBudget");
        Objects.requireNonNull(history, "history");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(protectionGuard, "protectionGuard");
        final var verified = auditManifestCompanion(image, manifest, physicalLimits, quotaLimits, ledgerLimits,
                semanticLimits);
        final var authenticated = verified.ledger().dependencies().authenticateAdmissionImages(
                verified.snapshot().admissions(), semanticLimits.maximumAdmissionFrameBytes(), authenticationBudget,
                history, authority, protectionGuard);
        return new LedgerProof(verified.ledger().root(), authenticated);
    }

    /** Original Profile semantic publication checks after the same immutable Manifest/companion image audit. */
    public static LedgerProof auditManifestProfileAuthentication(final Path image, final CheckpointManifest manifest,
            final CheckpointManifestLimits physicalLimits, final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits, final TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            final BoundedReadBudget authenticationBudget,
            final TargetCheckpointSemanticSnapshot.ProfileAuthority authority,
            final Runnable protectionGuard) {
        Objects.requireNonNull(authenticationBudget, "authenticationBudget");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(protectionGuard, "protectionGuard");
        final var verified = auditManifestCompanion(image, manifest, physicalLimits, quotaLimits, ledgerLimits,
                semanticLimits);
        verified.snapshot().authenticateProfiles(verified.ledger().dependencies(), semanticLimits,
                authenticationBudget, authority, protectionGuard);
        return verified.ledger();
    }

    /** Requires schema 3 and original Outcome provenance/Owner-service/key/time/evidence authentication. */
    public static LedgerProof auditManifestOutcomeAuthentication(final Path image, final CheckpointManifest manifest,
            final CheckpointManifestLimits physicalLimits, final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits, final TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            final BoundedReadBudget authenticationBudget,
            final TargetCheckpointDependencies.OutcomeImageAuthority authority, final Runnable protectionGuard) {
        Objects.requireNonNull(authenticationBudget, "authenticationBudget");
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(protectionGuard, "protectionGuard");
        final var verified = auditManifestCompanion(image, manifest, physicalLimits, quotaLimits, ledgerLimits,
                semanticLimits);
        verified.snapshot().authenticateOutcomes(verified.ledger().dependencies(), semanticLimits,
                authenticationBudget, authority, protectionGuard);
        return verified.ledger();
    }

    /** Independent actual-object/signature audit after the physical image fold; retained counter values are never inputs. */
    public static LedgerProof auditManifestExternalEvidenceAuthentication(Path image, CheckpointManifest manifest,
            CheckpointManifestLimits physicalLimits, QuotaAuditLimits quotaLimits, LedgerAuditLimits ledgerLimits,
            TargetCheckpointSemanticSnapshot.Limits semanticLimits, BoundedReadBudget authenticationBudget,
            TargetCheckpointDependencies.ExternalEvidenceAuthority authority, Runnable protectionGuard) {
        final var verified=auditManifestCompanion(image,manifest,physicalLimits,quotaLimits,ledgerLimits,semanticLimits);
        verified.snapshot().authenticateExternalEvidence(verified.ledger().dependencies(),semanticLimits,authority,
                authenticationBudget,protectionGuard);
        if(!verified.ledger().dependencies().recoveryEvidence().isEmpty())
            throw new IllegalStateException("recovery evidence requires complete purpose-signed proof authentication authority");
        return verified.ledger();
    }
    /** Full original CONTROL objects and purpose-signed recovery objects/seals on this exact immutable image cut. */
    public static LedgerProof auditManifestExternalEvidenceAuthentication(Path image,CheckpointManifest manifest,
            CheckpointManifestLimits physicalLimits,QuotaAuditLimits quotaLimits,LedgerAuditLimits ledgerLimits,
            TargetCheckpointSemanticSnapshot.Limits semanticLimits,BoundedReadBudget authenticationBudget,
            TargetCheckpointDependencies.ExternalEvidenceAuthority controlAuthority,
            TargetCheckpointDependencies.RecoveryEvidenceAuthority recoveryAuthority,Runnable protectionGuard){
        final var verified=auditManifestCompanion(image,manifest,physicalLimits,quotaLimits,ledgerLimits,semanticLimits);
        verified.snapshot().authenticateExternalEvidence(verified.ledger().dependencies(),semanticLimits,controlAuthority,authenticationBudget,protectionGuard);
        verified.snapshot().authenticateRecoveryEvidence(verified.ledger().dependencies(),semanticLimits,recoveryAuthority,authenticationBudget,protectionGuard);
        return verified.ledger();
    }

    private record ManifestCompanion(LedgerProof ledger, TargetCheckpointSemanticSnapshot snapshot) {}

    /** Must authenticate every original semantic/control/input/evidence and keep all image/Source/pin rights held. */
    @FunctionalInterface
    public interface ImageAuthority {
        void requireAuthenticated(CheckpointManifest manifest, LedgerProof ledger,
                TargetCheckpointSemanticSnapshot snapshot);
        default void requireLifecycleAuthenticated(CheckpointManifest manifest,LedgerProof proof,TargetCheckpointSemanticSnapshot snapshot){
            if(proof.dependencies().hasLifecycleInputs())throw new IllegalStateException("image authority lacks original Target lifecycle authentication");
        }
        default void requireTerminalPoliciesAuthenticated(CheckpointManifest manifest,LedgerProof proof,TargetCheckpointSemanticSnapshot snapshot){
            if(!snapshot.terminalPolicies().isEmpty())throw new IllegalStateException("image authority lacks original terminal-policy publication authentication");
        }
        /** Load/authenticate original RETAINED transitions and the committed closure TimeFence under this image's held Source history. */
        default com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts inlinePayloadSourceFacts(
                CheckpointManifest manifest,LedgerProof proof,TargetCheckpointSemanticSnapshot snapshot,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody body,long maximumBytes){
            throw new IllegalStateException("image authority has no original inline Source retention/fence inputs");
        }
        /**
         * Actual recovery/reader/Source-retention admission must keep this image from legally depending on the
         * current primary. Old readable/replayable views block unless their real protocol excludes activation
         * or requires complete replay through the closed safe cut before service. Historical authentication,
         * phase, ancestry alone and a caller's empty inventory do not satisfy this. The image protection must
         * hold these rights and actual read exits through the Native Source commit; this is no new authority.
         */
        default void requireInlinePayloadIndependent(CheckpointManifest manifest,LedgerProof proof,TargetCheckpointSemanticSnapshot snapshot,
                com.nereusstream.delay.protocol.TargetResourceReclaimBody body,RecoveryFloorRef actualFloor,
                com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts closure){
            throw new IllegalStateException("image has no real primary-payload reader/recovery independence proof");
        }
    }
    /** Preserves all existing image obligations and adds mandatory original g8 proof authentication. */
    public static ImageAuthority withLifecycleAuthority(ImageAuthority original,TargetCheckpointLifecycleInputs.Authority lifecycle,
            java.util.function.Supplier<BoundedReadBudget> budgets,TargetCheckpointSemanticSnapshot.Limits limits,Runnable protection){
        Objects.requireNonNull(original);Objects.requireNonNull(lifecycle);Objects.requireNonNull(budgets);Objects.requireNonNull(limits);Objects.requireNonNull(protection);
        return new ImageAuthority(){
            public void requireAuthenticated(CheckpointManifest manifest,LedgerProof proof,TargetCheckpointSemanticSnapshot snapshot){original.requireAuthenticated(manifest,proof,snapshot);}
            public void requireLifecycleAuthenticated(CheckpointManifest manifest,LedgerProof proof,TargetCheckpointSemanticSnapshot snapshot){
                protection.run();snapshot.authenticateLifecycle(proof.dependencies(),limits,lifecycle,Objects.requireNonNull(budgets.get()),protection);protection.run();
            }
            public void requireTerminalPoliciesAuthenticated(CheckpointManifest manifest,LedgerProof proof,TargetCheckpointSemanticSnapshot snapshot){
                original.requireTerminalPoliciesAuthenticated(manifest,proof,snapshot);
            }
            public com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts inlinePayloadSourceFacts(CheckpointManifest m,LedgerProof p,TargetCheckpointSemanticSnapshot s,
                    com.nereusstream.delay.protocol.TargetResourceReclaimBody b,long bytes){return original.inlinePayloadSourceFacts(m,p,s,b,bytes);}
            public void requireInlinePayloadIndependent(CheckpointManifest m,LedgerProof p,TargetCheckpointSemanticSnapshot s,
                    com.nereusstream.delay.protocol.TargetResourceReclaimBody b,RecoveryFloorRef f,com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts c){
                original.requireInlinePayloadIndependent(m,p,s,b,f,c);
            }
        };
    }

    /** Keeps the original whole-image and lifecycle authority; adds every original retry/content/use check. */
    public static ImageAuthority withTerminalPolicyAuthority(ImageAuthority original,com.nereusstream.delay.runtime.RetryPolicyCatalog policies,
            java.util.function.Supplier<BoundedReadBudget> budgets,TargetCheckpointSemanticSnapshot.Limits limits,Runnable protection){
        Objects.requireNonNull(original);Objects.requireNonNull(policies);Objects.requireNonNull(budgets);Objects.requireNonNull(limits);Objects.requireNonNull(protection);
        return new ImageAuthority(){
            public void requireAuthenticated(CheckpointManifest manifest,LedgerProof proof,TargetCheckpointSemanticSnapshot snapshot){original.requireAuthenticated(manifest,proof,snapshot);}
            public void requireLifecycleAuthenticated(CheckpointManifest manifest,LedgerProof proof,TargetCheckpointSemanticSnapshot snapshot){original.requireLifecycleAuthenticated(manifest,proof,snapshot);}
            public void requireTerminalPoliciesAuthenticated(CheckpointManifest manifest,LedgerProof proof,TargetCheckpointSemanticSnapshot snapshot){
                protection.run();snapshot.authenticateTerminalPolicies(proof.dependencies(),limits,Objects.requireNonNull(budgets.get()),policies,protection);protection.run();
            }
            public com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts inlinePayloadSourceFacts(CheckpointManifest m,LedgerProof p,TargetCheckpointSemanticSnapshot s,
                    com.nereusstream.delay.protocol.TargetResourceReclaimBody b,long bytes){return original.inlinePayloadSourceFacts(m,p,s,b,bytes);}
            public void requireInlinePayloadIndependent(CheckpointManifest m,LedgerProof p,TargetCheckpointSemanticSnapshot s,
                    com.nereusstream.delay.protocol.TargetResourceReclaimBody b,RecoveryFloorRef f,com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts c){
                original.requireInlinePayloadIndependent(m,p,s,b,f,c);
            }
        };
    }

    /** Original signed frames from an actual fully authenticated immutable image; not writer closure by itself. */
    public record UnusedReserveWriter(com.nereusstream.delay.protocol.TargetQuotaAttemptBudget expected,
            com.nereusstream.delay.protocol.SystemMutation admission,SourcePosition admissionSource,
            com.nereusstream.delay.protocol.SystemMutation outcome,SourcePosition outcomeSource){}
    /** Private receipt from a full authenticated physical image and real held reader/restore qualification. */
    public static final class InlinePayloadCopy {
        private final AuthenticatedImage image;
        private final byte[] envelope;
        private final List<com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.Policy> policies;
        private final com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts facts;
        private final com.nereusstream.delay.protocol.TargetQuotaMutation released;
        private InlinePayloadCopy(AuthenticatedImage image,com.nereusstream.delay.protocol.SystemMutation input,
                List<com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.Policy> policies,
                com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts facts,com.nereusstream.delay.protocol.TargetQuotaMutation released){
            this.image=image;envelope=Bytes.sha256(input.canonicalEnvelope());this.policies=List.copyOf(policies);this.facts=facts;this.released=released;
        }
        public CheckpointManifest manifest(){return image.manifest();}
        public List<com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.Policy> policies(){return policies;}
        public com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts facts(){return facts;}
        public com.nereusstream.delay.protocol.TargetQuotaMutation released(){return released;}
        public void requireFor(com.nereusstream.delay.protocol.SystemMutation input){requireCurrent();if(!Arrays.equals(envelope,Bytes.sha256(input.canonicalEnvelope())))throw new IllegalArgumentException("inline image changed original Source frame");}
        public void requireCurrent(){image.requireCurrent();}
        public long retainedBytes(){long bytes=Math.addExact(4096L,facts.retainedBytes());for(var policy:policies)
            bytes=Math.addExact(bytes,policy.use().canonicalBytes().length+policy.retry().canonicalBytes().length+policy.terminal().canonicalBytes().length+256L);return bytes;}
    }
    public static final class UnusedReserveCopy {
        private final AuthenticatedImage image;
        private final byte[] envelope;
        private final List<UnusedReserveWriter> writers;
        private final com.nereusstream.delay.protocol.TargetQuotaMutation retainedDecision;
        private final UnusedReserveCopy replayBasis;
        private UnusedReserveCopy(AuthenticatedImage image,byte[] envelope,List<UnusedReserveWriter> writers,com.nereusstream.delay.protocol.TargetQuotaMutation retainedDecision){
            this(image,envelope,writers,retainedDecision,null);}
        private UnusedReserveCopy(AuthenticatedImage image,byte[] envelope,List<UnusedReserveWriter> writers,com.nereusstream.delay.protocol.TargetQuotaMutation retainedDecision,UnusedReserveCopy replayBasis){
            this.image=image;this.envelope=envelope;this.writers=writers;this.retainedDecision=retainedDecision;this.replayBasis=replayBasis;}
        public List<UnusedReserveWriter> writers(){return writers;}
        public boolean containsRetainedDecision(){return retainedDecision!=null;}
        public boolean containsRetainedDecisionAt(SourcePosition source){return retainedDecision!=null&&retainedDecision.source().equals(source);}
        public com.nereusstream.delay.protocol.TargetQuotaMutation retainedDecision(){return retainedDecision;}
        public void requireFor(com.nereusstream.delay.protocol.SystemMutation input){
            if(!Arrays.equals(envelope,Bytes.sha256(input.canonicalEnvelope())))throw new IllegalArgumentException("unused-reserve copy changed exact Source frame");
            requireCurrent();
        }
        public void requireCurrent(){image.requireCurrent();if(replayBasis!=null)replayBasis.requireCurrent();}
        public long retainedBytes(){long bytes=Math.addExact(128L,retainedDecision==null?0:retainedDecision.canonicalBytes().length);for(var writer:writers)bytes=Math.addExact(bytes,
                Math.addExact(writer.expected().canonicalBytes().length,Math.addExact(writer.admission().encodeFrame().length,writer.outcome().encodeFrame().length+512L)));return bytes;}
    }

    /** Exact image and mandatory full external authentication, held until upload/Catalog/restore handoff ends. */
    public static final class AuthenticatedImage {
        private final Path image;
        private final CheckpointManifest manifest;
        private final CheckpointManifestLimits physical;
        private final QuotaAuditLimits quota;
        private final LedgerAuditLimits ledger;
        private final TargetCheckpointSemanticSnapshot.Limits semantic;
        private final ImageAuthority authority;
        private final Runnable protection;

        private AuthenticatedImage(Path image, CheckpointManifest manifest, CheckpointManifestLimits physical,
                QuotaAuditLimits quota, LedgerAuditLimits ledger, TargetCheckpointSemanticSnapshot.Limits semantic,
                ImageAuthority authority, Runnable protection) {
            this.image = image.toAbsolutePath().normalize();
            this.manifest = manifest;
            this.physical = physical;
            this.quota = quota;
            this.ledger = ledger;
            this.semantic = semantic;
            this.authority = authority;
            this.protection = protection;
        }

        public CheckpointManifest manifest() { return manifest; }
        public Path image() { return image; }

        /** Reauthenticates the complete immutable bytes; a previously good digest or callback is insufficient. */
        public void authenticate() {
            auditManifestImageAuthentication(image, manifest, physical, quota, ledger, semantic, authority, protection);
        }

        /** Bound decoded/canonical audit inputs before starting another complete authentication pass. */
        public long authenticationInputWindowBytes(){
            final long encoded=Math.addExact(physical.maxManifestBytes(),Math.addExact(quota.maxKeyValueBytes(),
                    Math.addExact(ledger.maxKeyValueBytes(),Math.addExact(ledger.maxPointReadBytes(),semantic.maximumBytes()))));
            return Math.multiplyExact(8L,encoded);
        }
        /** Preparation only: the complete original Source inputs are frozen before the signed request can escape. */
        com.nereusstream.delay.protocol.TargetResourceReclaimBody freezeInlinePayloadSourceFacts(
                com.nereusstream.delay.protocol.TargetResourceReclaimBody draft,long maximumBytes){
            if(!draft.inlinePayloadRelease()||draft.selfContainedInlinePayloadRelease()
                    ||!Arrays.equals(manifest.checkpointId(),draft.floor().checkpointId())
                    ||!Arrays.equals(manifest.manifestSha256(),draft.floor().manifestSha256()))throw new IllegalArgumentException("SourceFacts preparation requires exact current Floor");
            requireCurrent();final var audited=authenticateManifestCompanion(image,manifest,physical,quota,ledger,semantic,authority,protection);
            final var facts=Objects.requireNonNull(authority.inlinePayloadSourceFacts(manifest,audited.ledger(),audited.snapshot(),draft,maximumBytes),"original held inline Source inputs");
            if(facts.retainedBytes()>maximumBytes)throw new IllegalStateException("original SourceFacts exceed reserved preparation window");
            final var policies=com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.policies(audited.ledger().dependencies(),audited.snapshot(),draft,this::requireCurrent);
            com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.requireClosed(audited.ledger().dependencies(),draft,policies,facts,false,this::requireCurrent);
            final var body=draft.withSourceFacts(facts.freeze(audited.ledger().dependencies(),draft));
            if(body.canonicalBytes().length>maximumBytes)throw new IllegalStateException("complete self-contained request exceeds original preparation window");
            authority.requireInlinePayloadIndependent(manifest,audited.ledger(),audited.snapshot(),body,body.floor(),facts);requireCurrent();return body;
        }
        /** Current actual Floor only. Includes the exact pre-delete fold or this exact historical APPLIED release. */
        InlinePayloadCopy authenticateInlinePayloadFloor(com.nereusstream.delay.protocol.SystemMutation input,RecoveryFloorRef actualFloor,long maximumBytes){
            final var body=com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(input.canonicalBody());
            if(!body.inlinePayloadRelease()||!Arrays.equals(manifest.checkpointId(),actualFloor.checkpointId())
                    ||!Arrays.equals(manifest.manifestSha256(),actualFloor.manifestSha256()))throw new IllegalArgumentException("inline witness is not the actual Floor");
            requireCurrent();final var audited=authenticateManifestCompanion(image,manifest,physical,quota,ledger,semantic,authority,protection);
            if(!body.selfContainedInlinePayloadRelease())throw new IllegalStateException("body10 lacks durable original SourceFacts and cannot obtain positive release protection");
            final var facts=com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.SourceFacts.from(body.sourceFacts());
            if(facts.retainedBytes()>maximumBytes)throw new IllegalStateException("inline Source inputs exceed original held window");
            requireCurrent();final var policies=com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.policies(audited.ledger().dependencies(),audited.snapshot(),body,this::requireCurrent);
            final var released=com.nereusstream.delay.runtime.TargetInlinePayloadReclaim.requireClosed(audited.ledger().dependencies(),body,policies,facts,true,this::requireCurrent);
            if(released!=null){
                final var history=audited.ledger().dependencies().sourceHistories().stream().filter(h->h.decidedAt().equals(released)).findFirst().orElseThrow();
                if(!Arrays.equals(history.input().canonicalEnvelope(),input.canonicalEnvelope()))throw new IllegalStateException("inline Floor contains another actual release");
            }
            authority.requireInlinePayloadIndependent(manifest,audited.ledger(),audited.snapshot(),body,actualFloor,facts);requireCurrent();
            final var result=new InlinePayloadCopy(this,input,policies,facts,released);
            if(result.retainedBytes()>maximumBytes)throw new IllegalStateException("complete inline Floor proof exceeds held input window");return result;
        }
        /** Every actual Pin image is fully authenticated and independently qualified; no unused-prefix relaxation. */
        InlinePayloadCopy authenticateInlinePayloadPin(com.nereusstream.delay.protocol.SystemMutation input,RecoveryFloorRef actualFloor,InlinePayloadCopy floor){
            floor.requireFor(input);requireCurrent();final var body=com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(input.canonicalBody());
            final var audited=authenticateManifestCompanion(image,manifest,physical,quota,ledger,semantic,authority,protection);
            if(!manifest.shardId().equals(body.shard())||!Arrays.equals(manifest.recoveryLineageId(),body.lineage())
                    ||!Arrays.equals(audited.ledger().root().root().tenantScope(),body.scope().tenantScope()))throw new IllegalArgumentException("inline Pin image changed Source scope");
            authority.requireInlinePayloadIndependent(manifest,audited.ledger(),audited.snapshot(),body,actualFloor,floor.facts());
            floor.requireCurrent();requireCurrent();return new InlinePayloadCopy(this,input,floor.policies(),floor.facts(),null);
        }

        /** Authenticates the actual immutable image and requires its complete exact Native51 copy. */
        public RetiringCopy authenticateRetiringCopy(com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent expected){
            final var actual=Objects.requireNonNull(expected).record();final var resource=actual.copy().resource();
            if(manifest.storeFormatVersion()!=2||!manifest.shardId().equals(resource.shard())
                    ||!Arrays.equals(manifest.recoveryLineageId(),resource.recoveryLineage()))
                throw new IllegalArgumentException("retirement recovery image changed original SHARD/lineage/format");
            requireCurrent();final var audited=auditManifestImageAuthentication(image,manifest,physical,quota,ledger,semantic,authority,protection);
            actual.appliedAt().requireAtOrBefore(audited.root().mutationSequence(),audited.root().source());
            if(!Arrays.equals(audited.root().root().tenantScope(),resource.tenantScope()))
                throw new IllegalArgumentException("retirement recovery image changed original tenant");
            final byte[] key=actual.key();com.nereusstream.delay.protocol.TargetEvidenceRetiringRecord retained=null;
            for(var candidate:audited.dependencies().retirements())if(Arrays.equals(candidate.key(),key)){
                if(retained!=null)throw new IllegalStateException("duplicate image retirement projection");retained=candidate;
            }
            if(retained==null||!Arrays.equals(retained.canonicalBytes(),actual.canonicalBytes()))
                throw new IllegalStateException("authenticated recovery image lacks the complete exact APPLIED retirement copy");
            requireCurrent();return new RetiringCopy(this,Bytes.sha256(actual.canonicalBytes()));
        }
        /** Historical replay may observe a newer Floor already containing this exact confirmed Source first. */
        EvidenceCopy authenticateHistoricalCopy(com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent expected,
                com.nereusstream.delay.protocol.SystemMutation confirmation,SourcePosition confirmationSource){
            final var actual=Objects.requireNonNull(expected).record();final var resource=actual.copy().resource();
            final var body=com.nereusstream.delay.protocol.TargetEvidenceDeleteConfirmedBody.decode(confirmation.canonicalBody());
            if(!body.markerRetained()||manifest.storeFormatVersion()!=2||!manifest.shardId().equals(resource.shard())
                    ||!Arrays.equals(manifest.recoveryLineageId(),resource.recoveryLineage()))throw new IllegalArgumentException("historical image scope changed");
            requireCurrent();final var audited=auditManifestImageAuthentication(image,manifest,physical,quota,ledger,semantic,authority,protection);
            actual.appliedAt().requireAtOrBefore(audited.root().mutationSequence(),audited.root().source());
            if(!Arrays.equals(audited.root().root().tenantScope(),resource.tenantScope()))throw new IllegalArgumentException("historical image tenant changed");
            boolean retained=false;
            for(var row:audited.dependencies().retirements())if(Arrays.equals(row.key(),actual.key())){
                if(!Arrays.equals(row.canonicalBytes(),actual.canonicalBytes()))throw new IllegalStateException("historical image changes exact Native51 copy");retained=true;
            }
            for(var row:audited.dependencies().evidenceConfirmations())if(Arrays.equals(row.body().resource().exactIdentity().identityHash(),resource.exactIdentity().identityHash())){
                if(!Arrays.equals(row.confirmation().canonicalEnvelope(),confirmation.canonicalEnvelope())||!row.intentAppliedAt().equals(actual.appliedAt())
                        ||!Arrays.equals(row.appliedAt().source().canonicalBytes(),confirmationSource.canonicalBytes()))
                    throw new IllegalStateException("historical image confirms another Source frame/intent");
                row.appliedAt().requireAtOrBefore(audited.root().mutationSequence(),audited.root().source());retained=true;
            }
            if(!retained)throw new IllegalStateException("historical recovery image lacks exact Native51 or the exact Native52 first");
            requireCurrent();return new HistoricalCopy(this,Bytes.sha256(actual.canonicalBytes()));
        }

        /** Full physical/ledger/original-input audit of the exact Native unused-reserve group. */
        public UnusedReserveCopy authenticateUnusedReserveCopy(com.nereusstream.delay.protocol.SystemMutation input){
            return authenticateUnusedReserveCopy(input,null);
        }
        /** Complete historical prefix only; it grants no ability to activate or consume this old reserve. */
        UnusedReserveCopy authenticateUnusedReservePredecessorCopy(com.nereusstream.delay.protocol.SystemMutation input,UnusedReserveCopy currentFloor){
            Objects.requireNonNull(currentFloor).requireFor(input);
            return authenticateUnusedReserveCopy(input,currentFloor);
        }
        private UnusedReserveCopy authenticateUnusedReserveCopy(com.nereusstream.delay.protocol.SystemMutation input,UnusedReserveCopy currentFloor){
            final var body=com.nereusstream.delay.protocol.TargetResourceReclaimBody.decode(input.canonicalBody());
            if(body.inlinePayloadRelease())throw new IllegalArgumentException("unused reserve image qualification cannot authorize payload deletion");
            requireCurrent();
            final var audited=authenticateManifestCompanion(image,manifest,physical,quota,ledger,semantic,authority,protection);
            final var dependencies=audited.ledger().dependencies();
            if(!manifest.shardId().equals(body.shard())||!Arrays.equals(manifest.recoveryLineageId(),body.lineage())
                    ||!Arrays.equals(dependencies.root().root().tenantScope(),body.scope().tenantScope()))
                throw new IllegalArgumentException("unused-reserve image changed original Source scope/lineage");
            final var histories=new java.util.ArrayList<UnusedReserveWriter>();
            for(var operation:body.operations()){
                final var expected=((com.nereusstream.delay.protocol.TargetResourceReclaimBody.UnusedAttemptReserve)operation).expected();
                final var actual=dependencies.admissions().stream().filter(a->Arrays.equals(a.attempt().publishAttemptId(),expected.publishAttemptId()))
                        .findFirst().orElse(null);
                if(currentFloor!=null&&compareExactBoundary(manifest.appliedShardLogPosition(),currentFloor.image.manifest.appliedShardLogPosition())<0
                        &&Long.compareUnsigned(manifest.shardMutationSequence(),currentFloor.image.manifest.shardMutationSequence())<0){
                    final var writer=currentFloor.writers.stream().filter(w->Arrays.equals(w.expected().canonicalBytes(),expected.canonicalBytes())).findFirst().orElseThrow(
                            ()->new IllegalStateException("current Floor omits exact original reserve witness"));
                    if(compareExactBoundary(manifest.appliedShardLogPosition(),writer.outcomeSource())<0){
                        requireUnusedReservePredecessor(actual,writer,manifest,dependencies);
                        histories.add(writer);continue;
                    }
                }
                if(actual==null)throw new IllegalStateException("authenticated image lacks actual retained Budget");
                dependencies.requireSystemPosition(actual.first());
                if(!Arrays.equals(actual.attempt().canonicalBytes(),expected.canonicalBytes())){
                    // A newer actual Floor can contain the same release first during cold Source replay.
                    final var decision=dependencies.sourceHistories().stream().filter(h->Arrays.equals(h.input().canonicalEnvelope(),input.canonicalEnvelope()))
                            .findFirst().orElseThrow(()->new IllegalStateException("image Budget differs from expected without exact reclaim Source history"));
                    if(actual.attempt().phase()!=com.nereusstream.delay.protocol.TargetQuotaAttemptBudget.Phase.RETAINED
                            ||!actual.attempt().mutation().equals(decision.decidedAt()))throw new IllegalStateException("image Budget is not the exact reclaim successor");
                }
                final var admitted=audited.snapshot().admissions().stream().filter(a->Arrays.equals(a.image().mutationHash(),expected.admissionDigest())
                        &&Arrays.equals(com.nereusstream.delay.protocol.TargetPublishAdmissionBody.decode(a.image().canonicalBody()).publishAttemptId(),expected.publishAttemptId()))
                        .findFirst().orElseThrow(()->new IllegalStateException("original authenticated Admission frame is unavailable"));
                final var resolved=audited.snapshot().outcomes().stream().filter(o->Arrays.equals(o.source().canonicalBytes(),expected.resolvedAt().source().canonicalBytes())
                        &&Arrays.equals(Bytes.sha256(o.image().canonicalEnvelope()),expected.resolvedAt().mutationDigest()))
                        .findFirst().orElseThrow(()->new IllegalStateException("original authenticated definitive Outcome frame is unavailable"));
                final var outcomeFirst=dependencies.outcomeResults().stream().filter(first->first.mutation().equals(expected.resolvedAt())
                        &&Arrays.equals(com.nereusstream.delay.runtime.SystemMutationResult.decode(first.typedPayload()).mutationId(),resolved.image().systemMutationId()))
                        .findFirst().orElseThrow(()->new IllegalStateException("authenticated image lacks original definitive Outcome first"));
                dependencies.requireSystemPosition(outcomeFirst);
                histories.add(new UnusedReserveWriter(expected,admitted.image(),admitted.source(),resolved.image(),resolved.source()));
            }
            com.nereusstream.delay.protocol.TargetQuotaMutation retained=null;
            final var decision=dependencies.sourceHistories().stream().filter(h->Arrays.equals(h.input().canonicalEnvelope(),input.canonicalEnvelope())).findFirst().orElse(null);
            if(decision!=null)for(var first:dependencies.retirementResults()){
                final var result=com.nereusstream.delay.runtime.SystemMutationResult.decode(first.typedPayload());
                if(result.applyStatus()==com.nereusstream.delay.runtime.ApplyStatus.APPLIED&&Arrays.equals(result.mutationId(),input.systemMutationId())){
                    dependencies.requireSystemPosition(first);
                    if(!first.mutation().equals(decision.decidedAt())||!result.equals(com.nereusstream.delay.runtime.SystemMutationResult.from(input,
                            com.nereusstream.delay.runtime.ApplyStatus.APPLIED,com.nereusstream.delay.protocol.StableCode.OK,decision.decidedAt().source().canonicalBytes())))
                        throw new IllegalStateException("authenticated reclaim decision changed its exact Source first");
                    retained=decision.decidedAt();
                }
            }
            requireCurrent();if(currentFloor!=null)currentFloor.requireFor(input);
            return new UnusedReserveCopy(this,Bytes.sha256(input.canonicalEnvelope()),List.copyOf(histories),retained,currentFloor);
        }

        private static void requireUnusedReservePredecessor(TargetCheckpointDependencies.Admission actual,UnusedReserveWriter writer,CheckpointManifest manifest,
                TargetCheckpointDependencies dependencies){
            final var expected=writer.expected();
            if(Long.compareUnsigned(manifest.shardMutationSequence(),expected.resolvedAt().sequence())>=0)
                throw new IllegalStateException("older reserve image crosses original resolution sequence");
            if(compareExactBoundary(manifest.appliedShardLogPosition(),writer.admissionSource())<0){
                if(actual!=null)throw new IllegalStateException("older image contains a reserve before its original Admission");
                return;
            }
            if(actual==null)throw new IllegalStateException("older image lost a Source-admitted reserve");
            final var first=actual.first();
            dependencies.requireSystemPosition(first);
            if(!first.mutation().source().equals(writer.admissionSource())
                    ||!Arrays.equals(first.mutation().mutationDigest(),Bytes.sha256(writer.admission().canonicalEnvelope()))
                    ||!com.nereusstream.delay.runtime.SystemMutationResult.decode(first.typedPayload()).equals(
                            com.nereusstream.delay.runtime.SystemMutationResult.from(writer.admission(),com.nereusstream.delay.runtime.ApplyStatus.APPLIED,
                                    com.nereusstream.delay.protocol.StableCode.OK,writer.admissionSource().canonicalBytes())))
                throw new IllegalStateException("older image changed original Admission first");
            final var original=com.nereusstream.delay.protocol.TargetQuotaAttemptBudget.admit(expected.locator(),expected.tenantScope(),expected.publishAttemptId(),
                    expected.admissionDigest(),expected.accounting(),expected.executionBytes(),expected.commitment(),expected.allocated(),first.mutation(),expected.recoveryLineage());
            if(!Arrays.equals(original.canonicalBytes(),actual.attempt().canonicalBytes()))
                throw new IllegalStateException("older image is not the exact original ADMITTED reserve");
        }
        private static int compareExactBoundary(SourcePosition actual,SourcePosition boundary){
            final int order=actual.compareTo(boundary);
            if(order==0&&!Arrays.equals(actual.canonicalBytes(),boundary.canonicalBytes()))throw new IllegalStateException("recovery prefix changed metadata at the same Source position");
            return order;
        }

        /** Short recheck of held permissions/pins at the CAS boundary; provider acquisition happens before it. */
        public void requireCurrent() { protection.run(); }

        /** Preserves the exact authenticated image's finite limits/authority for its private restore copy. */
        public ShardStore.TargetRestoreAuthentication restoreAuthentication(BoundedReadBudget installationBudget) {
            requireCurrent();
            return new ShardStore.TargetRestoreAuthentication(quota, ledger, semantic, authority, protection,
                    Objects.requireNonNull(installationBudget, "installationBudget"));
        }
    }

    /** Actual immutable image proof only; Catalog/Floor/Pin/query/physical permission remain separate obligations. */
    sealed interface EvidenceCopy permits RetiringCopy,HistoricalCopy {
        CheckpointManifest manifest();void requireFor(com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent expected);void requireCurrent();
    }
    public static final class RetiringCopy implements EvidenceCopy {
        private final AuthenticatedImage image;
        private final byte[] recordDigest;
        private RetiringCopy(AuthenticatedImage image,byte[] recordDigest){this.image=image;this.recordDigest=recordDigest;}
        public CheckpointManifest manifest(){return image.manifest();}
        public void requireFor(com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent expected){
            requireCurrent();if(!Arrays.equals(recordDigest,Bytes.sha256(expected.record().canonicalBytes())))
                throw new IllegalArgumentException("recovery copy belongs to another actual Native retirement");
        }
        /** Rechecks original held immutable-image permissions/pins; no file or Registry I/O here. */
        public void requireCurrent(){image.requireCurrent();}
    }
    private static final class HistoricalCopy implements EvidenceCopy {
        private final AuthenticatedImage image;private final byte[] intentDigest;
        private HistoricalCopy(AuthenticatedImage image,byte[] digest){this.image=image;intentDigest=digest;}
        public CheckpointManifest manifest(){return image.manifest();}
        public void requireCurrent(){image.requireCurrent();}
        public void requireFor(com.nereusstream.delay.runtime.TargetEvidenceRetirementStore.AppliedIntent expected){requireCurrent();
            if(!Arrays.equals(intentDigest,Bytes.sha256(expected.record().canonicalBytes())))throw new IllegalArgumentException("historical image belongs to another actual intent");}
    }

    public static AuthenticatedImage authenticateImage(Path image, CheckpointManifest manifest,
            CheckpointManifestLimits physical, QuotaAuditLimits quota, LedgerAuditLimits ledger,
            TargetCheckpointSemanticSnapshot.Limits semantic, ImageAuthority authority, Runnable protection) {
        final var result = new AuthenticatedImage(Objects.requireNonNull(image, "image"), manifest, physical, quota,
                ledger, semantic, Objects.requireNonNull(authority, "authority"),
                Objects.requireNonNull(protection, "protection"));
        result.authenticate();
        return result;
    }

    /** Raw candidate request only; construction performs no image/provider I/O or authentication. */
    public record CandidateImage(Path image, CheckpointUploadIntent pending, long lineageGeneration,
            List<CheckpointManifest.FileEntry> fileObjects, CheckpointManifestLimits physical,
            QuotaAuditLimits quota, LedgerAuditLimits ledger, TargetCheckpointSemanticSnapshot.Limits semantic,
            ImageAuthority authority, Runnable protection) {
        public CandidateImage {
            image = Objects.requireNonNull(image, "image").toAbsolutePath().normalize();
            Objects.requireNonNull(pending, "pending");
            if (pending.state() != CheckpointUploadState.PENDING_UPLOAD
                    || (pending.parentCheckpointId() == null) != (lineageGeneration == 0)) {
                throw new IllegalArgumentException("Target candidate requires pending intent and exact lineage shape");
            }
            fileObjects = List.copyOf(Objects.requireNonNull(fileObjects, "fileObjects"));
            requireFinitePhysicalLimits(physical);
            if (fileObjects.isEmpty() || fileObjects.size() > physical.maxFiles()) {
                throw new IllegalArgumentException("Target candidate file objects exceed their finite bound");
            }
            long bytes = 0;
            for (var file : fileObjects) {
                physical.validateFile(file.name(), file.length());
                if (file.objectKey().length > physical.maxObjectIdentityBytes()
                        || file.objectVersion().length > physical.maxObjectIdentityBytes()
                        || file.etag() != null && file.etag().length > physical.maxObjectIdentityBytes()) {
                    throw new IllegalArgumentException("Target candidate object identity exceeds its finite bound");
                }
                bytes = Math.addExact(bytes, file.length());
            }
            if (bytes > physical.maxTotalFileBytes()) {
                throw new IllegalArgumentException("Target candidate declared bytes exceed their finite bound");
            }
            Objects.requireNonNull(quota, "quota");
            Objects.requireNonNull(ledger, "ledger");
            Objects.requireNonNull(semantic, "semantic");
            Objects.requireNonNull(authority, "authority");
            Objects.requireNonNull(protection, "protection");
        }
    }

    /**
     * Builds a complete Manifest from this immutable candidate's actual native metadata and companion, then
     * performs mandatory full external authentication. Invoke in the existing CHECKPOINT WorkClass; caller
     * supplies exact provider object identities and holds all original image/Owner/Source/pin protections.
     */
    public static AuthenticatedImage authenticateCandidate(CandidateImage request) {
        final var candidate = Objects.requireNonNull(request, "request");
        candidate.protection().run();
        final var inspected = inspectImage(candidate.image(), candidate.pending().shard().shardId(),
                candidate.physical(), null, candidate.quota(), candidate.ledger(), null, true);
        candidate.protection().run();
        final var proof = inspected.ledger().root();
        final var pending = candidate.pending();
        final var fields = inspected.manifestFields();
        if (!Arrays.equals(fields.checkpointId(), pending.checkpointId())
                || fields.ownerEpoch() != pending.owner().ownerEpoch()
                || !Arrays.equals(proof.metadata().storeIncarnation(), pending.sourceStoreIncarnation())
                || !Arrays.equals(proof.root().recoveryLineage(), pending.recoveryLineageId())) {
            throw new IllegalArgumentException("Target candidate metadata differs from its exact upload intent");
        }
        final var snapshot = TargetCheckpointSemanticSnapshotFile.read(candidate.image(), candidate.semantic());
        snapshot.validateAgainst(inspected.ledger().dependencies(), candidate.semantic());
        snapshot.requireExternalEvidenceComplete(inspected.ledger().dependencies());
        snapshot.requireRecoveryEvidenceComplete(inspected.ledger().dependencies());
        snapshot.requireLifecycleComplete(inspected.ledger().dependencies(),candidate.semantic());
        candidate.protection().run();
        final var time = pending.checkpointCreatedAt();
        final var parent = pending.parentCheckpointId() == null ? null : new CheckpointManifest.ParentCheckpoint(
                pending.parentCheckpointId(), Bytes.hex(pending.parentManifestSha256()));
        final var signature = time.sourceSignature();
        final var manifest = new CheckpointManifest(fields.checkpointId(), pending.recoveryLineageId(),
                candidate.lineageGeneration(), parent, fields.restoredFromCheckpointId(),
                new CheckpointManifest.CreatedBy(pending.owner().deploymentId(), pending.owner().workerRunId(),
                        fields.ownerEpoch()),
                new CheckpointManifest.CreatedAt(time.earliestEpochMs(), time.latestEpochMs(), time.source().name(),
                        time.sourceId(), time.sourceConfigGeneration(), time.sampleSequence(), time.monotonicAnchorNs(),
                        time.sourceEvidenceSha256(), time.sourceKeyVersion(), signature.length == 0 ? null : signature),
                proof.metadata().shardId(), proof.metadata().dbIdentity(), proof.metadata().storeIncarnationUuid(),
                proof.metadata().storeFormatVersion(), proof.mutationSequence(), proof.source(),
                snapshot.snapshotDigest(), snapshot.semanticInputsDigest(), fields.evidenceCursors(),
                candidate.fileObjects(), proof.aggregate().mutation().localOrdinal());
        return authenticateImage(candidate.image(), manifest, candidate.physical(), candidate.quota(),
                candidate.ledger(), candidate.semantic(), candidate.authority(), candidate.protection());
    }

    private record ManifestFields(byte[] checkpointId, long ownerEpoch,
            List<com.nereusstream.delay.protocol.EvidenceCursor> evidenceCursors, byte[] restoredFromCheckpointId) {}

    private record ImageInspection(LedgerProof ledger, ManifestFields manifestFields) {}

    /** One complete immutable image audit followed by mandatory external authentication outside Store monitors. */
    public static LedgerProof auditManifestImageAuthentication(final Path image, final CheckpointManifest manifest,
            final CheckpointManifestLimits physicalLimits, final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits, final TargetCheckpointSemanticSnapshot.Limits semanticLimits,
            final ImageAuthority authority, final Runnable protection) {
        return authenticateManifestCompanion(image,manifest,physicalLimits,quotaLimits,ledgerLimits,semanticLimits,authority,protection).ledger();
    }

    private static ManifestCompanion authenticateManifestCompanion(Path image,CheckpointManifest manifest,
            CheckpointManifestLimits physicalLimits,QuotaAuditLimits quotaLimits,LedgerAuditLimits ledgerLimits,
            TargetCheckpointSemanticSnapshot.Limits semanticLimits,ImageAuthority authority,Runnable protection){
        Objects.requireNonNull(authority, "authority");
        Objects.requireNonNull(protection, "protection").run();
        final var verified = auditManifestCompanion(image, manifest, physicalLimits, quotaLimits, ledgerLimits,
                semanticLimits);
        protection.run();
        authority.requireAuthenticated(manifest, verified.ledger(), verified.snapshot());
        protection.run();
        authority.requireLifecycleAuthenticated(manifest,verified.ledger(),verified.snapshot());
        protection.run();
        authority.requireTerminalPoliciesAuthenticated(manifest,verified.ledger(),verified.snapshot());
        protection.run();
        return verified;
    }

    private static ManifestCompanion auditManifestCompanion(final Path image, final CheckpointManifest manifest,
            final CheckpointManifestLimits physicalLimits, final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits, final TargetCheckpointSemanticSnapshot.Limits semanticLimits) {
        requireFormat2Manifest(manifest, physicalLimits);
        Objects.requireNonNull(semanticLimits, "semanticLimits");
        final var proof = validateImage(image, manifest.shardId(), physicalLimits, manifest,
                Objects.requireNonNull(quotaLimits, "quotaLimits"),
                Objects.requireNonNull(ledgerLimits, "ledgerLimits"), null);
        if (proof.dependencies().controls().stream().noneMatch(control -> control.family() == ColumnFamily.META
                && Arrays.equals(control.key(), KeyCodec.metaFixed(10)))) {
            throw new IllegalArgumentException("Target Manifest requires its initial reader/control snapshot");
        }
        final var snapshot = TargetCheckpointSemanticSnapshotFile.read(image, semanticLimits);
        if (snapshot.schemaVersion() < 2) {
            throw new IllegalArgumentException("Target Manifest requires complete original-Control schema 2");
        }
        if (!proof.dependencies().outcomeResults().isEmpty() && snapshot.schemaVersion() < 3) {
            throw new IllegalArgumentException("Target Manifest with retained Outcomes requires schema 3");
        }
        snapshot.validateAgainst(proof.dependencies(), semanticLimits);
        snapshot.requireExternalEvidenceComplete(proof.dependencies());
        snapshot.requireRecoveryEvidenceComplete(proof.dependencies());
        snapshot.requireLifecycleComplete(proof.dependencies(),semanticLimits);
        if (!Bytes.constantTimeEquals(manifest.controlStateDigest(), snapshot.snapshotDigest())) {
            throw new IllegalArgumentException("Target Manifest control-state digest differs from its companion");
        }
        if (!Bytes.constantTimeEquals(manifest.referencedSemanticVersionsDigest(), snapshot.semanticInputsDigest())) {
            throw new IllegalArgumentException("Target Manifest semantic-version digest differs from its companion");
        }
        return new ManifestCompanion(proof, snapshot);
    }

    private static void requireFormat2Manifest(
            final CheckpointManifest manifest, final CheckpointManifestLimits limits) {
        Objects.requireNonNull(manifest, "manifest");
        if (manifest.storeFormatVersion() != 2) {
            throw new IllegalArgumentException("Target image binding requires a format-2 manifest");
        }
        manifest.validateLimits(Objects.requireNonNull(limits, "limits"));
    }

    private static LedgerProof validateImage(
            final Path image,
            final ShardId expectedShard,
            final CheckpointManifestLimits limits,
            final CheckpointManifest manifest,
            final QuotaAuditLimits quotaLimits,
            final LedgerAuditLimits ledgerLimits,
            final CandidateIdentity candidate) {
        return inspectImage(image, expectedShard, limits, manifest, quotaLimits, ledgerLimits, candidate, false)
                .ledger();
    }

    private static ImageInspection inspectImage(Path image, ShardId expectedShard, CheckpointManifestLimits limits,
            CheckpointManifest manifest, QuotaAuditLimits quotaLimits, LedgerAuditLimits ledgerLimits,
            CandidateIdentity candidate, boolean requireManifestFields) {
        Objects.requireNonNull(image, "image");
        Objects.requireNonNull(expectedShard, "expectedShard");
        requireFinitePhysicalLimits(limits);
        final var files = CheckpointFileInventory.collect(image, limits);
        if (manifest != null) {
            if (files.size() != manifest.files().size()) {
                throw new IllegalArgumentException("Target image file count does not match manifest");
            }
            for (int index = 0; index < files.size(); index++) {
                final var actual = files.get(index);
                final var declared = manifest.files().get(index);
                if (!actual.name().equals(declared.name())
                        || actual.length() != declared.length()
                        || !Bytes.constantTimeEquals(actual.checksum(), declared.checksum())) {
                    throw new IllegalArgumentException("Target image file inventory does not match manifest");
                }
            }
        }
        if (!Files.isRegularFile(image.resolve("CURRENT"), LinkOption.NOFOLLOW_LINKS)
                || files.stream()
                        .noneMatch(file -> file.name().startsWith("MANIFEST-")
                                && !file.name().contains("/"))) {
            throw new IllegalArgumentException("Target checkpoint is not a complete RocksDB image");
        }
        final List<byte[]> names;
        try (Options options = new Options()) {
            names = RocksDB.listColumnFamilies(options, image.toString());
        } catch (RocksDBException failure) {
            throw new IllegalArgumentException("cannot inspect Target checkpoint column families", failure);
        }
        final List<String> familyNames = names.stream()
                .map(name -> new String(name, StandardCharsets.UTF_8))
                .toList();
        final Set<String> expectedFamilies = new HashSet<>();
        expectedFamilies.add(new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8));
        for (ColumnFamily family : ColumnFamily.values()) {
            expectedFamilies.add(family.rocksName());
        }
        final Set<String> actualFamilies = new HashSet<>();
        actualFamilies.addAll(familyNames);
        if (actualFamilies.size() != names.size() || !actualFamilies.equals(expectedFamilies)) {
            throw new IllegalArgumentException("Target checkpoint column-family set differs from the Store");
        }
        final List<ColumnFamilyOptions> options = new ArrayList<>();
        final List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        final List<ColumnFamilyHandle> handles = new ArrayList<>();
        try {
            for (byte[] name : names) {
                final var familyOptions = new ColumnFamilyOptions();
                options.add(familyOptions);
                descriptors.add(new ColumnFamilyDescriptor(name, familyOptions));
            }
            try (DBOptions dbOptions = new DBOptions().setCreateIfMissing(false).setCreateMissingColumnFamilies(false);
                    RocksDB db = RocksDB.openReadOnly(dbOptions, image.toString(), descriptors, handles)) {
                final int metaIndex = familyNames.indexOf(ColumnFamily.META.rocksName());
                final ColumnFamilyHandle meta = handles.get(metaIndex);
                final RootProof proof = readRoot(db, meta, expectedShard);
                if (manifest != null) {
                    requireManifestImageIdentity(db, meta, proof, manifest);
                }
                if (candidate != null) {
                    requireLocalCandidateImageIdentity(db, meta, proof, candidate);
                }
                if (quotaLimits != null) {
                    auditQuotaProjections(db, meta, proof, quotaLimits);
                }
                TargetCheckpointDependencies dependencies = null;
                if (ledgerLimits != null) {
                    final Map<ColumnFamily, ColumnFamilyHandle> familyHandles = new EnumMap<>(ColumnFamily.class);
                    for (ColumnFamily family : ColumnFamily.values()) {
                        familyHandles.put(family, handles.get(familyNames.indexOf(family.rocksName())));
                    }
                    final ColumnFamilyHandle defaultHandle = handles.get(
                            familyNames.indexOf(new String(RocksDB.DEFAULT_COLUMN_FAMILY, StandardCharsets.UTF_8)));
                    dependencies = TargetCheckpointLedgerAudit.audit(
                            db, familyHandles, defaultHandle, proof, ledgerLimits);
                }
                ManifestFields fields = null;
                if (requireManifestFields) {
                    final var checkpoint = requiredFixed(db, meta, 7, "checkpoint identity");
                    final var epoch = requiredFixed(db, meta, 8, "opened Owner epoch");
                    Bytes.requireLength(epoch, Long.BYTES, "opened Owner epoch");
                    final var cursors = StoreRuntimeMetadata.decodeEvidenceCursors(
                            requiredFixed(db, meta, 6, "evidence cursors"));
                    final var base = db.get(meta, KeyCodec.metaRecovery(1));
                    final var restoredFrom = base == null ? null : RecoveryCandidateRef.decode(
                            TargetValueEnvelope.decode(base, FIXED_VALUE_TYPE).payload()).checkpointId();
                    fields = new ManifestFields(checkpoint, Bytes.readU64be(epoch, 0), cursors, restoredFrom);
                }
                return new ImageInspection(new LedgerProof(proof, dependencies), fields);
            } catch (RocksDBException failure) {
                throw new IllegalArgumentException("cannot open Target checkpoint read-only", failure);
            }
        } finally {
            for (ColumnFamilyHandle handle : handles) {
                handle.close();
            }
            for (ColumnFamilyOptions option : options) {
                option.close();
            }
        }
    }

    static void requireFinitePhysicalLimits(final CheckpointManifestLimits limits) {
        Objects.requireNonNull(limits, "limits");
        if (limits.maxFiles() == Integer.MAX_VALUE
                || limits.maxTotalFileBytes() == Long.MAX_VALUE
                || limits.maxIndividualFileBytes() == Long.MAX_VALUE
                || limits.maxPathBytes() == Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Target checkpoint root validation requires finite physical limits");
        }
    }

    private static void auditQuotaProjections(
            final RocksDB db, final ColumnFamilyHandle meta, final RootProof proof, final QuotaAuditLimits limits)
            throws RocksDBException {
        final var budget = new QuotaAuditBudget(limits);
        final ShardId shard = proof.metadata().shardId();
        final byte[] tenant = proof.bookkeeping().tenantScope();
        final List<TargetQuotaCounter> counters = scanQuota(
                db,
                meta,
                TargetKeyCodec.QUOTA_COUNTER_TAG,
                TargetQuotaCounter.VALUE_TYPE,
                budget,
                (key, payload) -> TargetQuotaCounter.decodeForStore(key, payload, shard));
        final List<TargetQuotaTotal> totals = scanQuota(
                db,
                meta,
                TargetKeyCodec.QUOTA_TOTAL_TAG,
                TargetQuotaTotal.VALUE_TYPE,
                budget,
                (key, payload) -> TargetQuotaTotal.decodeForStore(key, payload, shard, tenant));
        final List<TargetQuotaGrantActivation> grants = scanQuota(
                db,
                meta,
                TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG,
                TargetQuotaGrantActivation.VALUE_TYPE,
                budget,
                (key, payload) -> TargetQuotaGrantActivation.decodeForStore(key, payload, shard, tenant));
        auditQuotaRecords(proof, counters, totals, grants);
    }

    static void auditQuotaRecords(
            final RootProof proof,
            final List<TargetQuotaCounter> counters,
            final List<TargetQuotaTotal> totals,
            final List<TargetQuotaGrantActivation> grants) {
        final ShardId shard = proof.metadata().shardId();
        final byte[] tenant = proof.bookkeeping().tenantScope();
        proof.bookkeeping().auditInventory(proof.aggregate(), counters, totals, grants);
        final Map<TargetQuotaIdentity, TargetQuotaCounter> byIdentity = new HashMap<>();
        final Map<TargetPartitionId, TargetQuotaUsage> targetUsage = new HashMap<>();
        TargetQuotaUsage shardUsage = TargetQuotaUsage.empty();
        for (TargetQuotaCounter counter : counters) {
            proof.aggregate().requireCounter(counter);
            if (byIdentity.put(counter.identity(), counter) != null) {
                throw new IllegalStateException("duplicate Target quota counter identity");
            }
            if (!counter.identity().kind().isMirror()) {
                shardUsage = shardUsage.add(counter.usage());
                if (counter.identity().kind().hasTarget()) {
                    targetUsage.merge(counter.identity().target(), counter.usage(), TargetQuotaUsage::add);
                }
            }
        }
        if (!shardUsage.equals(proof.aggregate().usage())) {
            throw new IllegalStateException("Target quota aggregate differs from primary counters");
        }
        for (TargetQuotaCounter counter : counters) {
            if (counter.identity().kind().isMirror()) {
                if (!byIdentity.containsKey(counter.identity().primary())) {
                    throw new IllegalStateException("Target quota tenant mirror has no primary counter");
                }
                continue;
            }
            final TargetQuotaIdentity primary = counter.identity();
            final TargetQuotaIdentity mirror = new TargetQuotaIdentity(
                    primary.kind().hasTarget()
                            ? TargetQuotaIdentity.Kind.TENANT_TARGET
                            : TargetQuotaIdentity.Kind.TENANT_SHARD,
                    shard,
                    primary.accountingIncarnation(),
                    primary.target(),
                    tenant);
            final TargetQuotaCounter actualMirror = byIdentity.get(mirror);
            if (actualMirror == null
                    || !counter.usage().resources().equals(actualMirror.usage().resources())) {
                throw new IllegalStateException("Target quota primary and tenant mirror disagree");
            }
        }
        final Map<TargetPartitionId, TargetQuotaTotal> byTarget = new HashMap<>();
        for (TargetQuotaTotal total : totals) {
            total.requireAggregate(proof.aggregate());
            if (byTarget.put(total.scope().target(), total) != null
                    || !total.usage()
                            .equals(targetUsage.getOrDefault(total.scope().target(), TargetQuotaUsage.empty()))) {
                throw new IllegalStateException("Target quota total differs from primary counters");
            }
        }
        for (TargetQuotaCounter counter : counters) {
            if (counter.identity().kind() == TargetQuotaIdentity.Kind.TARGET) {
                final TargetQuotaTotal total = byTarget.get(counter.identity().target());
                if (total == null) {
                    throw new IllegalStateException("Target quota primary counter has no total");
                }
                total.requireCounter(counter);
            }
        }
        if (!byTarget.keySet().containsAll(targetUsage.keySet())) {
            throw new IllegalStateException("Target quota primary counter has no total");
        }
        for (TargetQuotaGrantActivation grant : grants) {
            grant.mutation().requireAtOrBefore(proof.aggregate().mutation());
        }
    }

    private static <T> List<T> scanQuota(
            final RocksDB db,
            final ColumnFamilyHandle meta,
            final int tag,
            final int valueType,
            final QuotaAuditBudget budget,
            final BiFunction<byte[], byte[], T> decoder)
            throws RocksDBException {
        final List<T> records = new ArrayList<>();
        try (RocksIterator iterator = db.newIterator(meta)) {
            iterator.seek(new byte[] {(byte) tag});
            while (iterator.isValid()) {
                final byte[] key = iterator.key();
                if (key.length == 0 || Byte.toUnsignedInt(key[0]) != tag) {
                    break;
                }
                final byte[] value = iterator.value();
                budget.charge(key.length, value.length);
                if (key.length < 2 || Byte.toUnsignedInt(key[1]) != TargetKeyCodec.KEY_FORMAT) {
                    throw new IllegalArgumentException("Target quota projection has another key format");
                }
                records.add(decoder.apply(
                        key, TargetValueEnvelope.decode(value, valueType).payload()));
                iterator.next();
            }
            iterator.status();
        }
        return records;
    }

    private static final class QuotaAuditBudget {
        private final QuotaAuditLimits limits;
        private int records;
        private long bytes;

        private QuotaAuditBudget(final QuotaAuditLimits limits) {
            this.limits = limits;
        }

        private void charge(final int keyBytes, final int valueBytes) {
            final long nextBytes = (long) keyBytes + valueBytes;
            if (records >= limits.maxRecords() || nextBytes > limits.maxKeyValueBytes() - bytes) {
                throw new IllegalArgumentException("Target quota audit budget exceeded");
            }
            records++;
            bytes += nextBytes;
        }
    }

    private static RootProof readRoot(final RocksDB db, final ColumnFamilyHandle meta, final ShardId shard) {
        try {
            final byte[] format = requiredFixed(db, meta, 1, "store format");
            if (format.length != Integer.BYTES || Bytes.readU32be(format, 0) != 2) {
                throw new IllegalArgumentException("Target checkpoint has another Store format");
            }
            final StoreMetadata metadata = StoreMetadata.decode(requiredFixed(db, meta, 2, "shard identity"));
            if (metadata.storeFormatVersion() != 2 || !metadata.shardId().equals(shard)) {
                throw new IllegalArgumentException("Target checkpoint identity belongs to another Shard or format");
            }
            final SourcePosition source = SourcePositionCodec.decode(requiredFixed(db, meta, 3, "source position"));
            final byte[] sequenceBytes = requiredFixed(db, meta, 5, "mutation sequence");
            if (sequenceBytes.length != Long.BYTES) {
                throw new IllegalArgumentException("Target checkpoint mutation sequence has another width");
            }
            final long sequence = Bytes.readU64be(sequenceBytes, 0);
            if (sequence == 0 || !source.shardId().equals(shard)) {
                throw new IllegalArgumentException("Target checkpoint source frontier belongs to another Shard");
            }
            final byte[] bookkeepingKey = TargetQuotaBookkeeping.keyFor(shard);
            final byte[] bookkeepingBytes =
                    requiredTarget(db, meta, bookkeepingKey, TargetQuotaBookkeeping.VALUE_TYPE, "bookkeeping root");
            final TargetQuotaBookkeeping bookkeeping = TargetQuotaBookkeeping.decode(bookkeepingBytes);
            if (!Arrays.equals(bookkeeping.key(), bookkeepingKey)
                    || bookkeeping.owner().kind() != TargetQuotaIdentity.Kind.SHARD
                    || !bookkeeping.owner().shard().equals(shard)) {
                throw new IllegalArgumentException("Target checkpoint bookkeeping belongs to another Shard");
            }
            final byte[] rootKey = bookkeeping.owner().key();
            rootKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
            final TargetQuotaIncarnation root = TargetQuotaIncarnation.decodeForStore(
                    rootKey,
                    requiredTarget(db, meta, rootKey, TargetQuotaIncarnation.VALUE_TYPE, "root incarnation"),
                    shard,
                    bookkeeping.tenantScope());
            if (!bookkeeping.owner().equals(root.identity())
                    || !Arrays.equals(
                            bookkeeping.accounting().canonicalBytes(),
                            root.accounting().canonicalBytes())) {
                throw new IllegalArgumentException("Target checkpoint root and bookkeeping disagree");
            }
            final byte[] aggregateKey = TargetQuotaAggregate.genesis(
                            shard, root.identity().accountingIncarnation())
                    .key();
            final TargetQuotaAggregate aggregate = TargetQuotaAggregate.decodeForStore(
                    aggregateKey,
                    requiredTarget(db, meta, aggregateKey, TargetQuotaAggregate.VALUE_TYPE, "aggregate"),
                    shard);
            if (!Arrays.equals(
                            aggregate.accountingIncarnation(), root.identity().accountingIncarnation())
                    || aggregate.mutation() == null
                    || aggregate.mutation().sequence() != sequence
                    || Long.compareUnsigned(bookkeeping.revision(), aggregate.revision()) > 0
                    || !Arrays.equals(aggregate.mutation().source().canonicalBytes(), source.canonicalBytes())) {
                throw new IllegalArgumentException("Target checkpoint aggregate and source frontier disagree");
            }
            root.latestMutation().requireAtOrBefore(aggregate.mutation());
            bookkeeping.mutation().requireAtOrBefore(aggregate.mutation());
            return new RootProof(metadata, source, sequence, root, bookkeeping, aggregate);
        } catch (RocksDBException failure) {
            throw new IllegalArgumentException("cannot read Target checkpoint root", failure);
        }
    }

    private static void requireManifestImageIdentity(
            final RocksDB db, final ColumnFamilyHandle meta, final RootProof proof, final CheckpointManifest manifest)
            throws RocksDBException {
        if (!Bytes.constantTimeEquals(proof.metadata().dbIdentity(), manifest.dbIdentity())
                || !proof.metadata().storeIncarnationUuid().equals(manifest.sourceStoreIncarnation())
                || !Bytes.constantTimeEquals(proof.root().recoveryLineage(), manifest.recoveryLineageId())
                || proof.mutationSequence() != manifest.shardMutationSequence()
                || manifest.targetLocalOrdinal() != null
                        && manifest.targetLocalOrdinal().longValue() != proof.aggregate().mutation().localOrdinal()
                || !Bytes.constantTimeEquals(
                        proof.source().canonicalBytes(),
                        manifest.appliedShardLogPosition().canonicalBytes())) {
            throw new IllegalArgumentException("Target image Store/source identity does not match manifest");
        }
        final byte[] checkpointId = requiredFixed(db, meta, 7, "checkpoint identity");
        if (!Bytes.constantTimeEquals(checkpointId, manifest.checkpointId())) {
            throw new IllegalArgumentException("Target image checkpoint identity does not match manifest");
        }
        final byte[] ownerEpoch = requiredFixed(db, meta, 8, "opened Owner epoch");
        if (ownerEpoch.length != Long.BYTES
                || Bytes.readU64be(ownerEpoch, 0) != manifest.createdBy().ownerEpoch()) {
            throw new IllegalArgumentException("Target image Owner epoch does not match manifest");
        }
        final byte[] cursorBytes = requiredFixed(db, meta, 6, "evidence cursors");
        if (!StoreRuntimeMetadata.decodeEvidenceCursors(cursorBytes).equals(manifest.evidenceCursors())) {
            throw new IllegalArgumentException("Target image evidence cursors do not match manifest");
        }
    }

    private static void requireLocalCandidateImageIdentity(
            final RocksDB db, final ColumnFamilyHandle meta, final RootProof proof, final CandidateIdentity expected)
            throws RocksDBException {
        if (!Bytes.constantTimeEquals(
                        proof.metadata().encode(), expected.metadata().encode())
                || !Bytes.constantTimeEquals(proof.root().recoveryLineage(), expected.lineage())
                || proof.mutationSequence() != expected.sequence()
                || db.getLatestSequenceNumber() != expected.nativeSequence()
                || !Bytes.constantTimeEquals(
                        proof.source().canonicalBytes(), expected.source().canonicalBytes())) {
            throw new IllegalArgumentException("Target candidate Store, lineage or write cut differs from live Store");
        }
        if (!Bytes.constantTimeEquals(
                requiredFixed(db, meta, 7, "checkpoint identity"),
                expected.runtime().lastCheckpointId())) {
            throw new IllegalArgumentException("Target candidate checkpoint identity differs from live Store");
        }
        final byte[] epoch = requiredFixed(db, meta, 8, "opened Owner epoch");
        if (epoch.length != Long.BYTES
                || Bytes.readU64be(epoch, 0) != expected.runtime().lastOpenedOwnerEpoch()) {
            throw new IllegalArgumentException("Target candidate opened Owner epoch differs from live Store");
        }
        if (!StoreRuntimeMetadata.decodeEvidenceCursors(requiredFixed(db, meta, 6, "evidence cursors"))
                .equals(expected.runtime().evidenceCursors())) {
            throw new IllegalArgumentException("Target candidate evidence cursors differ from live Store");
        }
    }

    private record CandidateIdentity(
            StoreMetadata metadata,
            byte[] lineage,
            SourcePosition source,
            long sequence,
            long nativeSequence,
            StoreRuntimeMetadata runtime) {
        private CandidateIdentity {
            Objects.requireNonNull(metadata, "metadata");
            Bytes.requireLength(lineage, 16, "lineage");
            lineage = Bytes.copy(lineage);
            Objects.requireNonNull(source, "source");
            Objects.requireNonNull(runtime, "runtime");
            if (sequence == 0 || nativeSequence == 0) {
                throw new IllegalArgumentException("Target candidate has no source or native write sequence");
            }
        }
    }

    private static byte[] requiredFixed(
            final RocksDB db, final ColumnFamilyHandle meta, final int keyKind, final String name)
            throws RocksDBException {
        final byte[] encoded = db.get(meta, KeyCodec.metaFixed(keyKind));
        if (encoded == null) {
            throw new IllegalArgumentException("Target checkpoint is missing " + name);
        }
        return ValueEnvelope.decode(encoded, FIXED_VALUE_TYPE).payload();
    }

    private static byte[] requiredTarget(
            final RocksDB db, final ColumnFamilyHandle meta, final byte[] key, final int type, final String name)
            throws RocksDBException {
        final byte[] encoded = db.get(meta, key);
        if (encoded == null) {
            throw new IllegalArgumentException("Target checkpoint is missing " + name);
        }
        return TargetValueEnvelope.decode(encoded, type).payload();
    }
}
