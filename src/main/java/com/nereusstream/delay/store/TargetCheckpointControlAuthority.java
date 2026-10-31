package com.nereusstream.delay.store;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationBodyCodec;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetInitialControlSnapshot;
import com.nereusstream.delay.protocol.TargetParentControlCredit;
import com.nereusstream.delay.protocol.TargetParentControlCreditBody;
import com.nereusstream.delay.protocol.TargetQuotaGrantControlBody;
import com.nereusstream.delay.protocol.TargetScopeControlObligation;
import com.nereusstream.delay.protocol.TargetScopeControlObligationBody;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetParentControlCreditVerifier;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.runtime.TargetResultRecord;
import com.nereusstream.delay.runtime.TargetScopeControlObligationVerifier;
import com.nereusstream.delay.runtime.TargetStoreBootstrap;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;

/** Exact retained Control authentication; external original-Source key/role/scope/history protection is mandatory. */
public final class TargetCheckpointControlAuthority implements TargetCheckpointDependencies.ControlImageAuthority {
    public record Authorization(
            ControlAuthorizationContext actor, ControlOperationAuthorization.TargetScopeProof scope) {
        public Authorization {
            Objects.requireNonNull(actor, "actor");
            Objects.requireNonNull(scope, "scope");
        }
    }

    /** Must authenticate original Broker history and retain its credential/retention/pin protection. */
    @FunctionalInterface
    public interface HistoryAuthority {
        void requireOriginal(TargetCheckpointDependencies.ControlImage input, TargetResultRecord first);
    }

    /**
     * Authenticates this immutable Prepared registration's visibility, actor/roles and complete target scope at the
     * exact original Source. Current registry existence or signed author hashes alone do not establish that fact.
     */
    @FunctionalInterface
    public interface SourceAuthority {
        Authorization resolve(PreparedControlOperation registered, TargetCheckpointDependencies.ControlImage input);
    }

    /** Original held history: reads complete fixed chunks and authenticates their old universe and protections.
     * This includes independent management/eligible readers, exact mapping/custody/conservation, added-copy
     * funding, every original C-after Source classification and durable intent. Hashes/row lists are indexes. */
    public interface MigrationMaterials {
        /**
         * Same original held full bundle; the caller/core performs actual immutable reads, not a yes/no permission
         * check.
         */
        default TargetMigrationPayloadMaterials payloadMaterials() {
            throw new IllegalStateException("original migration holder has no retained payload material");
        }

        default void requireComplete(TargetCheckpointDependencies dependencies) {
            payloadMaterials().requireComplete(dependencies);
        }
        /** The original pre-A management policy and complete authority/succession history, independently pinned. */
        default PublicKey authenticateRootIssuerAuthorization(
                com.nereusstream.delay.protocol.TargetMigrationRootIssuerAuthorization statement,
                com.nereusstream.delay.protocol.TargetMigrationPlan plan,
                com.nereusstream.delay.protocol.TargetMigrationActivationAttempt attempt,
                SourcePosition actualA) {
            throw new IllegalStateException(
                    "migration materials lack the original signed A2 Root issuer policy history");
        }
        /** Independently pinned issuer of plan.managementAuthorityDigest; the A Source signer is not that authority. */
        default PublicKey authenticateRootTupleIssuer(
                com.nereusstream.delay.protocol.TargetMigrationRoot root,
                com.nereusstream.delay.protocol.TargetMigrationActivationAttempt attempt,
                com.nereusstream.delay.protocol.TargetProtocolTupleAuthorization authorization,
                SourcePosition tupleSource) {
            throw new IllegalStateException(
                    "migration materials lack the original independently pinned management snapshot issuer");
        }

        default void requireRetirementHistorical(
                TargetCheckpointDependencies.ControlImage input,
                TargetResultRecord first,
                com.nereusstream.delay.protocol.TargetMigrationRoot root,
                com.nereusstream.delay.protocol.TargetMigrationRetirementRecord retirement) {
            throw new IllegalStateException(
                    "original migration holder lacks actual retirement/Catalog/resource history");
        }

        default void requireArchiveHistorical(
                TargetCheckpointDependencies.ControlImage input,
                TargetResultRecord first,
                TargetCheckpointDependencies dependencies) {
            throw new IllegalStateException("original migration holder lacks actual received archive/raw/fee history");
        }

        void requireCurrent();

        void requireHistorical(
                TargetCheckpointDependencies.ControlImage input,
                TargetResultRecord first,
                com.nereusstream.delay.protocol.TargetMigrationRoot root,
                List<com.nereusstream.delay.protocol.TargetMigrationOrigin> origins,
                List<com.nereusstream.delay.protocol.TargetMigrationCustodyGuard> guards,
                List<com.nereusstream.delay.protocol.TargetMigrationPrefixRecord> originalPrefix,
                com.nereusstream.delay.protocol.TargetMigrationActivationAttempt attempt);
    }

    private final ControlTargetRegistrationAuthority registrations;
    private final TargetQuotaGrantControlVerifier.KeyAuthority keys;
    private final HistoryAuthority history;
    private final SourceAuthority source;
    private final BoundedReadBudget budget;
    private final Runnable protection;
    private final com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations;
    private final com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority writers;
    private final TargetParentControlCreditVerifier.CapacityAuthority parentCredits;
    private final TargetScopeControlObligationVerifier.CapacityAuthority scopeObligations;
    private final com.nereusstream.delay.runtime.TargetScopeFeeAuthority scopeFees;
    private final MigrationMaterials migrations;

    /** All external I/O must have finite provider deadlines; use outside Store locks with a held protection set. */
    public TargetCheckpointControlAuthority(
            ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys,
            HistoryAuthority history,
            SourceAuthority source,
            BoundedReadBudget budget,
            Runnable protection) {
        this(registrations, keys, history, source, budget, protection, null);
    }

    public TargetCheckpointControlAuthority(
            ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys,
            HistoryAuthority history,
            SourceAuthority source,
            BoundedReadBudget budget,
            Runnable protection,
            com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations) {
        this(registrations, keys, history, source, budget, protection, observations, null);
    }

    public TargetCheckpointControlAuthority(
            ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys,
            HistoryAuthority history,
            SourceAuthority source,
            BoundedReadBudget budget,
            Runnable protection,
            com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations,
            com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority writers) {
        this(registrations, keys, history, source, budget, protection, observations, writers, null);
    }

    public TargetCheckpointControlAuthority(
            ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys,
            HistoryAuthority history,
            SourceAuthority source,
            BoundedReadBudget budget,
            Runnable protection,
            com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations,
            com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority writers,
            TargetParentControlCreditVerifier.CapacityAuthority parentCredits) {
        this(registrations, keys, history, source, budget, protection, observations, writers, parentCredits, null);
    }

    public TargetCheckpointControlAuthority(
            ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys,
            HistoryAuthority history,
            SourceAuthority source,
            BoundedReadBudget budget,
            Runnable protection,
            com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations,
            com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority writers,
            TargetParentControlCreditVerifier.CapacityAuthority parentCredits,
            TargetScopeControlObligationVerifier.CapacityAuthority scopeObligations) {
        this(
                registrations,
                keys,
                history,
                source,
                budget,
                protection,
                observations,
                writers,
                parentCredits,
                scopeObligations,
                null);
    }

    public TargetCheckpointControlAuthority(
            ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys,
            HistoryAuthority history,
            SourceAuthority source,
            BoundedReadBudget budget,
            Runnable protection,
            com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations,
            com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority writers,
            TargetParentControlCreditVerifier.CapacityAuthority parentCredits,
            TargetScopeControlObligationVerifier.CapacityAuthority scopeObligations,
            com.nereusstream.delay.runtime.TargetScopeFeeAuthority scopeFees) {
        this(
                registrations,
                keys,
                history,
                source,
                budget,
                protection,
                observations,
                writers,
                parentCredits,
                scopeObligations,
                scopeFees,
                null);
    }

    private TargetCheckpointControlAuthority(
            ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys,
            HistoryAuthority history,
            SourceAuthority source,
            BoundedReadBudget budget,
            Runnable protection,
            com.nereusstream.delay.runtime.TargetSloObservationEventStore.ObservationAuthority observations,
            com.nereusstream.delay.runtime.TargetSloWriterEpochVerifier.MaterializationAuthority writers,
            TargetParentControlCreditVerifier.CapacityAuthority parentCredits,
            TargetScopeControlObligationVerifier.CapacityAuthority scopeObligations,
            com.nereusstream.delay.runtime.TargetScopeFeeAuthority scopeFees,
            MigrationMaterials migrations) {
        this.registrations = Objects.requireNonNull(registrations, "registrations");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.history = Objects.requireNonNull(history, "history");
        this.source = Objects.requireNonNull(source, "source");
        this.budget = Objects.requireNonNull(budget, "budget");
        this.protection = Objects.requireNonNull(protection, "protection");
        this.observations = observations;
        this.writers = writers;
        this.parentCredits = parentCredits;
        this.scopeObligations = scopeObligations;
        this.scopeFees = scopeFees;
        this.migrations = migrations;
    }

    /** Immutable composition under the same original budget/protection; no live Runtime qualification setter. */
    public TargetCheckpointControlAuthority withMigrationMaterials(MigrationMaterials original) {
        return new TargetCheckpointControlAuthority(
                registrations,
                keys,
                history,
                source,
                budget,
                protection,
                observations,
                writers,
                parentCredits,
                scopeObligations,
                scopeFees,
                Objects.requireNonNull(original));
    }

    @Override
    public PublicKey authenticateMigration(
            TargetCheckpointDependencies.ControlImage input,
            TargetResultRecord first,
            com.nereusstream.delay.protocol.TargetMigrationRoot root,
            List<com.nereusstream.delay.protocol.TargetMigrationOrigin> origins,
            List<com.nereusstream.delay.protocol.TargetMigrationCustodyGuard> guards,
            List<com.nereusstream.delay.protocol.TargetMigrationPrefixRecord> originalPrefix,
            com.nereusstream.delay.protocol.TargetMigrationActivationAttempt attempt) {
        final var authenticated = authenticateControl(input, first);
        if (authenticated == null) {
            return null;
        }
        final var body = com.nereusstream.delay.protocol.TargetMigrationControlBody.decode(
                input.image().canonicalBody());
        body.requireInput(input.image());
        final var actual = Objects.requireNonNull(root, "original activated migration root");
        final var intent = Objects.requireNonNull(attempt, "original durable before-SDK intent");
        intent.requirePlan(actual.plan());
        if (!actual.activated() || !actual.plan().scope().equals(body.request().scope())) {
            throw new IllegalStateException("historical migration Control changed original active root/scope");
        }
        if (body.request() instanceof com.nereusstream.delay.protocol.TargetMigrationActivateRequest activation) {
            if (!activation.plan().equals(actual.plan())
                    || !activation.candidate().equals(intent.candidate())
                    || !Arrays.equals(
                            authenticated.registered().canonicalBytes(),
                            intent.control().canonicalBytes())
                    || !Arrays.equals(
                            input.image().canonicalEnvelope(),
                            actual.activation().canonicalEnvelope())) {
                throw new IllegalStateException("historical actual A changed original exact Prepared/candidate/frame");
            }
        } else {
            final var request = (com.nereusstream.delay.protocol.TargetMigrationSettlementRequest) body.request();
            final var guard = guards.stream()
                    .filter(value -> value.messageId().equals(request.messageId()))
                    .findFirst()
                    .orElseThrow();
            if (!guard.settled()
                    || !Arrays.equals(
                            guard.settlement().canonicalEnvelope(),
                            input.image().canonicalEnvelope())) {
                throw new IllegalStateException("historical custody settlement lost original exact guard/Source");
            }
        }
        final var materials = Objects.requireNonNull(migrations, "original migration bundle/custody history authority");
        before();
        materials.requireCurrent();
        materials.requireHistorical(input, first, actual, origins, guards, originalPrefix, intent);
        if (body.request() instanceof com.nereusstream.delay.protocol.TargetMigrationActivateRequest activation
                && activation.rootIssuerAuthorization() != null) {
            final var statement = activation.rootIssuerAuthorization();
            statement.requirePlan(actual.plan());
            final var issuer = Objects.requireNonNull(
                    materials.authenticateRootIssuerAuthorization(
                            statement,
                            actual.plan(),
                            intent,
                            actual.activatedAt().source()),
                    "original independently pinned A2 management issuer");
            if (!statement.verifySignature(issuer)) {
                throw new IllegalStateException("actual A2 changed its original Root issuer statement signature");
            }
        }
        materials.requireCurrent();
        after();
        return authenticated.key();
    }

    @Override
    public PublicKey authenticateMigrationRetirement(
            TargetCheckpointDependencies.ControlImage input,
            TargetResultRecord first,
            com.nereusstream.delay.protocol.TargetMigrationRoot root,
            com.nereusstream.delay.protocol.TargetMigrationRetirementRecord retirement) {
        final var authenticated = authenticateControl(input, first);
        if (authenticated == null) {
            return null;
        }
        final var record =
                Objects.requireNonNull(retirement, "APPLIED retirement has no actual permanent Native71 record");
        record.requireRoot(Objects.requireNonNull(root));
        final var body = com.nereusstream.delay.protocol.TargetMigrationControlBody.decode(
                input.image().canonicalBody());
        body.requireInput(input.image());
        final com.nereusstream.delay.protocol.SystemMutation original;
        final com.nereusstream.delay.protocol.PreparedControlOperation prepared;
        final com.nereusstream.delay.protocol.TargetQuotaMutation at;
        if (body.request() instanceof com.nereusstream.delay.protocol.TargetMigrationRetirementIntentRequest request) {
            if (!request.equals(record.intention())) {
                throw new IllegalStateException("retirement history changed the full original inventory/member cut");
            }
            original = record.intentInput();
            prepared = record.intentControl();
            at = record.intentAt();
        } else if (body.request()
                instanceof com.nereusstream.delay.protocol.TargetMigrationRetirementConfirmRequest request) {
            if (!record.retired() || !request.equals(record.confirmation())) {
                throw new IllegalStateException("retirement history changed actual complete confirmation");
            }
            request.requireIntent(record.intention(), record.intentAt());
            original = record.confirmationInput();
            prepared = record.confirmationControl();
            at = record.confirmedAt();
        } else {
            throw new IllegalArgumentException("independent retirement authentication requires intent or confirmation");
        }
        if (!Arrays.equals(original.canonicalEnvelope(), input.image().canonicalEnvelope())
                || !first.mutation().equals(at)
                || !Arrays.equals(
                        prepared.canonicalBytes(), authenticated.registered().canonicalBytes())) {
            throw new IllegalStateException("retirement Control lost exact original Prepared/frame/Source first");
        }
        final var materials = Objects.requireNonNull(migrations, "original complete retirement materials");
        before();
        materials.requireCurrent();
        materials.requireRetirementHistorical(input, first, root, record);
        materials.requireCurrent();
        after();
        return authenticated.key();
    }

    @Override
    public PublicKey authenticateMigrationArchive(
            TargetCheckpointDependencies.ControlImage input,
            TargetResultRecord first,
            TargetCheckpointDependencies dependencies) {
        final var authenticated = authenticateControl(input, first);
        if (authenticated == null) {
            return null;
        }
        final var root = Objects.requireNonNull(dependencies.migrationRoot());
        final var retirement = Objects.requireNonNull(dependencies.migrationRetirement());
        final var body = com.nereusstream.delay.protocol.TargetMigrationControlBody.decode(
                input.image().canonicalBody());
        body.requireInput(input.image());
        if (!(body.request() instanceof com.nereusstream.delay.protocol.TargetMigrationArchiveRequest request)) {
            throw new IllegalArgumentException("archive Control required");
        }
        final var step = Objects.requireNonNull(
                dependencies.migrationArchiveState(
                        com.nereusstream.delay.protocol.TargetMigrationArchiveState.historyKey(
                                root.plan().migrationId(), first.mutation().sequence())),
                "APPLIED archive Control has no actual permanent Native71 history step");
        step.requireRetirement(root, retirement);
        if (!step.request().equals(request)
                || !Arrays.equals(
                        step.input().canonicalEnvelope(), input.image().canonicalEnvelope())
                || !Arrays.equals(
                        step.control().canonicalBytes(),
                        authenticated.registered().canonicalBytes())) {
            throw new IllegalStateException("archive history changed its full exact original Prepared/Source/receipt");
        }
        final var materials = Objects.requireNonNull(migrations);
        before();
        materials.requireCurrent();
        materials.requireArchiveHistorical(input, first, dependencies);
        materials.requireCurrent();
        after();
        return authenticated.key();
    }

    /** Reuses the genuinely registered/signed actual A and its original protected material owner. */
    @Override
    public TargetMigrationPayloadMaterials migrationPayloadMaterials(TargetCheckpointDependencies d) {
        final var root = Objects.requireNonNull(d.migrationRoot(), "actual migration root");
        final byte[] key = com.nereusstream.delay.protocol.Bytes.concat(
                new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                root.activation().systemMutationId());
        final var first = Objects.requireNonNull(d.resultRecord(key), "actual A first");
        if (authenticateMigration(
                        new TargetCheckpointDependencies.ControlImage(
                                root.activatedAt().source(), root.activation()),
                        first,
                        root,
                        d.migrationOrigins(),
                        d.migrationGuards(),
                        d.migrationPrefixes(),
                        d.migrationAttempt())
                == null) {
            throw new IllegalStateException("migration payload material has no authenticated actual A");
        }
        final var materials = Objects.requireNonNull(migrations);
        materials.requireCurrent();
        final var value = Objects.requireNonNull(materials.payloadMaterials());
        value.held().requireCurrent();
        after();
        return value;
    }

    @Override
    public com.nereusstream.delay.runtime.TargetScopeFeeAuthority scopeFeeAuthority() {
        return scopeFees;
    }

    @Override
    public PublicKey authenticateObservation(
            TargetCheckpointDependencies.ControlImage input,
            TargetResultRecord first,
            com.nereusstream.delay.protocol.TargetSloObservationGeneration generation,
            com.nereusstream.delay.protocol.TargetSloObservationSample sample) {
        return authenticateObservation(input, first, generation, sample, null);
    }

    @Override
    public PublicKey authenticateObservation(
            TargetCheckpointDependencies.ControlImage input,
            TargetResultRecord first,
            com.nereusstream.delay.protocol.TargetSloObservationGeneration generation,
            com.nereusstream.delay.protocol.TargetSloObservationSample sample,
            com.nereusstream.delay.protocol.TargetSloWriterEpoch writer) {
        protection.run();
        before();
        history.requireOriginal(input, first);
        after();
        if (!first.appliedFirst()) {
            return null;
        }
        final var body = com.nereusstream.delay.protocol.TargetSloObservationEventBody.decode(
                input.image().canonicalBody());
        if (generation == null
                || sample == null
                || sample.generation() != body.generation()
                || generation.generation() != body.generation()
                || !Arrays.equals(sample.bindingDigest(), body.bindingDigest())) {
            throw new IllegalStateException("checkpoint SLO event lost original frozen generation/binding");
        }
        generation.policy().requireSource(input.source());
        if (body.writerEpoch() == null) {
            if (writer != null
                    || generation.policy().requiresWriterEpoch()
                    || !Arrays.equals(
                            input.image().authorIdentity(),
                            generation.policy().service().canonicalBytes())
                    || input.image().signingKeyVersion() != generation.policy().keyVersion()) {
                throw new IllegalStateException("checkpoint SLO event lost its mandatory original writer epoch");
            }
        } else {
            if (writer == null || !writer.reference().equals(body.writerEpoch())) {
                throw new IllegalStateException("checkpoint body5 lacks its exact original applied writer epoch");
            }
            final var writerAuthority =
                    Objects.requireNonNull(writers, "original checkpoint writer qualification authority");
            before();
            writerAuthority.requireCurrent();
            after();
            final var contract = writer.contract();
            contract.requireSource(input.source());
            if (!generation.policy().scope().equals(contract.scope())
                    || !Arrays.equals(generation.policy().lineage(), contract.lineage())
                    || !Arrays.equals(
                            input.image().authorIdentity(), contract.service().canonicalBytes())
                    || input.image().signingKeyVersion() != contract.keyVersion()
                    || input.image().retryUntilEpochMs() > contract.maximumRetryUntil()
                    || input.image().encodeFrame().length > contract.limits().maximumFrameBytes()
                    || input.source().brokerPersistenceTimeEpochMs()
                            > input.image().retryUntilEpochMs()
                    || writer.register().appliedAt().source().compareTo(input.source()) >= 0
                    || writer.closed()
                            && writer.close()
                                            .body()
                                            .request()
                                            .fenceAt()
                                            .source()
                                            .compareTo(input.source())
                                    <= 0) {
                throw new IllegalStateException(
                        "checkpoint body5 changed its actual registered writer/deadline/Fence range");
            }
        }
        final var authority = Objects.requireNonNull(
                observations, "original checkpoint SLO endpoint/time/history and budget authority");
        final var original = body.isCustody() ? body.exportedSample() : sample;
        if (body.isCustody()) {
            body.custody().requireSample(generation, original);
            before();
            final var collectorKey = authority.collectorKey(body.custody(), input.source());
            after();
            if (collectorKey == null || !body.custody().verifySignature(collectorKey)) {
                throw new IllegalStateException(
                        "checkpoint custody lacks its original independent collector signature");
            }
        } else {
            if (body.isSourceAssigned()) {
                body.evidence().requireSample(sample);
            } else {
                body.finalObservation().validateAgainst(sample.observation().start());
                if (body.evidence() != null) {
                    body.evidence().requireObservation(sample, body.finalObservation());
                }
            }
        }
        before();
        authority.requireCurrent();
        if (body.evidence() != null && body.evidence().isRecoveryGap()) {
            authority.requireRecoveryGap(generation, original, input.image(), input.source());
        } else {
            authority.requireAuthorized(generation, original, input.image(), input.source());
        }
        authority.requireCurrent();
        after();
        before();
        final var key = keys.resolve(input.image().signingKeyVersion(), input.source());
        if (body.writerEpoch() != null) {
            writers.requireCurrent();
        }
        after();
        return key;
    }

    @Override
    public PublicKey authenticate(TargetCheckpointDependencies.ControlImage input, TargetResultRecord first) {
        if (first.appliedFirst()
                && com.nereusstream.delay.protocol.TargetSloWriterControlBody.isControl(input.image())) {
            throw new IllegalStateException(("checkpoint writer Control requires its retained Native62 and independent "
                    + "materialization authority"));
        }
        if (first.appliedFirst() && TargetParentControlCreditBody.isControl(input.image())) {
            throw new IllegalStateException(
                    ("checkpoint parent Control credit requires its retained Native63 and original "
                            + "historical funding authority"));
        }
        if (first.appliedFirst() && TargetScopeControlObligationBody.isControl(input.image())) {
            throw new IllegalStateException(
                    ("checkpoint Scope Control obligation requires its retained Native64 and original "
                            + "historical resource authority"));
        }
        final var verified = authenticateControl(input, first);
        return verified == null ? null : verified.key();
    }

    @Override
    public PublicKey authenticateParentCredit(
            TargetCheckpointDependencies.ControlImage input,
            TargetResultRecord first,
            TargetParentControlCredit credit) {
        final var authenticated = authenticateControl(input, first);
        if (authenticated == null) {
            return null;
        }
        final var actual =
                Objects.requireNonNull(credit, "APPLIED parent Control lacks its complete retained Native63");
        final var body = TargetParentControlCreditBody.decode(input.image().canonicalBody());
        final var funding = actual.funding();
        if (!body.request().definition().equals(actual.definition())
                || !Arrays.equals(
                        funding.input().canonicalEnvelope(), input.image().canonicalEnvelope())
                || !funding.appliedAt().equals(first.mutation())
                || !Arrays.equals(
                        input.source().canonicalBytes(),
                        funding.appliedAt().source().canonicalBytes())) {
            throw new IllegalStateException(
                    "checkpoint parent credit changed its actual original funding Control/first/Source");
        }
        if (parentCredits == null) {
            throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable(
                    "original checkpoint parent Control credit funding authority is unavailable");
        }
        before();
        parentCredits.requireCurrent();
        parentCredits.requireHistoricalAuthorized(actual);
        parentCredits.requireCurrent();
        after();
        return authenticated.key();
    }

    @Override
    public PublicKey authenticateScopeObligation(
            TargetCheckpointDependencies.ControlImage input,
            TargetResultRecord first,
            TargetScopeControlObligation obligation,
            TargetParentControlCredit parent) {
        final var authenticated = authenticateControl(input, first);
        if (authenticated == null) {
            return null;
        }
        final var actual =
                Objects.requireNonNull(obligation, "APPLIED Scope Control lacks its complete retained Native64");
        final var payer = Objects.requireNonNull(parent, "APPLIED Scope Control lacks its actual paid Native63 parent");
        payer.requireRoot(actual.definition().originalRoot());
        final var body = TargetScopeControlObligationBody.decode(input.image().canonicalBody());
        final var funding = actual.funding();
        if (!body.request().definition().equals(actual.definition())
                || !Arrays.equals(funding.input().encodeFrame(), input.image().encodeFrame())
                || !funding.appliedAt().equals(first.mutation())
                || !Arrays.equals(
                        input.source().canonicalBytes(),
                        funding.appliedAt().source().canonicalBytes())
                || !payer.reference().equals(actual.parentReference())
                || !payer.scope().equals(actual.scope())
                || !Arrays.equals(payer.lineage(), actual.lineage())) {
            throw new IllegalStateException(
                    "checkpoint Scope obligation changed its actual original allocation Control/first/Source/parent");
        }
        final var allocation = payer.allocations().stream()
                .filter(value -> value.scope().equals(actual.headerReference()))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(
                        "checkpoint Scope obligation lost its exact permanent parent allocation"));
        if (!Arrays.equals(allocation.allocationGrantDigest(), actual.allocationGrantDigest())
                || !allocation.commitment().equals(actual.parentCommitment())
                || !allocation.allocatedAt().equals(actual.fundedAt())) {
            throw new IllegalStateException(
                    "checkpoint Scope obligation changed its actual parent grant/commitment/Source");
        }
        if (scopeObligations == null) {
            throw new com.nereusstream.delay.ownership.TargetSourceAuthorityPreparation.InputUnavailable(
                    "original checkpoint Scope Control obligation resource authority is unavailable");
        }
        before();
        scopeObligations.requireCurrent();
        scopeObligations.requireHistoricalAuthorized(actual, payer);
        scopeObligations.requireCurrent();
        after();
        return authenticated.key();
    }

    @Override
    public PublicKey authenticateWriterEpoch(
            TargetCheckpointDependencies.ControlImage input,
            TargetResultRecord first,
            com.nereusstream.delay.protocol.TargetSloWriterEpoch writer,
            com.nereusstream.delay.protocol.TargetSloWriterEpoch predecessor) {
        final var authenticated = authenticateControl(input, first);
        if (authenticated == null) {
            return null;
        }
        final var request = com.nereusstream.delay.protocol.TargetSloWriterControlBody.decode(
                        input.image().canonicalBody())
                .request();
        final var actual =
                Objects.requireNonNull(writer, "APPLIED writer Control lacks its complete retained Native62");
        final var original = request.contract() != null
                ? actual.register()
                : Objects.requireNonNull(actual.close(), "APPLIED writer CLOSE lost its original Control");
        if (!Arrays.equals(original.input().canonicalEnvelope(), input.image().canonicalEnvelope())
                || !original.appliedAt().equals(first.mutation())) {
            throw new IllegalStateException("checkpoint writer authority changed its actual original Control/Source");
        }
        final com.nereusstream.delay.protocol.TargetSloWriterEpoch retained;
        if (request.contract() != null) {
            if (request.epoch() == 1) {
                if (predecessor != null || request.expectedPriorDigest() != null) {
                    throw new IllegalStateException("first writer REGISTER changed its predecessor");
                }
                retained = null;
            } else {
                if (predecessor == null
                        || !Arrays.equals(predecessor.contractDigest(), request.expectedPriorDigest())
                        || !Arrays.equals(
                                predecessor.contract().writerIdentityDigest(),
                                actual.contract().writerIdentityDigest())
                        || predecessor.contract().epoch() + 1 != request.epoch()
                        || predecessor.register().appliedAt().source().compareTo(input.source()) >= 0) {
                    throw new IllegalStateException("checkpoint writer REGISTER lost its original applied predecessor");
                }
                // A later CLOSE was not visible to this original REGISTER's materialization decision.
                retained = predecessor.closed()
                                && predecessor.close().appliedAt().source().compareTo(input.source()) >= 0
                        ? new com.nereusstream.delay.protocol.TargetSloWriterEpoch(predecessor.register(), null)
                        : predecessor;
            }
        } else {
            retained = new com.nereusstream.delay.protocol.TargetSloWriterEpoch(actual.register(), null);
        }
        final var authority =
                Objects.requireNonNull(writers, "original checkpoint writer resource/Fence qualification authority");
        before();
        authority.requireCurrent();
        if (request.fence() != null) {
            authority.requireFenceAuthorized(request.fence(), request.fenceAt());
        }
        authority.requireAuthorized(request, retained, input.source());
        authority.requireCurrent();
        after();
        return authenticated.key();
    }

    private AuthenticatedControl authenticateControl(
            TargetCheckpointDependencies.ControlImage input, TargetResultRecord first) {
        protection.run();
        final var result = SystemMutationResult.decode(first.typedPayload());
        final var image = input.image();
        if (first.kind() != TargetResultRecord.Kind.SYSTEM
                || result.mutationType() != SystemMutationType.APPLY_SHARD_CONTROL
                || !Arrays.equals(
                        first.mutation().source().canonicalBytes(),
                        input.source().canonicalBytes())
                || !Arrays.equals(first.mutation().mutationDigest(), Bytes.sha256(image.canonicalEnvelope()))
                || !Arrays.equals(result.mutationId(), image.systemMutationId())
                || !Arrays.equals(result.mutationHash(), image.mutationHash())
                || !Arrays.equals(result.authorIdentity(), image.authorIdentity())
                || result.retryUntilEpochMs() != image.retryUntilEpochMs()) {
            throw new IllegalArgumentException("Control authentication differs from its original first result");
        }
        before();
        history.requireOriginal(input, first);
        after();
        // An expired/unauthorized input may be rejected before key/registration/RBAC authentication.
        if (result.applyStatus() != ApplyStatus.APPLIED) {
            return null;
        }
        final var decoded = SystemMutation.decodeTargetControlFrame(image.encodeFrame());
        final var fields = SystemMutationBodyCodec.fields(decoded.type(), decoded.canonicalBody());
        final var refField = fields.stream()
                .filter(field -> field.number() == 10)
                .findFirst()
                .orElseThrow();
        final var ref = ControlRef.decode(QueryCodecSupport.bytes(refField, 10));
        before();
        final var registered = registrations
                .find(ref.operationId())
                .orElseThrow(() -> new IllegalStateException("retained APPLIED Control has no immutable registration"));
        if (!budget.tryCharge(ref.operationId().length, registered.canonicalBytes().length)) {
            throw budget.incomplete();
        }
        after();
        final var target = registered.targets().stream()
                .filter(value -> value.targetIndex() == ref.targetIndex())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Control target absent from registration"));
        ControlTargetMutationBinding.validate(registered, target, decoded);
        if (com.nereusstream.delay.ownership.TargetSloControlScopeAuthority.reference(decoded)
                .isPresent()) {
            before();
            try (var member = registrations.captureScopedMutation(
                    registered, target, decoded, budget.maxBytes() - budget.chargedBytes(), this::after)) {
                if (member.retainedBytes() > Integer.MAX_VALUE
                        || !budget.tryCharge(ref.operationId().length, (int) member.retainedBytes())) {
                    throw budget.incomplete();
                }
                member.validate(registered, target, decoded);
                after();
            }
        }
        before();
        final var preparedKey = keys.resolve((int) registered.signingKeyVersion(), input.source());
        final var imageKey = keys.resolve(decoded.signingKeyVersion(), input.source());
        after();
        if (preparedKey == null
                || imageKey == null
                || !registered.verifySignature(preparedKey)
                || !decoded.verifySignature(imageKey)) {
            throw new IllegalArgumentException("retained APPLIED Control has no authorized original signature");
        }
        final var expectedAuthor = AuthorIdentity.control(
                registered.author().operationActorIdHash(),
                registered.author().authenticatedRoleSetHash(),
                registered.author().tenantResourceScopeHash());
        if (!Arrays.equals(expectedAuthor.canonicalBytes(), decoded.authorIdentity())
                || input.source().brokerPersistenceTimeEpochMs() > decoded.retryUntilEpochMs()) {
            throw new IllegalArgumentException("APPLIED Control changes its Prepared author/original retry window");
        }
        before();
        final var authorization = Objects.requireNonNull(source.resolve(registered, input), "Source authorization");
        ControlOperationAuthorization.authorize(registered, authorization.actor(), authorization.scope());
        after();
        return new AuthenticatedControl(registered, imageKey);
    }

    /**
     * Reauthenticates the persisted initial reader/Profile input and every original Control on this image cut.
     * Invoke outside live Store monitors. Snapshot issuer trust/reader set/Profile pins are distinct from Control
     * issuer trust and must be established at the original initial Source, never at the later checkpoint cut.
     */
    public void authenticateSnapshot(
            TargetCheckpointDependencies dependencies,
            TargetCheckpointSemanticSnapshot snapshot,
            TargetCheckpointSemanticSnapshot.Limits limits,
            TargetStoreBootstrap.InitialControlAuthority initialAuthority) {
        if (snapshot.schemaVersion() < 2) {
            throw new IllegalArgumentException("Control authentication requires complete original-Control schema 2");
        }
        snapshot.validateAgainst(dependencies, limits);
        if (dependencies.migrationRoot() != null) {
            before();
            Objects.requireNonNull(migrations, "original complete migration materials")
                    .requireComplete(dependencies);
            after();
            final var firsts = new TreeMap<String, TargetResultRecord>();
            dependencies.controlResults().forEach(first -> firsts.put(Bytes.hex(first.logicalId()), first));
            for (var input : dependencies.correlateControlImages(snapshot.controls(), limits.maximumBytes())) {
                before();
                if (!budget.tryCharge(
                        input.source().canonicalBytes().length, input.image().encodeFrame().length)) {
                    throw budget.incomplete();
                }
                final var first = Objects.requireNonNull(
                        firsts.remove(Bytes.hex(input.image().systemMutationId())), "actual migration/control first");
                final var key = dependencies.authenticateInput(input, first, this);
                if (first.appliedFirst() && (key == null || !input.image().verifySignature(key))) {
                    throw new IllegalArgumentException(
                            "migration checkpoint lacks original authenticated Source Control");
                }
                after();
            }
            TargetScopeNativeUsageAudit.require(dependencies, snapshot.controls(), scopeFees, budget, protection);
            after();
            return;
        }
        Objects.requireNonNull(initialAuthority, "initialAuthority");
        final var initial = dependencies.controls().stream()
                .filter(control ->
                        control.family() == ColumnFamily.META && Arrays.equals(control.key(), KeyCodec.metaFixed(10)))
                .map(control -> TargetInitialControlSnapshot.decode(control.payload()))
                .findFirst()
                .orElseThrow(
                        () -> new IllegalArgumentException("Control authentication lacks initial reader snapshot"));
        final var firstResults = new TreeMap<String, TargetResultRecord>();
        for (var first : dependencies.controlResults()) {
            firstResults.put(
                    Bytes.hex(SystemMutationResult.decode(first.typedPayload()).mutationId()), first);
        }
        boolean initialAuthenticated = false;
        for (var input : dependencies.correlateControlImages(snapshot.controls(), limits.maximumBytes())) {
            final var first = firstResults.remove(Bytes.hex(input.image().systemMutationId()));
            before();
            if (!budget.tryCharge(
                    input.source().canonicalBytes().length, input.image().encodeFrame().length)) {
                throw budget.incomplete();
            }
            final boolean isInitial = Arrays.equals(
                    initial.initialGrant().systemMutationId(), input.image().systemMutationId());
            final var authenticated = isInitial ? authenticateControl(input, first) : null;
            if (!isInitial) {
                final var key = dependencies.authenticateInput(input, first, this);
                if (first.appliedFirst() && (key == null || !input.image().verifySignature(key))) {
                    throw new IllegalArgumentException(
                            "checkpoint original Control/event lacks its authorized signature");
                }
            }
            if (isInitial) {
                if (authenticated == null) {
                    throw new IllegalArgumentException("initial Control was not APPLIED/authenticated");
                }
                final var original =
                        TargetQuotaGrantControlBody.decode(input.image().canonicalBody());
                if (!Arrays.equals(
                                initial.initialGrant().request().canonicalBytes(),
                                original.request().canonicalBytes())
                        || !initial.initialGrant().controlRef().equals(original.controlRef())
                        || !Arrays.equals(
                                initial.authorIdentity(), input.image().authorIdentity())) {
                    throw new IllegalArgumentException("initial snapshot changes the original signed Shard grant");
                }
                initial.input()
                        .requireInitial(
                                dependencies.root().root().scope(),
                                dependencies.root().root().recoveryLineage(),
                                original.request().next(),
                                input.source(),
                                Bytes.sha256(input.image().canonicalEnvelope()));
                before();
                if (!budget.tryCharge(initial.key().length, initial.input().canonicalBytes().length)) {
                    throw budget.incomplete();
                }
                final var key = initialAuthority.authenticate(
                        initial.input(), authenticated.registered(), input.image(), input.source());
                after();
                if (key == null || !initial.input().verifySignature(key)) {
                    throw new IllegalArgumentException("initial reader/control snapshot issuer signature invalid");
                }
                initialAuthenticated = true;
            }
            after();
        }
        if (!initialAuthenticated) {
            throw new IllegalArgumentException("initial reader snapshot has no authenticated original Control");
        }
        TargetScopeNativeUsageAudit.require(dependencies, snapshot.controls(), scopeFees, budget, protection);
        after();
    }

    private record AuthenticatedControl(PreparedControlOperation registered, PublicKey key) {}

    /** Uses the same real initial issuer/reader/Profile authority, original Source frame and registration. */
    public TargetCheckpointLifecycleInputs.Authority withRootTupleIssuer(
            TargetCheckpointLifecycleInputs.Authority original,
            TargetStoreBootstrap.InitialControlAuthority initialAuthority,
            TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.ControlImage> originalInputs) {
        return new TargetCheckpointRootTupleAuthority(original, this, initialAuthority, dependencies, originalInputs);
    }
    /** Parallel actual-A Root path. It neither creates nor substitutes an ordinary initial META10 snapshot. */
    public TargetCheckpointLifecycleInputs.Authority withMigrationRootTupleIssuer(
            TargetCheckpointLifecycleInputs.Authority original,
            TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.ControlImage> originalInputs) {
        Objects.requireNonNull(migrations, "original complete management snapshot material");
        return new TargetCheckpointRootTupleAuthority(original, this, null, dependencies, originalInputs);
    }
    /** Same actual complete protected A image and material owner for the live Source activation provider. */
    public com.nereusstream.delay.runtime.TargetProtocolActivationStore.RootIssuerAuthority migrationRootTupleIssuer(
            TargetCheckpointRootVerifier.AuthenticatedImage image,
            com.nereusstream.delay.protocol.TargetProtocolActivatePayload payload) {
        Objects.requireNonNull(image);
        Objects.requireNonNull(payload);
        Objects.requireNonNull(migrations);
        return (evidence, extension, actualSource) -> {
            if (!Objects.requireNonNull(evidence).migrated()
                    || !Objects.requireNonNull(extension).equals(payload.rootAuthorization())) {
                throw new IllegalArgumentException(
                        "migration issuer acquisition changed its exact Root/tuple extension");
            }
            image.requireCurrent();
            final var dependencies = image.payloadSummaryDependencies();
            final var issuer = authenticateMigrationRootTupleIssuer(evidence, payload, actualSource, dependencies);
            image.requireCurrent();
            return issuer;
        };
    }

    void requireRootTupleAuthorization(
            com.nereusstream.delay.protocol.TargetProtocolActivationRecord activation,
            TargetStoreBootstrap.InitialControlAuthority initialAuthority,
            TargetCheckpointDependencies dependencies,
            List<TargetCheckpointDependencies.ControlImage> originalInputs) {
        final var evidence = Objects.requireNonNull(activation.rootEvidence(), "original Root issuer material");
        if (evidence.migrated()) {
            authenticateMigrationRootTupleIssuer(
                    evidence,
                    activation.body().payload(),
                    activation.appliedAt().source(),
                    dependencies);
            return;
        }
        Objects.requireNonNull(initialAuthority, "original initial Root issuer authority");
        final var initial = evidence.initial();
        final var stored = dependencies.controls().stream()
                .filter(control -> control.family() == ColumnFamily.META && Arrays.equals(control.key(), initial.key()))
                .findFirst()
                .orElseThrow();
        if (!Arrays.equals(stored.payload(), initial.canonicalBytes())) {
            throw new IllegalStateException("tuple issuer material changes the actual original META10");
        }
        final var input = originalInputs.stream()
                .filter(value -> value.source().equals(initial.mutation().source())
                        && Arrays.equals(
                                Bytes.sha256(value.image().canonicalEnvelope()),
                                initial.mutation().mutationDigest()))
                .findFirst()
                .orElseThrow(() ->
                        new IllegalStateException("tuple issuer lost the complete original initial Source frame"));
        final var first = Objects.requireNonNull(
                dependencies.resultRecord(Bytes.concat(
                        new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                        input.image().systemMutationId())),
                "original initial Source first");
        dependencies.requireSystemPosition(first);
        final var authenticated =
                Objects.requireNonNull(authenticateControl(input, first), "original Root grant was not APPLIED");
        if (!first.mutation().equals(initial.mutation())) {
            throw new IllegalStateException("tuple issuer changed the original initial first");
        }
        final var grant = TargetQuotaGrantControlBody.decode(input.image().canonicalBody());
        if (!Arrays.equals(
                        grant.request().canonicalBytes(),
                        initial.initialGrant().request().canonicalBytes())
                || !grant.controlRef().equals(initial.initialGrant().controlRef())
                || !Arrays.equals(initial.authorIdentity(), input.image().authorIdentity())) {
            throw new IllegalStateException("tuple issuer changed the original root grant/author");
        }
        initial.input()
                .requireInitial(
                        dependencies.root().root().scope(),
                        dependencies.root().root().recoveryLineage(),
                        grant.request().next(),
                        input.source(),
                        Bytes.sha256(input.image().canonicalEnvelope()));
        before();
        if (!budget.tryCharge(initial.key().length, initial.input().canonicalBytes().length)) {
            throw budget.incomplete();
        }
        final var issuer = Objects.requireNonNull(
                initialAuthority.authenticate(
                        initial.input(), authenticated.registered(), input.image(), input.source()),
                "original independently pinned Root issuer");
        after();
        evidence.require(activation.body().payload());
        if (!Arrays.equals(issuer.getEncoded(), evidence.issuerKey())
                || !initial.input().verifySignature(issuer)
                || !activation.body().payload().rootAuthorization().verifySignature(issuer)) {
            throw new IllegalStateException("tuple extension lacks the actual original snapshot issuer pin");
        }
        after();
    }

    private PublicKey authenticateMigrationRootTupleIssuer(
            com.nereusstream.delay.protocol.TargetProtocolRootIssuerEvidence evidence,
            com.nereusstream.delay.protocol.TargetProtocolActivatePayload payload,
            SourcePosition tupleSource,
            TargetCheckpointDependencies dependencies) {
        final var root = Objects.requireNonNull(dependencies.migrationRoot(), "actual Native65 Root");
        final var attempt =
                Objects.requireNonNull(dependencies.migrationAttempt(), "actual durable Native69 Prepared attempt");
        if (!evidence.migrated()
                || !Arrays.equals(root.canonicalBytes(), evidence.migration().canonicalBytes())
                || !Arrays.equals(attempt.digest(), evidence.activationAttemptDigest())) {
            throw new IllegalStateException("tuple issuer material changed actual migration Root/A/Prepared");
        }
        final var first = Objects.requireNonNull(
                dependencies.resultRecord(Bytes.concat(
                        new byte[] {TargetKeyCodec.RESULT_SYSTEM_TAG, TargetKeyCodec.KEY_FORMAT},
                        root.activation().systemMutationId())),
                "actual A first");
        dependencies.requireSystemPosition(first);
        if (authenticateMigration(
                        new TargetCheckpointDependencies.ControlImage(
                                root.activatedAt().source(), root.activation()),
                        first,
                        root,
                        dependencies.migrationOrigins(),
                        dependencies.migrationGuards(),
                        dependencies.migrationPrefixes(),
                        attempt)
                == null) {
            throw new IllegalStateException("migration tuple issuer has no authenticated actual A");
        }
        final var materials = Objects.requireNonNull(migrations);
        before();
        materials.requireComplete(dependencies);
        final var issuer = Objects.requireNonNull(
                materials.authenticateRootTupleIssuer(root, attempt, payload.rootAuthorization(), tupleSource),
                "original management snapshot issuer");
        if (root.rootIssuerAuthorization() != null) {
            final var statement = root.rootIssuerAuthorization();
            statement.requireExtension(payload.rootAuthorization());
            final var original = Objects.requireNonNull(
                    materials.authenticateRootIssuerAuthorization(
                            statement, root.plan(), attempt, root.activatedAt().source()),
                    "original actual A2 issuer policy pin");
            if (!Arrays.equals(original.getEncoded(), issuer.getEncoded()) || !statement.verifySignature(original)) {
                throw new IllegalStateException(
                        "tuple extension changed the actual independently authorized A2 issuer");
            }
        }
        evidence.require(payload);
        if (!Arrays.equals(issuer.getEncoded(), evidence.issuerKey())
                || !payload.rootAuthorization().verifySignature(issuer)) {
            throw new IllegalStateException(
                    "migration tuple extension lacks its original management snapshot issuer pin");
        }
        materials.requireCurrent();
        after();
        return issuer;
    }

    private void before() {
        protection.run();
        if (!budget.beforeRead()) {
            throw budget.incomplete();
        }
    }

    private void after() {
        protection.run();
        if (!budget.beforeTimedWork()) {
            throw budget.incomplete();
        }
    }
}
