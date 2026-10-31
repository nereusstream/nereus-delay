package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaMutation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSloWriterContract;
import com.nereusstream.delay.protocol.TargetSloWriterControlBody;
import com.nereusstream.delay.protocol.TargetSloWriterControlRequest;
import com.nereusstream.delay.protocol.TargetSloWriterEpoch;
import com.nereusstream.delay.protocol.TargetTimeFenceBody;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Original Control authority and exact Source-ordered writer-epoch transitions. */
public final class TargetSloWriterEpochVerifier {
    private TargetSloWriterEpochVerifier() {}

    @FunctionalInterface
    public interface KeyAuthority {
        PublicKey resolve(long signingKeyVersion, SourcePosition originalSource);
    }

    /** Already-held qualifications; missing resource or independent Fence authority fails closed. */
    public interface MaterializationAuthority {
        default TargetScopeFeeAuthority scopeFees() {
            return TargetScopeFeeAuthority.UNAVAILABLE;
        }

        default void requireAuthorized(
                TargetSloWriterControlRequest request, TargetSloWriterEpoch retained, SourcePosition source) {
            throw new IllegalStateException("original SLO writer resource qualification is not configured");
        }

        default void requireFenceAuthorized(SystemMutation originalInput, TargetQuotaMutation fenceAt) {
            throw new IllegalStateException("independent original TIME_FENCE qualification is not configured");
        }

        void requireCurrent();
    }

    public record Authority(
            ControlTargetRegistrationAuthority registrations,
            KeyAuthority keys,
            ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scopeProof,
            MaterializationAuthority materialization) {
        public Authority {
            Objects.requireNonNull(registrations);
            Objects.requireNonNull(keys);
            Objects.requireNonNull(actor);
            Objects.requireNonNull(scopeProof);
            Objects.requireNonNull(materialization);
        }
    }

    public record Change(TargetSloWriterControlBody body, TargetSloWriterEpoch retained) {}

    public static Change verifyFirstApplication(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            PreparedControlOperation prepared,
            SystemMutation input,
            SourcePosition source,
            Authority authority) {
        Objects.requireNonNull(authority);
        authority.materialization().requireCurrent();
        final var body = TargetSloWriterControlBody.decode(input.canonicalBody());
        final var request = body.request();
        if (!scope.equals(request.scope())
                || !Arrays.equals(lineage, request.lineage())
                || !scope.shard().equals(source.shardId())
                || !scope.shard().equals(input.shardId())
                || input.type() != SystemMutationType.APPLY_SHARD_CONTROL
                || input.retryUntilEpochMs() != body.retryUntil()
                || !Arrays.equals(input.logicalOperationIdentity(), body.logicalIdentity())
                || !request.operationRequest().equals(prepared.request())
                || prepared.targets().size() != 1) {
            throw denied("SLO writer Control changed its registered Source/request scope");
        }
        if (Long.compareUnsigned(source.brokerPersistenceTimeEpochMs(), body.retryUntil()) > 0) {
            throw new CommandResolutionException(
                    StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                    "SLO writer Control exceeded its original retry window");
        }
        final var preparedKey = authority.keys().resolve(prepared.signingKeyVersion(), source);
        final var inputKey = authority.keys().resolve(Integer.toUnsignedLong(input.signingKeyVersion()), source);
        if (preparedKey == null
                || inputKey == null
                || !prepared.verifySignature(preparedKey)
                || !input.verifySignature(inputKey)) {
            throw denied("SLO writer Control lacks its original signatures");
        }
        final var author = AuthorIdentity.decode(input.authorIdentity());
        if (author.kind() != AuthorIdentity.Kind.CONTROL
                || !Arrays.equals(author.first(), prepared.author().operationActorIdHash())
                || !Arrays.equals(author.second(), prepared.author().authenticatedRoleSetHash())
                || !Arrays.equals(author.digest(), prepared.author().tenantResourceScopeHash())) {
            throw denied("SLO writer Source author differs from its original Control actor");
        }
        try {
            ControlOperationAuthorization.authorize(prepared, authority.actor(), authority.scopeProof());
            ControlTargetMutationBinding.validate(prepared, prepared.targets().getFirst(), input);
        } catch (IllegalArgumentException rejected) {
            throw denied(rejected.getMessage());
        }
        final var registered =
                authority.registrations().find(prepared.operationId()).orElse(null);
        if (registered == null || !Arrays.equals(prepared.canonicalBytes(), registered.canonicalBytes())) {
            throw denied("SLO writer Control lacks its exact original registration");
        }
        authority.registrations().validateMutation(prepared, prepared.targets().getFirst(), input);
        if (body.admissionScope() != null) {
            authority
                    .registrations()
                    .validateScopedMutation(prepared, prepared.targets().getFirst(), input);
            final var paid =
                    TargetScopeControlObligationStore.requirePaidScope(reader, scope, lineage, body.admissionScope());
            paid.requireSource(source);
            if (paid.fundedAt().source().compareTo(source) >= 0) {
                throw stale("SLO writer Control requires its strictly prior Source-paid Scope");
            }
            // Frozen logical frame cap only; no Source occurrence or Native settlement is performed here.
            if (input.encodeFrame().length > paid.definition().plan().maximumSourceFrameBytes()) {
                throw new CommandResolutionException(
                        StableCode.HARD_QUOTA_EXCEEDED,
                        "SLO writer Control exceeds its paid Scope logical Source frame cap");
            }
        }

        final var activation = TargetProtocolActivationStore.applied(reader, scope, lineage, body.tuple());
        if (activation == null || activation.appliedAt().source().compareTo(source) >= 0) {
            throw new CommandResolutionException(
                    StableCode.UNACTIVATED_SYSTEM_PROTOCOL_VERSION,
                    "SLO writer epoch requires its independent prior activation");
        }
        final var root = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.SHARD,
                scope.shard(),
                reader.aggregate().accountingIncarnation(),
                null,
                null);
        final TargetSloWriterEpoch retained;
        if (request.operationKind() == ControlOperationKind.REGISTER_TARGET_SLO_WRITER_EPOCH) {
            final var contract = Objects.requireNonNull(request.contract());
            contract.requireSource(source);
            if (source.compareTo(contract.sourceAnchor()) <= 0) {
                throw denied("SLO writer REGISTER does not follow its original resource anchor");
            }
            if (Long.compareUnsigned(contract.epoch(), contract.limits().maximumEpochs()) > 0) {
                throw new CommandResolutionException(
                        StableCode.HARD_QUOTA_EXCEEDED, "SLO writer retained epoch window is full");
            }
            if (readEpoch(reader, scope, lineage, contract.writerIdentityDigest(), contract.epoch()) != null) {
                throw stale("SLO writer REGISTER names an existing exact epoch");
            }
            if (contract.epoch() == 1) {
                if (request.expectedPriorDigest() != null) {
                    throw stale("first SLO writer epoch has a predecessor");
                }
                retained = null;
            } else {
                retained = readEpoch(reader, scope, lineage, contract.writerIdentityDigest(), contract.epoch() - 1);
                if (retained == null
                        || request.expectedPriorDigest() == null
                        || !Arrays.equals(retained.contractDigest(), request.expectedPriorDigest())
                        || TargetQuotaMutation.increment(retained.contract().epoch()) != contract.epoch()) {
                    throw stale("SLO writer REGISTER lost its exact previous applied contract");
                }
                TargetSloWriterEpochStore.requireOrigin(
                        reader,
                        List.of(),
                        scope,
                        lineage,
                        root,
                        retained,
                        reader.aggregate().mutation());
                if (source.compareTo(retained.register().appliedAt().source()) <= 0) {
                    throw stale("SLO writer REGISTER does not advance the previous REGISTER Source");
                }
            }
        } else if (request.operationKind() == ControlOperationKind.CLOSE_TARGET_SLO_WRITER_EPOCH) {
            final var reference = Objects.requireNonNull(request.reference());
            retained = readEpoch(reader, scope, lineage, reference.writerIdentityDigest(), reference.epoch());
            if (retained == null || !retained.reference().equals(reference) || retained.closed()) {
                throw stale("SLO writer CLOSE does not name its exact open applied contract");
            }
            TargetSloWriterEpochStore.requireOrigin(
                    reader,
                    List.of(),
                    scope,
                    lineage,
                    root,
                    retained,
                    reader.aggregate().mutation());
            retained.contract().requireSource(source);
            try {
                retained.contract().requireSource(request.fenceAt().source());
            } catch (IllegalArgumentException foreign) {
                throw denied("SLO writer CLOSE changed its original Fence physical Source");
            }
            if (Long.compareUnsigned(
                                    request.fenceAt().sequence(),
                                    retained.register().appliedAt().sequence())
                            <= 0
                    || request.fenceAt()
                                    .source()
                                    .compareTo(retained.register().appliedAt().source())
                            <= 0
                    || request.fenceAt().source().compareTo(source) >= 0
                    || TargetTimeFenceBody.decode(request.fence().canonicalBody())
                                    .closeThrough()
                            < retained.contract().maximumRetryUntil()) {
                throw stale("SLO writer CLOSE lacks an intervening Fence covering its original deadline");
            }
            TargetSloWriterEpochStore.requireCloseFence(
                    reader,
                    List.of(),
                    scope,
                    lineage,
                    root,
                    retained,
                    request.fence(),
                    request.fenceAt(),
                    source,
                    reader.aggregate().mutation());
            if (reader.closedIngressDeadlineThrough() < retained.contract().maximumRetryUntil()) {
                throw stale("SLO writer CLOSE lacks the actual cumulative TIME_FENCE boundary");
            }
            authority.materialization().requireFenceAuthorized(request.fence(), request.fenceAt());
        } else {
            throw denied("not a SLO writer epoch lifecycle operation");
        }
        authority.materialization().requireAuthorized(request, retained, source);
        authority.materialization().requireCurrent();
        return new Change(body, retained);
    }

    public static TargetSloWriterEpoch readEpoch(
            TargetStoreBackend.Reader reader,
            TargetQuotaScope scope,
            byte[] lineage,
            TargetSloWriterContract.Reference reference) {
        Objects.requireNonNull(reference);
        final var epoch = readEpoch(reader, scope, lineage, reference.writerIdentityDigest(), reference.epoch());
        if (epoch != null && !epoch.reference().equals(reference)) {
            throw new IllegalStateException("SLO writer reference changed its original applied REGISTER");
        }
        return epoch;
    }

    private static TargetSloWriterEpoch readEpoch(
            TargetStoreBackend.Reader reader, TargetQuotaScope scope, byte[] lineage, byte[] writer, long epoch) {
        final var key = TargetSloWriterEpoch.key(scope, writer, epoch);
        final var raw = reader.get(ColumnFamily.META, key);
        return raw == null
                ? null
                : TargetSloWriterEpoch.decodeForStore(
                        key,
                        TargetValueEnvelope.decode(raw, TargetSloWriterEpoch.VALUE_TYPE)
                                .payload(),
                        scope,
                        lineage);
    }

    private static CommandResolutionException denied(String message) {
        return new CommandResolutionException(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, message);
    }

    private static CommandResolutionException stale(String message) {
        return new CommandResolutionException(StableCode.STALE_SYSTEM_MUTATION, message);
    }
}
