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
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationBodyCodec;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.runtime.ApplyStatus;
import com.nereusstream.delay.runtime.SystemMutationResult;
import com.nereusstream.delay.runtime.TargetQuotaGrantControlVerifier;
import com.nereusstream.delay.runtime.TargetResultRecord;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;

/** Exact retained Control authentication; external original-Source key/role/scope/history protection is mandatory. */
public final class TargetCheckpointControlAuthority implements TargetCheckpointDependencies.ControlImageAuthority {
    public record Authorization(ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scope) {
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

    private final ControlTargetRegistrationAuthority registrations;
    private final TargetQuotaGrantControlVerifier.KeyAuthority keys;
    private final HistoryAuthority history;
    private final SourceAuthority source;
    private final BoundedReadBudget budget;
    private final Runnable protection;

    /** All external I/O must have finite provider deadlines; use outside Store locks with a held protection set. */
    public TargetCheckpointControlAuthority(ControlTargetRegistrationAuthority registrations,
            TargetQuotaGrantControlVerifier.KeyAuthority keys, HistoryAuthority history, SourceAuthority source,
            BoundedReadBudget budget, Runnable protection) {
        this.registrations = Objects.requireNonNull(registrations, "registrations");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.history = Objects.requireNonNull(history, "history");
        this.source = Objects.requireNonNull(source, "source");
        this.budget = Objects.requireNonNull(budget, "budget");
        this.protection = Objects.requireNonNull(protection, "protection");
    }

    @Override
    public PublicKey authenticate(TargetCheckpointDependencies.ControlImage input, TargetResultRecord first) {
        protection.run();
        final var result = SystemMutationResult.decode(first.typedPayload());
        final var image = input.image();
        if (first.kind() != TargetResultRecord.Kind.SYSTEM
                || result.mutationType() != SystemMutationType.APPLY_SHARD_CONTROL
                || !Arrays.equals(first.mutation().source().canonicalBytes(), input.source().canonicalBytes())
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
        if (result.applyStatus() != ApplyStatus.APPLIED) { return null; }
        final var decoded = SystemMutation.decodeTargetControlFrame(image.encodeFrame());
        final var fields = SystemMutationBodyCodec.fields(decoded.type(), decoded.canonicalBody());
        final var refField = fields.stream().filter(field -> field.number() == 10).findFirst().orElseThrow();
        final var ref = ControlRef.decode(QueryCodecSupport.bytes(refField, 10));
        before();
        final var registered = registrations.find(ref.operationId())
                .orElseThrow(() -> new IllegalStateException("retained APPLIED Control has no immutable registration"));
        if (!budget.tryCharge(ref.operationId().length, registered.canonicalBytes().length)) {
            throw budget.incomplete();
        }
        after();
        final var target = registered.targets().stream()
                .filter(value -> value.targetIndex() == ref.targetIndex())
                .findFirst().orElseThrow(() -> new IllegalArgumentException("Control target absent from registration"));
        ControlTargetMutationBinding.validate(registered, target, decoded);
        before();
        final var preparedKey = keys.resolve((int) registered.signingKeyVersion(), input.source());
        final var imageKey = keys.resolve(decoded.signingKeyVersion(), input.source());
        after();
        if (preparedKey == null || imageKey == null || !registered.verifySignature(preparedKey)
                || !decoded.verifySignature(imageKey)) {
            throw new IllegalArgumentException("retained APPLIED Control has no authorized original signature");
        }
        final var expectedAuthor = AuthorIdentity.control(registered.author().operationActorIdHash(),
                registered.author().authenticatedRoleSetHash(), registered.author().tenantResourceScopeHash());
        if (!Arrays.equals(expectedAuthor.canonicalBytes(), decoded.authorIdentity())
                || input.source().brokerPersistenceTimeEpochMs() > decoded.retryUntilEpochMs()) {
            throw new IllegalArgumentException("APPLIED Control changes its Prepared author/original retry window");
        }
        before();
        final var authorization = Objects.requireNonNull(source.resolve(registered, input), "Source authorization");
        ControlOperationAuthorization.authorize(registered, authorization.actor(), authorization.scope());
        after();
        return imageKey;
    }

    private void before() {
        protection.run();
        if (!budget.beforeRead()) { throw budget.incomplete(); }
    }

    private void after() {
        protection.run();
        if (!budget.beforeTimedWork()) { throw budget.incomplete(); }
    }
}
