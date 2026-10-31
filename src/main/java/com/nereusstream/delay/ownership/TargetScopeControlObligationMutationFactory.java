package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.ControlAuthor;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlRef;
import com.nereusstream.delay.protocol.ControlTargetKind;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.ControlTargetRef;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.ShardSubject;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetScopeControlObligationBody;
import com.nereusstream.delay.protocol.TargetScopeControlObligationRequest;
import java.security.PrivateKey;
import java.util.List;
import java.util.Objects;

/** Freezes the independently funded bootstrap Control and Source frame; signing grants no resource authority. */
public final class TargetScopeControlObligationMutationFactory {
    private final int keyVersion;
    private final PrivateKey key;
    private final long queryPolicyVersion;
    private final long maximumFrameBytes;

    public TargetScopeControlObligationMutationFactory(
            int keyVersion, PrivateKey key, long queryPolicyVersion, long maximumFrameBytes) {
        if (keyVersion == 0
                || queryPolicyVersion == 0
                || maximumFrameBytes <= 0
                || maximumFrameBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Scope Control obligation requires assigned signing/query and finite frame limits");
        }
        this.keyVersion = keyVersion;
        this.key = Objects.requireNonNull(key);
        this.queryPolicyVersion = queryPolicyVersion;
        this.maximumFrameBytes = maximumFrameBytes;
    }

    public Prepared prepare(
            byte[] operationId,
            TargetScopeControlObligationRequest request,
            ControlAuthorizationContext authenticated,
            ControlOperationAuthorization.TargetScopeProof scope,
            long registrationRetryUntil,
            long sourceRetryUntil) {
        requireFiniteRetry(registrationRetryUntil);
        requireFiniteRetry(sourceRetryUntil);
        final var exact = Objects.requireNonNull(request);
        final var actor = Objects.requireNonNull(authenticated);
        final var author =
                new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash());
        final var operationRequest = exact.operationRequest();
        final var ref = new ControlRef(
                operationId, PreparedControlOperation.requestHash(exact.operationKind(), operationRequest), 0);
        // The immutable request plan selects the original body schema/tuple before signing either envelope.
        final var body = new TargetScopeControlObligationBody(exact.scope().shard(), sourceRetryUntil, ref, exact, 1);
        final var image = SystemMutation.signed(
                body.shard(),
                SystemMutationType.APPLY_SHARD_CONTROL,
                sourceRetryUntil,
                body.logicalIdentity(),
                body.canonicalBytes(),
                AuthorIdentity.control(
                                author.operationActorIdHash(),
                                author.authenticatedRoleSetHash(),
                                author.tenantResourceScopeHash())
                        .canonicalBytes(),
                keyVersion,
                key);
        final var target = new ControlTargetRef(
                0,
                ControlTargetKind.SHARD,
                new ShardSubject(body.shard()),
                image.systemMutationId(),
                image.mutationHash());
        final var control = PreparedControlOperation.prepare(
                operationId,
                exact.operationKind(),
                author,
                operationRequest,
                List.of(target),
                queryPolicyVersion,
                registrationRetryUntil,
                Integer.toUnsignedLong(keyVersion),
                key);
        ControlOperationAuthorization.authorize(control, actor, Objects.requireNonNull(scope));
        ControlTargetMutationBinding.validate(control, target, image);
        if (image.encodeFrame().length > maximumFrameBytes
                || control.canonicalBytes().length > maximumFrameBytes
                || image.canonicalEnvelope().length > TargetScopeControlObligationBody.MAX_INPUT_BYTES) {
            throw new IllegalArgumentException(
                    "Scope Control obligation exceeds its original registration/Source envelope bound");
        }
        return new Prepared(control, image);
    }

    private static void requireFiniteRetry(long retryUntil) {
        if (retryUntil < 0 || retryUntil == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Scope Control obligation requires finite independent retry deadlines");
        }
    }
    /**
     * Original plan schema, immutable registration and exact signed Source bytes, retained unchanged across UNKNOWN.
     */
    public static final class Prepared {
        private final PreparedControlOperation control;
        private final SystemMutation mutation;

        private Prepared(PreparedControlOperation control, SystemMutation mutation) {
            this.control = control;
            this.mutation = mutation;
        }

        public PreparedControlOperation control() {
            return control;
        }

        public SystemMutation mutation() {
            return mutation;
        }

        public static Prepared restore(PreparedControlOperation control, SystemMutation mutation) {
            final var original = Objects.requireNonNull(control);
            final var image = Objects.requireNonNull(mutation);
            requireFiniteRetry(original.registrationRetryUntil());
            requireFiniteRetry(image.retryUntilEpochMs());
            if (image.type() != SystemMutationType.APPLY_SHARD_CONTROL
                    || image.canonicalEnvelope().length > TargetScopeControlObligationBody.MAX_INPUT_BYTES) {
                throw new IllegalArgumentException(
                        "recovered Scope Control obligation changed its original protocol/bound");
            }
            final var body = TargetScopeControlObligationBody.decode(image.canonicalBody());
            if (original.kind() != body.request().operationKind()
                    || original.targets().size() != 1) {
                throw new IllegalArgumentException(
                        "recovered Scope Control obligation differs from its original operation");
            }
            ControlTargetMutationBinding.validate(original, original.targets().getFirst(), image);
            return new Prepared(original, image);
        }
    }
}
