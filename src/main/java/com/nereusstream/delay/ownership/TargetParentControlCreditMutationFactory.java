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
import com.nereusstream.delay.protocol.TargetParentControlCreditBody;
import com.nereusstream.delay.protocol.TargetParentControlCreditRequest;
import java.security.PrivateKey;
import java.util.List;
import java.util.Objects;

/** Freezes the original bootstrap credit Control and Source frame; signing does not fund its balance. */
public final class TargetParentControlCreditMutationFactory {
    private final int keyVersion;
    private final PrivateKey key;
    private final long queryPolicyVersion;
    private final long maximumFrameBytes;

    public TargetParentControlCreditMutationFactory(
            int keyVersion, PrivateKey key, long queryPolicyVersion, long maximumFrameBytes) {
        if (keyVersion == 0
                || queryPolicyVersion == 0
                || maximumFrameBytes <= 0
                || maximumFrameBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "parent Control credit requires assigned signing/query and finite frame limits");
        }
        this.keyVersion = keyVersion;
        this.key = Objects.requireNonNull(key);
        this.queryPolicyVersion = queryPolicyVersion;
        this.maximumFrameBytes = maximumFrameBytes;
    }

    public Prepared prepare(
            byte[] operationId,
            TargetParentControlCreditRequest request,
            ControlAuthorizationContext authenticated,
            ControlOperationAuthorization.TargetScopeProof scope,
            long registrationRetryUntil,
            long sourceRetryUntil) {
        final var exact = Objects.requireNonNull(request);
        final var actor = Objects.requireNonNull(authenticated);
        final var author =
                new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash());
        final var operationRequest = exact.operationRequest();
        final var ref = new ControlRef(
                operationId, PreparedControlOperation.requestHash(exact.operationKind(), operationRequest), 0);
        final var body = new TargetParentControlCreditBody(exact.scope().shard(), sourceRetryUntil, ref, exact, 1);
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
                || image.canonicalEnvelope().length > TargetParentControlCreditBody.MAX_INPUT_BYTES) {
            throw new IllegalArgumentException(
                    "parent Control credit exceeds its original registration/Source envelope bound");
        }
        return new Prepared(control, image);
    }
    /** Original immutable registration and exact signed Source bytes, retained unchanged across UNKNOWN. */
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
            if (image.type() != SystemMutationType.APPLY_SHARD_CONTROL
                    || image.canonicalEnvelope().length > TargetParentControlCreditBody.MAX_INPUT_BYTES) {
                throw new IllegalArgumentException(
                        "recovered parent Control credit changed its original protocol/bound");
            }
            final var body = TargetParentControlCreditBody.decode(image.canonicalBody());
            if (original.kind() != body.request().operationKind()
                    || original.targets().size() != 1) {
                throw new IllegalArgumentException(
                        "recovered parent Control credit differs from its original operation");
            }
            ControlTargetMutationBinding.validate(original, original.targets().getFirst(), image);
            return new Prepared(original, image);
        }
    }
}
