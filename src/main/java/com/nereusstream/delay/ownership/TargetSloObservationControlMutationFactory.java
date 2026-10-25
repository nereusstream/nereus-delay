package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import java.security.PrivateKey;
import java.util.List;
import java.util.Objects;

/** Freezes the original signed certification Control and Source frame before registration or append. */
public final class TargetSloObservationControlMutationFactory {
    private final int keyVersion;
    private final PrivateKey key;
    private final long queryPolicyVersion;
    private final long maximumFrameBytes;

    public TargetSloObservationControlMutationFactory(int keyVersion, PrivateKey key,
            long queryPolicyVersion, long maximumFrameBytes) {
        if (keyVersion == 0 || queryPolicyVersion == 0 || maximumFrameBytes <= 0
                || maximumFrameBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("Target SLO Control requires assigned signing/query and finite frame limits");
        }
        this.keyVersion = keyVersion; this.key = Objects.requireNonNull(key);
        this.queryPolicyVersion = queryPolicyVersion; this.maximumFrameBytes = maximumFrameBytes;
    }

    /** Signing does not make a generation effective or qualify its budget, custody or reader contract. */
    public Prepared prepare(byte[] operationId, TargetSloObservationControlRequest request,
            ControlAuthorizationContext authenticated, ControlOperationAuthorization.TargetScopeProof scope,
            long registrationRetryUntil, long sourceRetryUntil) {
        return prepare(operationId,request,authenticated,scope,null,registrationRetryUntil,sourceRetryUntil);
    }
    public Prepared prepare(byte[] operationId,TargetSloObservationControlRequest request,
            ControlAuthorizationContext authenticated,ControlOperationAuthorization.TargetScopeProof scope,
            TargetSloControlScopeHeader.Reference admissionScope,long registrationRetryUntil,long sourceRetryUntil){
        final var exact = Objects.requireNonNull(request);
        final var actor = Objects.requireNonNull(authenticated);
        final var author = new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash());
        final var operationRequest = exact.operationRequest();
        final var ref = new ControlRef(operationId, PreparedControlOperation.requestHash(exact.operationKind(), operationRequest), 0);
        final var body = new TargetSloObservationControlBody(exact.scope().shard(), sourceRetryUntil,
                ref, exact, exact.expectedPriorDigest() == null ? 1 : 0,admissionScope);
        final var image = SystemMutation.signed(body.shard(), SystemMutationType.APPLY_SHARD_CONTROL,
                sourceRetryUntil, body.logicalIdentity(), body.canonicalBytes(),
                AuthorIdentity.control(author.operationActorIdHash(), author.authenticatedRoleSetHash(),
                        author.tenantResourceScopeHash()).canonicalBytes(), keyVersion, key);
        final var target = new ControlTargetRef(0, ControlTargetKind.SHARD, new ShardSubject(body.shard()),
                image.systemMutationId(), image.mutationHash());
        final var control = PreparedControlOperation.prepare(operationId, exact.operationKind(), author,
                operationRequest, List.of(target), queryPolicyVersion, registrationRetryUntil,
                Integer.toUnsignedLong(keyVersion), key);
        ControlOperationAuthorization.authorize(control, actor, Objects.requireNonNull(scope));
        ControlTargetMutationBinding.validate(control, target, image);
        if (image.encodeFrame().length > maximumFrameBytes || control.canonicalBytes().length > maximumFrameBytes) {
            throw new IllegalArgumentException("Target SLO Control exceeds its configured registration/Source envelope");
        }
        return new Prepared(control, image);
    }

    /** Exact retry bytes; an append receipt does not imply APPLIED or durable certification coverage. */
    public static final class Prepared {
        private final PreparedControlOperation control;
        private final SystemMutation mutation;
        private Prepared(PreparedControlOperation control, SystemMutation mutation) {
            this.control = control; this.mutation = mutation;
        }
        public PreparedControlOperation control() { return control; }
        public SystemMutation mutation() { return mutation; }

        /** Reuses protected original signed inputs without a new signature, operation ID or Source position. */
        public static Prepared restore(PreparedControlOperation control, SystemMutation mutation) {
            final var image = Objects.requireNonNull(mutation);
            final var body = TargetSloObservationControlBody.decode(image.canonicalBody());
            final var original = Objects.requireNonNull(control);
            if (original.kind() != body.request().operationKind() || original.targets().size() != 1) {
                throw new IllegalArgumentException("recovered Target SLO Control differs from its original operation");
            }
            ControlTargetMutationBinding.validate(original, original.targets().getFirst(), image);
            return new Prepared(original, image);
        }
    }
}
