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
import com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationBody;
import com.nereusstream.delay.protocol.TargetPayloadSummaryInitializationRequest;
import java.security.PrivateKey;
import java.util.List;
import java.util.Objects;

/** Independently authorized platform Control preparation for the exact protected old-owner fold. */
public final class TargetPayloadSummaryControlMutationFactory {
    private final int keyVersion;
    private final PrivateKey key;
    private final long queryPolicyVersion, maximumFrameBytes;

    public TargetPayloadSummaryControlMutationFactory(
            int keyVersion, PrivateKey key, long queryPolicyVersion, long maximumFrameBytes) {
        if (keyVersion == 0
                || queryPolicyVersion == 0
                || maximumFrameBytes < 1
                || maximumFrameBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("summary management signing/query/frame limits must be assigned");
        }
        this.keyVersion = keyVersion;
        this.key = Objects.requireNonNull(key);
        this.queryPolicyVersion = queryPolicyVersion;
        this.maximumFrameBytes = maximumFrameBytes;
    }

    public Prepared prepare(
            byte[] operationId,
            TargetPayloadSummaryInitializationRequest request,
            ControlAuthorizationContext authenticated,
            ControlOperationAuthorization.TargetScopeProof exactScope,
            long registrationRetryUntil,
            long sourceRetryUntil) {
        final var actor = Objects.requireNonNull(authenticated);
        final var author =
                new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash());
        final var ref = new ControlRef(
                operationId,
                PreparedControlOperation.requestHash(request.operationKind(), request.operationRequest()),
                0);
        final var body =
                new TargetPayloadSummaryInitializationBody(request.scope().shard(), sourceRetryUntil, ref, request);
        final var input = SystemMutation.signed(
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
                input.systemMutationId(),
                input.mutationHash());
        final var control = PreparedControlOperation.prepare(
                operationId,
                request.operationKind(),
                author,
                request.operationRequest(),
                List.of(target),
                queryPolicyVersion,
                registrationRetryUntil,
                Integer.toUnsignedLong(keyVersion),
                key);
        ControlOperationAuthorization.authorize(control, actor, Objects.requireNonNull(exactScope));
        ControlTargetMutationBinding.validate(control, target, input);
        body.requireInput(input);
        if (input.encodeFrame().length > maximumFrameBytes || control.canonicalBytes().length > maximumFrameBytes) {
            throw new IllegalArgumentException("summary Control exceeds original finite Source/registration bounds");
        }
        return new Prepared(control, input);
    }

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
            final var body = TargetPayloadSummaryInitializationBody.decode(mutation.canonicalBody());
            body.requireInput(mutation);
            if (control.kind() != body.request().operationKind()
                    || control.targets().size() != 1
                    || !control.request().equals(body.request().operationRequest())) {
                throw new IllegalArgumentException("summary UNKNOWN retry changed original complete request");
            }
            ControlTargetMutationBinding.validate(control, control.targets().getFirst(), mutation);
            return new Prepared(control, mutation);
        }
    }
}
