package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.*;
import java.security.PrivateKey;
import java.util.List;
import java.util.Objects;

/** Original independent CONTROL signing/preparation; registration and actual Source application remain separate. */
public final class TargetMigrationControlMutationFactory {
    private final int keyVersion;
    private final PrivateKey key;
    private final long queryPolicyVersion;
    private final long maximumFrameBytes;

    public TargetMigrationControlMutationFactory(int keyVersion, PrivateKey key, long queryPolicyVersion, long maximumFrameBytes) {
        if (keyVersion == 0 || queryPolicyVersion == 0 || maximumFrameBytes <= 0 || maximumFrameBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("migration requires assigned Control signing/query and finite original frame limits");
        }
        this.keyVersion = keyVersion; this.key = Objects.requireNonNull(key);
        this.queryPolicyVersion = queryPolicyVersion; this.maximumFrameBytes = maximumFrameBytes;
    }

    public Prepared prepare(byte[] operationId, TargetMigrationRequest request, ControlAuthorizationContext authenticated,
            ControlOperationAuthorization.TargetScopeProof exactScope, long registrationRetryUntil, long sourceRetryUntil) {
        final var original = Objects.requireNonNull(request); final var actor = Objects.requireNonNull(authenticated);
        final var author = new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash());
        final var operationRequest = original.operationRequest();
        final var ref = new ControlRef(operationId, PreparedControlOperation.requestHash(original.operationKind(), operationRequest), 0);
        final var body = new TargetMigrationControlBody(original.scope().shard(), sourceRetryUntil, ref, original);
        final var input = SystemMutation.signed(body.shard(), SystemMutationType.APPLY_SHARD_CONTROL, sourceRetryUntil,
                body.logicalIdentity(), body.canonicalBytes(), AuthorIdentity.control(author.operationActorIdHash(),
                        author.authenticatedRoleSetHash(), author.tenantResourceScopeHash()).canonicalBytes(), keyVersion, key);
        final var target = new ControlTargetRef(0, ControlTargetKind.SHARD, new ShardSubject(body.shard()), input.systemMutationId(), input.mutationHash());
        final var control = PreparedControlOperation.prepare(operationId, original.operationKind(), author, operationRequest,
                List.of(target), queryPolicyVersion, registrationRetryUntil, Integer.toUnsignedLong(keyVersion), key);
        ControlOperationAuthorization.authorize(control, actor, Objects.requireNonNull(exactScope));
        ControlTargetMutationBinding.validate(control, target, input);
        body.requireInput(input);
        if (input.encodeFrame().length > maximumFrameBytes || control.canonicalBytes().length > maximumFrameBytes) {
            throw new IllegalArgumentException("migration Control exceeds its independently supplied finite Source/registration bounds");
        }
        return new Prepared(control, input);
    }

    /** Exact bytes survive UNKNOWN. Restoring neither registers nor authorizes a fresh signature. */
    public static final class Prepared {
        private final PreparedControlOperation control;
        private final SystemMutation mutation;
        private Prepared(PreparedControlOperation control, SystemMutation mutation) { this.control = control; this.mutation = mutation; }
        public PreparedControlOperation control() { return control; }
        public SystemMutation mutation() { return mutation; }
        public static Prepared restore(PreparedControlOperation control, SystemMutation mutation) {
            final var original = Objects.requireNonNull(control); final var input = Objects.requireNonNull(mutation);
            final var body = TargetMigrationControlBody.decode(input.canonicalBody()); body.requireInput(input);
            if (original.kind() != body.request().operationKind() || original.targets().size() != 1) {
                throw new IllegalArgumentException("migration retry changed its complete original operation");
            }
            ControlTargetMutationBinding.validate(original, original.targets().getFirst(), input);
            return new Prepared(original, input);
        }
    }
}
