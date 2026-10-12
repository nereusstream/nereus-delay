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
import com.nereusstream.delay.protocol.TargetChannelControlBody;
import com.nereusstream.delay.protocol.TargetChannelControlRequest;
import java.security.PrivateKey;
import java.util.List;
import java.util.Objects;

/** Prepares exact signed channel operation and Source frame once, before any registration or append I/O. */
public final class TargetChannelControlMutationFactory {
    private final int keyVersion;
    private final PrivateKey key;
    private final long queryPolicyVersion;
    private final long maximumFrameBytes;

    public TargetChannelControlMutationFactory(final int keyVersion, final PrivateKey key,
            final long queryPolicyVersion, final long maximumFrameBytes) {
        if (keyVersion == 0 || queryPolicyVersion == 0
                || maximumFrameBytes <= 0 || maximumFrameBytes == Long.MAX_VALUE) {
            throw new IllegalArgumentException("channel control requires assigned signing/query and finite frame limits");
        }
        this.keyVersion = keyVersion;
        this.key = Objects.requireNonNull(key, "key");
        this.queryPolicyVersion = queryPolicyVersion;
        this.maximumFrameBytes = maximumFrameBytes;
    }

    /** Receipt/query and Source retry policies are explicit; neither is inferred from a channel lease expiration. */
    public Prepared prepare(final byte[] operationId, final TargetChannelControlRequest request,
            final ControlAuthorizationContext authenticated, final ControlOperationAuthorization.TargetScopeProof scope,
            final long registrationRetryUntil, final long sourceRetryUntil) {
        final var exact = Objects.requireNonNull(request, "request");
        final var actor = Objects.requireNonNull(authenticated, "authenticated");
        final var author = new ControlAuthor(actor.actorIdHash(), actor.roleSet().digest(), actor.tenantResourceScopeHash());
        final var operationRequest = exact.operationRequest();
        final var ref = new ControlRef(operationId,
                PreparedControlOperation.requestHash(exact.operationKind(), operationRequest), 0);
        final var body = new TargetChannelControlBody(exact.channel().context().sourceShard(), sourceRetryUntil,
                ref, exact, exact.expectedPriorDigest() == null ? 1 : 0);
        final var image = SystemMutation.signed(body.shard(), SystemMutationType.APPLY_SHARD_CONTROL,
                sourceRetryUntil, body.logicalIdentity(), body.canonicalBytes(),
                AuthorIdentity.control(author.operationActorIdHash(), author.authenticatedRoleSetHash(),
                        author.tenantResourceScopeHash()).canonicalBytes(), keyVersion, key);
        final var target = new ControlTargetRef(0, ControlTargetKind.SHARD, new ShardSubject(body.shard()),
                image.systemMutationId(), image.mutationHash());
        final var prepared = PreparedControlOperation.prepare(operationId, exact.operationKind(), author,
                operationRequest, List.of(target), queryPolicyVersion, registrationRetryUntil,
                Integer.toUnsignedLong(keyVersion), key);
        ControlOperationAuthorization.authorize(prepared, actor, Objects.requireNonNull(scope, "scope"));
        ControlTargetMutationBinding.validate(prepared, target, image);
        if (image.encodeFrame().length > maximumFrameBytes || prepared.canonicalBytes().length > maximumFrameBytes) {
            throw new IllegalArgumentException("channel lifecycle exceeds its configured registration/Source envelope");
        }
        return new Prepared(prepared, image);
    }

    /** Immutable retry inputs. Constructed here only after the exact dual-signature/target binding is complete. */
    public static final class Prepared {
        private final PreparedControlOperation control;
        private final SystemMutation mutation;

        private Prepared(final PreparedControlOperation control, final SystemMutation mutation) {
            this.control = control;
            this.mutation = mutation;
        }

        public PreparedControlOperation control() { return control; }
        public SystemMutation mutation() { return mutation; }

        /** Reuses original registered/signed bytes recovered from protected history; no signing or Source allocation. */
        public static Prepared restore(final PreparedControlOperation control, final SystemMutation mutation) {
            final var body = TargetChannelControlBody.decode(Objects.requireNonNull(mutation, "mutation")
                    .canonicalBody());
            final var registered = Objects.requireNonNull(control, "control");
            if (registered.kind() != body.request().operationKind() || registered.targets().size() != 1) {
                throw new IllegalArgumentException("recovered lifecycle differs from its registered operation");
            }
            ControlTargetMutationBinding.validate(registered, registered.targets().getFirst(), mutation);
            return new Prepared(registered, mutation);
        }
    }
}
