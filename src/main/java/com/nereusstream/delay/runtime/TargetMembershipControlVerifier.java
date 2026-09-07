package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.DestinationProfileSemantic;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetDispatchCompatibility;
import com.nereusstream.delay.protocol.TargetMembershipControlBody;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetMembershipPolicy;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;

/** Authenticated first-application planning; the caller commits its change with the source result and position. */
public final class TargetMembershipControlVerifier {
    private TargetMembershipControlVerifier() {}

    /** Exact immutable policy approved for this operation at this physical source position. No hash-only approval. */
    @FunctionalInterface
    public interface PolicyAuthority {
        TargetMembershipPolicy resolve(byte[] policyRef, SourcePosition source, ControlOperationKind operation);
    }
    /** Source-protected Control signing keys, including retained keys required to verify historical registrations. */
    @FunctionalInterface
    public interface KeyAuthority {
        PublicKey resolve(int signingKeyVersion, SourcePosition source);
    }

    public record Authority(
            ControlTargetRegistrationAuthority registrations,
            PolicyAuthority policies,
            KeyAuthority keys,
            ProfileCatalog profiles,
            ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scopeProof) {
        public Authority {
            Objects.requireNonNull(registrations, "registrations");
            Objects.requireNonNull(policies, "policies");
            Objects.requireNonNull(keys, "keys");
            Objects.requireNonNull(profiles, "profiles");
            Objects.requireNonNull(actor, "actor");
            Objects.requireNonNull(scopeProof, "scopeProof");
        }
    }

    public enum Action {
        GRANT,
        CLOSE,
        ALREADY_CLOSED
    }

    public record Change(
            Action action,
            TargetMembershipAuthority.AppliedGrant before,
            TargetMembershipAuthority.AppliedGrant after) {
        public Change {
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(after, "after");
            if ((action == Action.GRANT) != (before == null)) {
                throw new IllegalArgumentException("membership change prior-state presence mismatch");
            }
        }
    }

    /**
     * The source applier must resolve deduplication first, preserve the first applied source position, and fence
     * all supplied authorities in one Owner/Store/source snapshot. This method performs no writes or source advance.
     */
    public static Change verifyFirstApplication(
            final PreparedControlOperation prepared,
            final SystemMutation mutation,
            final SourcePosition source,
            final CanonicalTargetPartition physical,
            final TargetMembershipAuthority membership,
            final Authority authority) {
        TargetSourcePosition.requireBounded(source);
        final var body = TargetMembershipControlBody.decode(mutation.canonicalBody());
        final var request = body.request();
        if (!mutation.shardId().equals(source.shardId())
                || !body.shard().equals(source.shardId())
                || !Arrays.equals(mutation.logicalOperationIdentity(), body.logicalIdentity())
                || !request.operationRequest().equals(prepared.request())) {
            throw unauthorized("membership source/body/prepared request mismatch");
        }
        if (source.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()) {
            throw new CommandResolutionException(
                    StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                    "membership source mutation is outside its signed retry window");
        }
        final PublicKey preparedKey = authority.keys.resolve((int) prepared.signingKeyVersion(), source);
        final PublicKey mutationKey = authority.keys.resolve(mutation.signingKeyVersion(), source);
        if (preparedKey == null
                || mutationKey == null
                || !prepared.verifySignature(preparedKey)
                || !mutation.verifySignature(mutationKey)) {
            throw unauthorized("membership Control signature/key is not authorized at source");
        }
        final var author = AuthorIdentity.decode(mutation.authorIdentity());
        if (author.kind() != AuthorIdentity.Kind.CONTROL
                || !Arrays.equals(author.first(), prepared.author().operationActorIdHash())
                || !Arrays.equals(author.second(), prepared.author().authenticatedRoleSetHash())
                || !Arrays.equals(author.digest(), prepared.author().tenantResourceScopeHash())) {
            throw unauthorized("membership mutation author differs from authenticated registered author");
        }
        // Classify local actor/RBAC rejection without hiding failures from the external scope authority.
        try {
            ControlOperationAuthorization.authorize(prepared, authority.actor, ignored -> true);
        } catch (IllegalArgumentException rejected) {
            throw unauthorized(rejected.getMessage());
        }
        if (!authority.scopeProof.covers(prepared)) {
            throw unauthorized("authenticated Control scope does not cover the complete membership target");
        }
        final var registered =
                authority.registrations.find(prepared.operationId()).orElse(null);
        if (registered == null || !Arrays.equals(prepared.canonicalBytes(), registered.canonicalBytes())) {
            throw unauthorized("membership Control operation is not registered exactly");
        }
        authority.registrations.validateMutation(prepared, prepared.targets().getFirst(), mutation);
        final var approved = authority.policies.resolve(request.policy().digest(), source, request.operationKind());
        if (approved == null
                || !approved.equals(request.policy())
                || !Arrays.equals(approved.controlResourceScope(), authority.actor.tenantResourceScopeHash())) {
            throw unauthorized("membership policy is not approved for this exact Control/source scope");
        }
        approved.offered().requireTargetProjection(physical);
        if (request.isIssue()) {
            final var grant = TargetMembershipGrant.fromRegistration(request.value(), mutation.mutationHash(), source);
            approved.requireGrant(grant);
            final var destination = authority.profiles.resolve(approved.memberProfile());
            if (destination == null
                    || !destination.ref().equals(approved.memberProfile())
                    || !(destination.body() instanceof DestinationProfileSemantic dest)) {
                throw unauthorized("membership Destination Profile is not available exactly");
            }
            final var capability = authority.profiles.resolve(dest.deliveryCapability());
            if (capability == null
                    || !capability.ref().equals(dest.deliveryCapability())
                    || !TargetDispatchCompatibility.fromProfiles(physical, destination, capability)
                            .equals(grant.required())) {
                throw unauthorized("membership required contract differs from exact Profile semantics");
            }
            grant.requireSourceRegistration(request.value(), prepared.operationId(), mutation.mutationHash(), source);
            return new Change(Action.GRANT, null, new TargetMembershipAuthority.AppliedGrant(grant, null));
        }
        final var before = membership.resolve(request.value());
        if (before == null || !Arrays.equals(before.grant().digest(), request.value())) {
            throw unauthorized("membership close does not identify an applied grant");
        }
        approved.requireGrant(before.grant());
        if (source.compareTo(before.grant().activationSource()) <= 0) {
            throw unauthorized("membership close does not follow its grant activation");
        }
        if (before.closedAt() != null) {
            final int order = source.compareTo(before.closedAt());
            if (order < 0
                    || (order == 0
                            && !Arrays.equals(
                                    source.canonicalBytes(), before.closedAt().canonicalBytes()))) {
                throw new IllegalArgumentException("membership close snapshot has a conflicting future source");
            }
            return new Change(Action.ALREADY_CLOSED, before, before);
        }
        return new Change(Action.CLOSE, before, new TargetMembershipAuthority.AppliedGrant(before.grant(), source));
    }

    private static CommandResolutionException unauthorized(final String message) {
        return new CommandResolutionException(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, message);
    }
}
