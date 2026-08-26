package com.nereusstream.delay.protocol;

import java.util.Objects;

/**
 * Local RBAC and resource-scope gate for a prepared Control Operation.
 *
 * <p>The caller supplies an authenticator-produced context and a resource
 * scope proof. This class checks their exact hash binding and the Registry's
 * minimum-role matrix; it does not validate certificates, tokens, Oxia state,
 * or target existence.</p>
 */
public final class ControlOperationAuthorization {
    private ControlOperationAuthorization() {}

    /** Resource-scope proof supplied by the authenticated control-plane adapter. */
    @FunctionalInterface
    public interface TargetScopeProof {
        boolean covers(PreparedControlOperation prepared);
    }

    /** Authorizes one prepared operation before registration I/O. */
    public static void authorize(
            final PreparedControlOperation prepared,
            final ControlAuthorizationContext context,
            final TargetScopeProof scopeProof) {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(scopeProof, "scopeProof");
        final ControlAuthor author = prepared.author();
        if (!Bytes.constantTimeEquals(author.operationActorIdHash(), context.actorIdHash())) {
            throw new IllegalArgumentException("Control actor hash is not authenticated for this operation");
        }
        if (!Bytes.constantTimeEquals(
                author.authenticatedRoleSetHash(), context.roleSet().digest())) {
            throw new IllegalArgumentException("Control role-set hash is not authenticated for this operation");
        }
        if (!Bytes.constantTimeEquals(author.tenantResourceScopeHash(), context.tenantResourceScopeHash())) {
            throw new IllegalArgumentException("Control resource-scope hash is not authenticated for this operation");
        }
        for (ControlRole required : requiredRoles(prepared)) {
            if (!context.roleSet().contains(required)) {
                throw new IllegalArgumentException("Control role is not authorized: " + required);
            }
        }
        if (!scopeProof.covers(prepared)) {
            throw new IllegalArgumentException("authenticated Control scope does not cover all targets");
        }
    }

    private static ControlRole[] requiredRoles(final PreparedControlOperation prepared) {
        return switch (prepared.kind()) {
            case STOP_NEW_SCHEDULES,
                    PAUSE_DESTINATION_LANE,
                    RESUME_DESTINATION_LANE,
                    CLOSE_DESTINATION_LANE,
                    BREAK_ORDERING_DOMAIN,
                    PUBLISH_DESTINATION_PROFILE_VERSION,
                    DEPRECATE_DESTINATION_PROFILE_VERSION ->
                new ControlRole[] {ControlRole.TENANT_POLICY_ADMINISTRATOR};
            case REPLAY_DEAD_LETTER -> new ControlRole[] {ControlRole.DEAD_LETTER_OPERATOR};
            case RESOLVE_UNCERTAIN -> resolveRoles(prepared.request());
            case DRAIN_SHARD,
                    FENCE_SHARD_FOR_MAINTENANCE,
                    FORCE_CHECKPOINT,
                    GET_CHECKPOINT_CATALOG,
                    PUBLISH_QUOTA_GRANT,
                    ROTATE_EQUIVALENT_SECRET_REFERENCE -> new ControlRole[] {ControlRole.PLATFORM_OPERATOR};
        };
    }

    private static ControlRole[] resolveRoles(final ControlOperationRequest request) {
        final ResolveUncertainRequest branch = request.branch() instanceof ResolveUncertainRequest value ? value : null;
        if (branch == null) {
            throw new IllegalArgumentException("Resolve request branch is not canonical");
        }
        return branch.resolutionKind() == UncertainResolutionKind.RETRY_ALLOW_POSSIBLE_DUPLICATE
                        || branch.resolutionKind() == UncertainResolutionKind.TERMINALIZE_POSSIBLE_DELIVERY
                ? new ControlRole[] {ControlRole.DEAD_LETTER_OPERATOR, ControlRole.TENANT_POLICY_ADMINISTRATOR}
                : new ControlRole[] {ControlRole.DEAD_LETTER_OPERATOR};
    }
}
