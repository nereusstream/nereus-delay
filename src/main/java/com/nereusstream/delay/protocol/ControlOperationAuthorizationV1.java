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
public final class ControlOperationAuthorizationV1 {
    private ControlOperationAuthorizationV1() {}

    /** Resource-scope proof supplied by the authenticated control-plane adapter. */
    @FunctionalInterface
    public interface TargetScopeProof {
        boolean covers(PreparedControlOperationV1 prepared);
    }

    /** Authorizes one prepared operation before registration I/O. */
    public static void authorize(
            final PreparedControlOperationV1 prepared,
            final ControlAuthorizationContextV1 context,
            final TargetScopeProof scopeProof) {
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(context, "context");
        Objects.requireNonNull(scopeProof, "scopeProof");
        final ControlAuthorV1 author = prepared.author();
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
        for (ControlRoleV1 required : requiredRoles(prepared)) {
            if (!context.roleSet().contains(required)) {
                throw new IllegalArgumentException("Control role is not authorized: " + required);
            }
        }
        if (!scopeProof.covers(prepared)) {
            throw new IllegalArgumentException("authenticated Control scope does not cover all targets");
        }
    }

    private static ControlRoleV1[] requiredRoles(final PreparedControlOperationV1 prepared) {
        return switch (prepared.kind()) {
            case STOP_NEW_SCHEDULES,
                    PAUSE_DESTINATION_LANE,
                    RESUME_DESTINATION_LANE,
                    CLOSE_DESTINATION_LANE,
                    BREAK_ORDERING_DOMAIN,
                    PUBLISH_DESTINATION_PROFILE_VERSION,
                    DEPRECATE_DESTINATION_PROFILE_VERSION ->
                new ControlRoleV1[] {ControlRoleV1.TENANT_POLICY_ADMINISTRATOR};
            case REPLAY_DEAD_LETTER -> new ControlRoleV1[] {ControlRoleV1.DEAD_LETTER_OPERATOR};
            case RESOLVE_UNCERTAIN -> resolveRoles(prepared.request());
            case DRAIN_SHARD,
                    FENCE_SHARD_FOR_MAINTENANCE,
                    FORCE_CHECKPOINT,
                    GET_CHECKPOINT_CATALOG,
                    PUBLISH_QUOTA_GRANT,
                    ROTATE_EQUIVALENT_SECRET_REFERENCE -> new ControlRoleV1[] {ControlRoleV1.PLATFORM_OPERATOR};
        };
    }

    private static ControlRoleV1[] resolveRoles(final ControlOperationRequestV1 request) {
        final ResolveUncertainRequestV1 branch =
                request.branch() instanceof ResolveUncertainRequestV1 value ? value : null;
        if (branch == null) {
            throw new IllegalArgumentException("Resolve request branch is not canonical");
        }
        return branch.resolutionKind() == UncertainResolutionKindV1.RETRY_ALLOW_POSSIBLE_DUPLICATE
                        || branch.resolutionKind() == UncertainResolutionKindV1.TERMINALIZE_POSSIBLE_DELIVERY
                ? new ControlRoleV1[] {ControlRoleV1.DEAD_LETTER_OPERATOR, ControlRoleV1.TENANT_POLICY_ADMINISTRATOR}
                : new ControlRoleV1[] {ControlRoleV1.DEAD_LETTER_OPERATOR};
    }
}
