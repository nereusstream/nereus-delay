package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.SystemMutationType;
import com.nereusstream.delay.protocol.TargetCloseBody;
import com.nereusstream.delay.protocol.TargetCloseRequest;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;

/** Source-position authorization of a platform Close; registration/coverage failures are never semantic denials. */
public final class TargetCloseVerifier {
    private TargetCloseVerifier() {}

    @FunctionalInterface
    public interface Keys {
        PublicKey resolve(int version, SourcePosition source);
    }

    /**
     * Prove the complete affected source set, Route/resource/tenant authority and close-policy safety at source.
     * Preserve admitted/possible-delivery obligations; prove required strict-order loss acknowledgements and
     * fencing of new admissions. The protected snapshot must remain valid until commit on this Shard.
     * A caller-selected subset or a boolean administrative role does not prove coverage.
     */
    @FunctionalInterface
    public interface Coverage {
        void requireAuthorized(
                TargetQuotaScope scope, TargetCloseRequest request, SourcePosition source, TargetQueueState queue);
    }

    public record Authority(
            ControlTargetRegistrationAuthority registrations,
            Keys keys,
            Coverage coverage,
            ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scopeProof) {
        public Authority {
            Objects.requireNonNull(registrations, "registrations");
            Objects.requireNonNull(keys, "keys");
            Objects.requireNonNull(coverage, "coverage");
            Objects.requireNonNull(actor, "actor");
            Objects.requireNonNull(scopeProof, "scopeProof");
        }
    }

    public static final class Decision {
        private final TargetCloseBody body;
        private final StableCode rejection;

        private Decision(TargetCloseBody body, StableCode rejection) {
            this.body = body;
            this.rejection = rejection;
        }

        public TargetCloseBody body() {
            return body;
        }

        public StableCode rejection() {
            return rejection;
        }
    }

    public static Decision decideFirstApplication(
            TargetQuotaScope scope,
            PreparedControlOperation prepared,
            SystemMutation mutation,
            SourcePosition source,
            TargetQueueState queue,
            Authority authority) {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        if (scope.target() != null
                || !scope.shard().equals(source.shardId())
                || !scope.shard().equals(mutation.shardId())
                || prepared.kind() != ControlOperationKind.CLOSE_TARGET
                || mutation.type() != SystemMutationType.APPLY_SHARD_CONTROL) {
            return reject(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final TargetCloseBody body;
        final AuthorIdentity author;
        try {
            body = TargetCloseBody.decode(mutation.canonicalBody());
            if (!body.shard().equals(scope.shard())
                    || body.retryUntil() != mutation.retryUntilEpochMs()
                    || !body.request().operationRequest().equals(prepared.request())
                    || !Arrays.equals(body.controlRef().operationId(), prepared.operationId())
                    || !Arrays.equals(body.logicalIdentity(), mutation.logicalOperationIdentity())) {
                return reject(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
            }
            ControlTargetMutationBinding.validate(
                    prepared, prepared.targets().get((int) body.controlRef().targetIndex()), mutation);
            author = AuthorIdentity.decode(mutation.authorIdentity());
            ControlOperationAuthorization.authorize(prepared, authority.actor(), ignored -> true);
        } catch (IllegalArgumentException invalid) {
            return reject(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        if (source.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()) {
            return reject(StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED);
        }
        if (author.kind() != AuthorIdentity.Kind.CONTROL
                || !Arrays.equals(author.first(), prepared.author().operationActorIdHash())
                || !Arrays.equals(author.second(), prepared.author().authenticatedRoleSetHash())
                || !Arrays.equals(author.digest(), prepared.author().tenantResourceScopeHash())) {
            return reject(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final var preparedKey = authority.keys().resolve((int) prepared.signingKeyVersion(), source);
        final var mutationKey = authority.keys().resolve(mutation.signingKeyVersion(), source);
        if (preparedKey == null
                || mutationKey == null
                || !prepared.verifySignature(preparedKey)
                || !mutation.verifySignature(mutationKey)
                || !authority.scopeProof().covers(prepared)) {
            return reject(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        final var registered =
                authority.registrations().find(prepared.operationId()).orElse(null);
        if (registered == null || !Arrays.equals(registered.canonicalBytes(), prepared.canonicalBytes())) {
            return reject(StableCode.UNAUTHORIZED_SYSTEM_MUTATION);
        }
        authority
                .registrations()
                .validateMutation(
                        prepared, prepared.targets().get((int) body.controlRef().targetIndex()), mutation);
        authority.coverage().requireAuthorized(scope, body.request(), source, queue);
        if (queue == null) {
            return reject(StableCode.NOT_FOUND);
        }
        final var selected = body.request().requireControlRef(body.controlRef());
        if (!queue.targetId().equals(body.request().target())
                || !Arrays.equals(queue.accountingIncarnation(), selected.accountingIncarnation())) {
            return reject(StableCode.RESOURCE_INCARNATION_MISMATCH);
        }
        if (queue.controlVersion() != selected.expectedControlVersion()) {
            return reject(StableCode.VERSION_CONFLICT);
        }
        if (queue.admissionState() == TargetQueueState.AdmissionState.CLOSED) {
            return reject(StableCode.TARGET_CLOSED);
        }
        return new Decision(body, null);
    }

    private static Decision reject(StableCode code) {
        return new Decision(null, code);
    }
}
