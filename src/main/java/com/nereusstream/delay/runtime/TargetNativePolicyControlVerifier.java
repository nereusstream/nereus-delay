package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalTargetPartition;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetMembershipGrant;
import com.nereusstream.delay.protocol.TargetNativePolicyControlBody;
import com.nereusstream.delay.protocol.TargetNativePolicyScope;
import com.nereusstream.delay.protocol.TargetQuotaGrantActivation;
import com.nereusstream.delay.protocol.TargetQuotaIdentity;
import com.nereusstream.delay.protocol.TargetQuotaIncarnation;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.runtime.TargetMembershipAuthority.AppliedGrant;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;

/** Authenticates and plans one first-applied Native authority change without writing the Store. */
public final class TargetNativePolicyControlVerifier {
    private TargetNativePolicyControlVerifier() {}

    @FunctionalInterface
    public interface KeyAuthority {
        PublicKey resolve(long signingKeyVersion, SourcePosition source);
    }

    public record Authority(
            ControlTargetRegistrationAuthority registrations,
            KeyAuthority keys,
            ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scopeProof) {
        public Authority {
            Objects.requireNonNull(registrations, "registrations");
            Objects.requireNonNull(keys, "keys");
            Objects.requireNonNull(actor, "actor");
            Objects.requireNonNull(scopeProof, "scopeProof");
        }
    }

    public enum Action {
        INSTALL_PUBLISHER,
        CLOSE_PUBLISHER,
        ACTIVATE_POLICY,
        APPROVE_MEMBER,
        CLOSE_MEMBER,
        ALREADY_CLOSED
    }

    public record Change(
            Action action, TargetNativePolicyControlBody body, TargetMembershipGrant grant, boolean registersScope) {
        public Change {
            Objects.requireNonNull(action, "action");
            Objects.requireNonNull(body, "body");
            final ControlOperationKind kind = body.request().operationKind();
            final boolean member = kind == ControlOperationKind.APPROVE_TARGET_NATIVE_MEMBER
                    || kind == ControlOperationKind.CLOSE_TARGET_NATIVE_MEMBER;
            if (member != (grant != null)) {
                throw new IllegalArgumentException("Target Native control plan member projection mismatch");
            }
        }

        public Change(
                final Action action, final TargetNativePolicyControlBody body, final TargetMembershipGrant grant) {
            this(action, body, grant, false);
        }

        public boolean createsRecord() {
            return action != Action.ALREADY_CLOSED;
        }
    }

    /** Every local and external authorization snapshot is checked before the prepared source batch is committed. */
    public static Change verifyFirstApplication(
            final TargetStoreBackend.Reader reader,
            final TargetQuotaScope shardScope,
            final byte[] lineage,
            final PreparedControlOperation prepared,
            final SystemMutation mutation,
            final SourcePosition source,
            final Authority authority) {
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(prepared, "prepared");
        Objects.requireNonNull(mutation, "mutation");
        Objects.requireNonNull(authority, "authority");
        TargetSourcePosition.requireBounded(source);
        final var body = TargetNativePolicyControlBody.decode(mutation.canonicalBody());
        final var request = body.request();
        if (!mutation.shardId().equals(source.shardId())
                || !body.shard().equals(source.shardId())
                || !Arrays.equals(mutation.logicalOperationIdentity(), body.logicalIdentity())
                || !request.operationRequest().equals(prepared.request())) {
            throw unauthorized("Target Native source/body/prepared request mismatch");
        }
        if (source.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()) {
            throw new CommandResolutionException(
                    StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                    "Target Native source mutation is outside its signed retry window");
        }
        final PublicKey preparedKey = authority.keys.resolve(prepared.signingKeyVersion(), source);
        final PublicKey mutationKey = authority.keys.resolve(mutation.signingKeyVersion(), source);
        if (preparedKey == null
                || mutationKey == null
                || !prepared.verifySignature(preparedKey)
                || !mutation.verifySignature(mutationKey)) {
            throw unauthorized("Target Native Control signature/key is not authorized at source");
        }
        final var author = AuthorIdentity.decode(mutation.authorIdentity());
        if (author.kind() != AuthorIdentity.Kind.CONTROL
                || !Arrays.equals(author.first(), prepared.author().operationActorIdHash())
                || !Arrays.equals(author.second(), prepared.author().authenticatedRoleSetHash())
                || !Arrays.equals(author.digest(), prepared.author().tenantResourceScopeHash())) {
            throw unauthorized("Target Native mutation author differs from authenticated Control author");
        }
        try {
            ControlOperationAuthorization.authorize(prepared, authority.actor, ignored -> true);
        } catch (IllegalArgumentException rejected) {
            throw unauthorized(rejected.getMessage());
        }
        if (!authority.scopeProof.covers(prepared)) {
            throw unauthorized("authenticated Control scope does not cover the complete Native target");
        }
        final var registered =
                authority.registrations.find(prepared.operationId()).orElse(null);
        if (registered == null || !Arrays.equals(prepared.canonicalBytes(), registered.canonicalBytes())) {
            throw unauthorized("Target Native Control operation is not registered exactly");
        }
        try {
            if (prepared.targets().size() != 1 || prepared.targets().getFirst().targetIndex() != 0) {
                throw new IllegalArgumentException("Target Native source Control requires exactly target index zero");
            }
            ControlTargetMutationBinding.validate(prepared, prepared.targets().getFirst(), mutation);
        } catch (IllegalArgumentException rejected) {
            throw unauthorized(rejected.getMessage());
        }
        authority.registrations.validateMutation(prepared, prepared.targets().getFirst(), mutation);
        final var scopeResolution = resolveScope(
                reader,
                shardScope,
                request.scope(),
                request.operationKind() == ControlOperationKind.INSTALL_TARGET_NATIVE_PUBLISHER_PERMISSION
                        || request.operationKind() == ControlOperationKind.APPROVE_TARGET_NATIVE_MEMBER);
        final var scope = scopeResolution.scope();
        if (!Arrays.equals(scope.controlResourceScope(), prepared.author().tenantResourceScopeHash())) {
            throw unauthorized("Target Native scope does not match the registered Control resource scope");
        }

        return switch (request.operationKind()) {
            case INSTALL_TARGET_NATIVE_PUBLISHER_PERMISSION -> {
                final byte[] installKey = TargetKeyCodec.nativePublisher(scope.digest(), request.keyGeneration());
                final byte[] closureKey =
                        TargetKeyCodec.nativePublisherClosure(scope.digest(), request.keyGeneration());
                if (TargetNativePolicyStoreAuthority.read(reader, installKey, shardScope, lineage) != null
                        || TargetNativePolicyStoreAuthority.read(reader, closureKey, shardScope, lineage) != null) {
                    throw unauthorized("Target Native publisher key generation is already used");
                }
                yield new Change(Action.INSTALL_PUBLISHER, body, null, scopeResolution.registersScope());
            }
            case CLOSE_TARGET_NATIVE_PUBLISHER_PERMISSION -> {
                final byte[] installKey = TargetKeyCodec.nativePublisher(scope.digest(), request.keyGeneration());
                final var installed = TargetNativePolicyStoreAuthority.read(reader, installKey, shardScope, lineage);
                if (installed == null) {
                    throw unauthorized("Target Native publisher close does not identify an installed key");
                }
                TargetNativePolicyStoreAuthority.requireOperation(
                        installed, ControlOperationKind.INSTALL_TARGET_NATIVE_PUBLISHER_PERMISSION);
                if (!scope.equals(installed.body().request().scope())) {
                    throw new IllegalStateException("Target Native publisher key is stored under another scope");
                }
                TargetNativePolicyStoreAuthority.requireAfter(source, installed.source(), "publisher closure");
                final byte[] closureKey =
                        TargetKeyCodec.nativePublisherClosure(scope.digest(), request.keyGeneration());
                final var closure = TargetNativePolicyStoreAuthority.read(reader, closureKey, shardScope, lineage);
                if (closure != null) {
                    TargetNativePolicyStoreAuthority.requireOperation(
                            closure, ControlOperationKind.CLOSE_TARGET_NATIVE_PUBLISHER_PERMISSION);
                    TargetNativePolicyStoreAuthority.requireSamePermission(installed, closure);
                    if (TargetNativePolicyStoreAuthority.comparePosition(source, closure.source()) < 0) {
                        throw new IllegalStateException("publisher close source precedes retained closure");
                    }
                    yield new Change(Action.ALREADY_CLOSED, body, null);
                }
                yield new Change(Action.CLOSE_PUBLISHER, body, null);
            }
            case ACTIVATE_TARGET_NATIVE_POLICY -> {
                final var snapshot = request.snapshot();
                final byte[] activationKey = TargetKeyCodec.nativeActivation(scope.digest(), snapshot.generation());
                if (TargetNativePolicyStoreAuthority.read(reader, activationKey, shardScope, lineage) != null) {
                    throw unauthorized("Target Native policy generation is already activated");
                }
                final var trust = TargetNativePolicyStoreAuthority.inView(reader, shardScope, lineage);
                final var permission = trust.publisher(scope.digest(), snapshot.issuerKeyGeneration(), source)
                        .orElseThrow(() -> unauthorized("Target Native issuer is not active at policy activation"));
                if (!scope.equals(permission.scope())
                        || snapshot.validUntilEpochMs() - snapshot.validFromEpochMs() > permission.maximumLeaseMs()
                        || !snapshot.verifySignature(permission.key())) {
                    throw unauthorized("Target Native snapshot signature, issuer or lease is not authorized");
                }
                yield new Change(Action.ACTIVATE_POLICY, body, null);
            }
            case APPROVE_TARGET_NATIVE_MEMBER -> {
                final byte[] approvalKey = TargetKeyCodec.nativeMember(scope.digest(), request.grantDigest());
                final byte[] closureKey = TargetKeyCodec.nativeMemberClosure(scope.digest(), request.grantDigest());
                if (TargetNativePolicyStoreAuthority.read(reader, approvalKey, shardScope, lineage) != null
                        || TargetNativePolicyStoreAuthority.read(reader, closureKey, shardScope, lineage) != null) {
                    throw unauthorized("Target Native member approval is already used or closed");
                }
                final var applied =
                        TargetMembershipStoreAuthority.resolve(reader, shardScope, lineage, request.grantDigest());
                if (applied == null || !applied.allowsFirstBinding(source)) {
                    throw unauthorized("Target Native member requires an active source-applied B2 grant");
                }
                requireMemberScope(reader, scope, applied);
                yield new Change(Action.APPROVE_MEMBER, body, applied.grant(), scopeResolution.registersScope());
            }
            case CLOSE_TARGET_NATIVE_MEMBER -> {
                final byte[] approvalKey = TargetKeyCodec.nativeMember(scope.digest(), request.grantDigest());
                final var approved = TargetNativePolicyStoreAuthority.read(reader, approvalKey, shardScope, lineage);
                if (approved == null) {
                    throw unauthorized("Target Native member close does not identify an approval");
                }
                TargetNativePolicyStoreAuthority.requireOperation(
                        approved, ControlOperationKind.APPROVE_TARGET_NATIVE_MEMBER);
                TargetNativePolicyStoreAuthority.requireAfter(source, approved.source(), "Native member closure");
                final var grant = approved.grant();
                final byte[] closureKey = TargetKeyCodec.nativeMemberClosure(scope.digest(), request.grantDigest());
                final var closure = TargetNativePolicyStoreAuthority.read(reader, closureKey, shardScope, lineage);
                if (closure != null) {
                    TargetNativePolicyStoreAuthority.requireOperation(
                            closure, ControlOperationKind.CLOSE_TARGET_NATIVE_MEMBER);
                    TargetNativePolicyStoreAuthority.requireSameMember(approved, closure);
                    if (TargetNativePolicyStoreAuthority.comparePosition(source, closure.source()) < 0) {
                        throw new IllegalStateException("Native member close source precedes retained closure");
                    }
                    yield new Change(Action.ALREADY_CLOSED, body, grant);
                }
                yield new Change(Action.CLOSE_MEMBER, body, grant);
            }
            default -> throw new IllegalArgumentException("not a Target Native policy Control operation");
        };
    }

    private static ScopeResolution resolveScope(
            final TargetStoreBackend.Reader reader,
            final TargetQuotaScope shardScope,
            final TargetNativePolicyScope requestScope,
            final boolean mayRegister) {
        final byte[] scopeKey = TargetKeyCodec.nativePolicyScope(requestScope.digest());
        final byte[] rawScope = reader.get(ColumnFamily.META, scopeKey);
        if (rawScope == null) {
            if (!mayRegister) {
                throw unauthorized("Target Native Control references an unregistered policy scope");
            }
            requireScopeTarget(reader, shardScope, requestScope);
            return new ScopeResolution(requestScope, true);
        }
        final var stored = com.nereusstream.delay.protocol.TargetNativePolicyScope.decodeForStore(
                scopeKey,
                TargetValueEnvelope.decode(rawScope, com.nereusstream.delay.protocol.TargetNativePolicyScope.VALUE_TYPE)
                        .payload(),
                shardScope.shard());
        if (!stored.equals(requestScope)) {
            throw unauthorized("Target Native request differs from the persisted immutable policy scope");
        }
        requireScopeTarget(reader, shardScope, stored);
        return new ScopeResolution(stored, false);
    }

    private static void requireScopeTarget(
            final TargetStoreBackend.Reader reader,
            final TargetQuotaScope shardScope,
            final TargetNativePolicyScope stored) {
        if (!stored.sourceShard().equals(shardScope.shard())) {
            throw unauthorized("Target Native scope belongs to another source Shard");
        }
        final byte[] identityKey = TargetKeyCodec.identity(stored.target());
        final byte[] rawIdentity = reader.get(ColumnFamily.META, identityKey);
        if (rawIdentity == null) {
            throw unauthorized("Target Native control target has no source-registered physical identity");
        }
        final var physical = CanonicalTargetPartition.decodeForStore(
                identityKey,
                TargetValueEnvelope.decode(rawIdentity, CanonicalTargetPartition.VALUE_TYPE)
                        .payload());
        if (!physical.id().equals(stored.target())
                || physical.resource().kind() != com.nereusstream.delay.protocol.BrokerResourceIdentity.Kind.PULSAR) {
            throw unauthorized("Target Native control requires its exact physical Pulsar Target");
        }
        final var targetScope = shardScope.forTarget(stored.target());
        final var targetIdentity = new TargetQuotaIdentity(
                TargetQuotaIdentity.Kind.TARGET,
                shardScope.shard(),
                stored.accountingIncarnation(),
                stored.target(),
                null);
        final byte[] ownerKey = targetIdentity.key();
        ownerKey[0] = TargetKeyCodec.QUOTA_INCARNATION_TAG;
        final byte[] rawOwner = reader.get(ColumnFamily.META, ownerKey);
        if (rawOwner == null) {
            throw unauthorized("Target Native control target has no exact applied quota allocation");
        }
        final var owner = TargetQuotaIncarnation.decodeForStore(
                ownerKey,
                TargetValueEnvelope.decode(rawOwner, TargetQuotaIncarnation.VALUE_TYPE)
                        .payload(),
                shardScope.shard(),
                shardScope.tenantScope());
        if (!owner.identity().equals(targetIdentity)) {
            throw new IllegalStateException("Target Native target allocation identity mismatch");
        }
        final byte[] allocationKey = Bytes.concat(
                new byte[] {TargetKeyCodec.QUOTA_GRANT_ACTIVATION_TAG, TargetKeyCodec.KEY_FORMAT},
                targetScope.keySuffix());
        final byte[] rawAllocation = reader.get(ColumnFamily.META, allocationKey);
        if (rawAllocation == null) {
            throw unauthorized("Target Native control target has no applied quota grant");
        }
        final var allocation = TargetQuotaGrantActivation.decodeForStore(
                allocationKey,
                TargetValueEnvelope.decode(rawAllocation, TargetQuotaGrantActivation.VALUE_TYPE)
                        .payload(),
                shardScope.shard(),
                shardScope.tenantScope());
        if (allocation.allocation() == null
                || !allocation.allocation().identity().equals(targetIdentity)) {
            throw unauthorized("Target Native control target allocation is not active");
        }
        allocation.mutation().requireAtOrBefore(reader.aggregate().mutation());
        owner.latestMutation().requireAtOrBefore(reader.aggregate().mutation());
    }

    private record ScopeResolution(TargetNativePolicyScope scope, boolean registersScope) {}

    private static void requireMemberScope(
            final TargetStoreBackend.Reader reader, final TargetNativePolicyScope scope, final AppliedGrant applied) {
        final TargetMembershipGrant grant = applied.grant();
        final byte[] identityKey = TargetKeyCodec.identity(scope.target());
        final byte[] rawIdentity = reader.get(ColumnFamily.META, identityKey);
        if (rawIdentity == null) {
            throw new IllegalStateException("Target Native member approval lacks physical Target identity");
        }
        final var physical = CanonicalTargetPartition.decodeForStore(
                identityKey,
                TargetValueEnvelope.decode(rawIdentity, CanonicalTargetPartition.VALUE_TYPE)
                        .payload());
        scope.requireReferences(physical, grant.offered(), grant.controls());
        if (!scope.sourceShard().equals(grant.controls().sourceShard())) {
            throw unauthorized("Target Native member grant belongs to another source scope");
        }
    }

    private static CommandResolutionException unauthorized(final String message) {
        return new CommandResolutionException(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, message);
    }
}
