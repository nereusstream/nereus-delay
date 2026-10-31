package com.nereusstream.delay.runtime;

import com.nereusstream.delay.ownership.ControlTargetRegistrationAuthority;
import com.nereusstream.delay.protocol.AuthorIdentity;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlOperationKind;
import com.nereusstream.delay.protocol.ControlTargetMutationBinding;
import com.nereusstream.delay.protocol.OwnerIdentity;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.StableCode;
import com.nereusstream.delay.protocol.SystemMutation;
import com.nereusstream.delay.protocol.TargetChannelControlBody;
import com.nereusstream.delay.protocol.TargetChannelControlRecord;
import com.nereusstream.delay.protocol.TargetChannelControlRequest;
import com.nereusstream.delay.protocol.TargetChannelIdentity;
import com.nereusstream.delay.protocol.TargetChannelSlot;
import com.nereusstream.delay.protocol.TargetQueueState;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.store.ColumnFamily;
import com.nereusstream.delay.store.TargetKeyCodec;
import com.nereusstream.delay.store.TargetStoreBackend;
import com.nereusstream.delay.store.TargetValueEnvelope;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Objects;

/** Authenticates a channel lifecycle event and plans only its exact Source-owned slot transition. */
public final class TargetChannelControlVerifier {
    private TargetChannelControlVerifier() {}

    @FunctionalInterface
    public interface KeyAuthority {
        PublicKey resolve(long signingKeyVersion, SourcePosition source);
    }

    /** Captured credential/resource/dispatch/schema/reader permissions must remain protected through commit. */
    @FunctionalInterface
    public interface MaterializationAuthority {
        void requireAuthorized(TargetChannelControlRequest request, SourcePosition source);
    }

    public record Authority(
            ControlTargetRegistrationAuthority registrations,
            KeyAuthority keys,
            ControlAuthorizationContext actor,
            ControlOperationAuthorization.TargetScopeProof scopeProof,
            MaterializationAuthority materialization,
            int activatedChannelSlots,
            OwnerIdentity currentOwner) {
        public Authority {
            Objects.requireNonNull(registrations, "registrations");
            Objects.requireNonNull(keys, "keys");
            Objects.requireNonNull(actor, "actor");
            Objects.requireNonNull(scopeProof, "scopeProof");
            Objects.requireNonNull(materialization, "materialization");
            Objects.requireNonNull(currentOwner, "currentOwner");
            if (activatedChannelSlots <= 0 || activatedChannelSlots == Integer.MAX_VALUE) {
                throw new IllegalArgumentException("channel slot activation must be finite and positive");
            }
        }
    }

    public record Change(TargetChannelControlBody body, TargetChannelSlot prior) {}

    public static Change verifyFirstApplication(
            final TargetStoreBackend.Reader reader,
            final TargetQuotaScope scope,
            final byte[] lineage,
            final PreparedControlOperation prepared,
            final SystemMutation mutation,
            final SourcePosition source,
            final Authority authority,
            final OwnerIdentity actualOwner) {
        Objects.requireNonNull(authority, "authority");
        final var body = TargetChannelControlBody.decode(mutation.canonicalBody());
        final var request = body.request();
        if (!body.shard().equals(scope.shard())
                || !body.shard().equals(source.shardId())
                || !mutation.shardId().equals(source.shardId())
                || !Arrays.equals(mutation.logicalOperationIdentity(), body.logicalIdentity())
                || mutation.retryUntilEpochMs() != body.retryUntil()
                || !request.operationRequest().equals(prepared.request())) {
            throw unauthorized("channel Source/body/registered request mismatch");
        }
        if (source.brokerPersistenceTimeEpochMs() > mutation.retryUntilEpochMs()) {
            throw new CommandResolutionException(
                    StableCode.SYSTEM_MUTATION_RETRY_WINDOW_EXPIRED,
                    "channel lifecycle exceeded its signed retry window");
        }
        final PublicKey preparedKey = authority.keys.resolve(prepared.signingKeyVersion(), source);
        final PublicKey mutationKey = authority.keys.resolve(mutation.signingKeyVersion(), source);
        if (preparedKey == null
                || mutationKey == null
                || !prepared.verifySignature(preparedKey)
                || !mutation.verifySignature(mutationKey)) {
            throw unauthorized("Target channel Control signature/key is not authorized at source");
        }
        final var author = AuthorIdentity.decode(mutation.authorIdentity());
        if (author.kind() != AuthorIdentity.Kind.CONTROL
                || !Arrays.equals(author.first(), prepared.author().operationActorIdHash())
                || !Arrays.equals(author.second(), prepared.author().authenticatedRoleSetHash())
                || !Arrays.equals(author.digest(), prepared.author().tenantResourceScopeHash())) {
            throw unauthorized("Target channel mutation author differs from authenticated Control author");
        }
        try {
            ControlOperationAuthorization.authorize(prepared, authority.actor, ignored -> true);
        } catch (IllegalArgumentException rejected) {
            throw unauthorized(rejected.getMessage());
        }
        if (!authority.scopeProof.covers(prepared)) {
            throw unauthorized("authenticated Control scope does not cover the complete channel target");
        }
        final var registered =
                authority.registrations.find(prepared.operationId()).orElse(null);
        if (registered == null || !Arrays.equals(prepared.canonicalBytes(), registered.canonicalBytes())) {
            throw unauthorized("Target channel Control operation is not registered exactly");
        }
        try {
            if (prepared.targets().size() != 1 || prepared.targets().getFirst().targetIndex() != 0) {
                throw new IllegalArgumentException("Target channel source Control requires exactly target index zero");
            }
            ControlTargetMutationBinding.validate(prepared, prepared.targets().getFirst(), mutation);
        } catch (IllegalArgumentException rejected) {
            throw unauthorized(rejected.getMessage());
        }
        authority.registrations.validateMutation(prepared, prepared.targets().getFirst(), mutation);
        if (!request.owner().equals(Objects.requireNonNull(actualOwner, "actualOwner"))) {
            throw new CommandResolutionException(
                    StableCode.STALE_SYSTEM_MUTATION, "channel lifecycle requested a different current Owner");
        }
        authority.materialization.requireAuthorized(request, source);
        final var proposed = request.channel();
        final var prior = readSlot(reader, proposed, lineage);
        final boolean create = request.operationKind() == ControlOperationKind.CREATE_TARGET_CHANNEL;
        if (create != (prior == null)) {
            throw unauthorized("channel slot absence differs from its registered lifecycle precondition");
        }
        if (prior != null) {
            if (!Arrays.equals(prior.channel().digest(), request.expectedPriorDigest())) {
                throw unauthorized("channel lifecycle has a different current predecessor");
            }
            if (source.compareTo(prior.mutation().source()) <= 0) {
                throw new IllegalStateException("channel lifecycle does not advance its original Source");
            }
        }
        switch (request.operationKind()) {
            case CREATE_TARGET_CHANNEL, RENEW_TARGET_CHANNEL -> {
                final var queueKey = TargetKeyCodec.state(proposed.context().target());
                final var queueRaw = reader.get(ColumnFamily.META, queueKey);
                if (queueRaw == null) {
                    throw unauthorized("channel creation/renewal lacks its Target queue");
                }
                final var queue =
                        TargetQueueState.decode(TargetValueEnvelope.decode(queueRaw, TargetQueueState.VALUE_TYPE)
                                .payload());
                try {
                    proposed.requireNewWorkProjection(queue, authority.activatedChannelSlots);
                } catch (IllegalArgumentException denied) {
                    throw unauthorized(denied.getMessage());
                }
                if (prior != null) {
                    if (prior.phase() != TargetChannelSlot.Phase.OPEN) {
                        throw unauthorized("stopped channel cannot be renewed as new work");
                    }
                    try {
                        proposed.requireSuccessorOf(prior.channel());
                    } catch (IllegalArgumentException denied) {
                        throw unauthorized(denied.getMessage());
                    }
                }
                if (reader.get(ColumnFamily.META, proposed.encodedKey()) != null) {
                    throw unauthorized("channel immutable generation has already been registered");
                }
            }
            case STOP_TARGET_CHANNEL_NEW_WORK -> {
                prior.channel().requireExactFrozenIdentity(proposed);
            }
            default -> throw unauthorized("unknown channel lifecycle operation");
        }
        return new Change(body, prior);
    }

    /** Reads the actual current slot and original immutable event; never defaults a bare NV20 to initialized. */
    public static TargetChannelSlot readSlot(
            final TargetStoreBackend.Reader reader, final TargetChannelIdentity channel, final byte[] lineage) {
        final byte[] key = TargetKeyCodec.channelSlot(channel.context().producerIdentity());
        final byte[] raw = reader.get(ColumnFamily.META, key);
        if (raw == null) {
            return null;
        }
        final var slot = TargetChannelSlot.decodeForStore(
                key,
                TargetValueEnvelope.decode(raw, TargetChannelSlot.VALUE_TYPE).payload(),
                reader.shardId());
        if (!Arrays.equals(slot.recoveryLineage(), lineage)) {
            throw new IllegalStateException("channel slot belongs to another recovery lineage");
        }
        slot.mutation().requireAtOrBefore(reader.aggregate().mutation());
        final byte[] eventRaw = reader.get(ColumnFamily.META, slot.controlKey());
        if (eventRaw == null) {
            throw new IllegalStateException("channel slot lacks its original Source event");
        }
        final var event = TargetChannelControlRecord.decodeForStore(
                slot.controlKey(),
                TargetValueEnvelope.decode(eventRaw, TargetChannelControlRecord.VALUE_TYPE)
                        .payload(),
                reader.shardId());
        slot.requireControl(event);
        final byte[] identityRaw = reader.get(ColumnFamily.META, slot.channel().encodedKey());
        if (identityRaw == null
                || !Arrays.equals(
                        TargetValueEnvelope.decode(identityRaw, TargetChannelIdentity.VALUE_TYPE)
                                .payload(),
                        slot.channel().canonicalBytes())) {
            throw new IllegalStateException("channel slot lacks its exact immutable identity");
        }
        return slot;
    }

    private static CommandResolutionException unauthorized(final String message) {
        return new CommandResolutionException(StableCode.UNAUTHORIZED_SYSTEM_MUTATION, message);
    }
}
