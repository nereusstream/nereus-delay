package com.nereusstream.delay.protocol;

import com.nereusstream.delay.runtime.CurrentSendWorkKind;
import com.nereusstream.delay.runtime.TargetClaimRecord;
import com.nereusstream.delay.runtime.TargetMessageRecord;
import java.util.Arrays;
import java.util.Objects;

/** Exact ordinary managed publication commitment. It is not a credential, permit or physical success proof. */
public record TargetOrdinaryPublicationBinding(
        TargetMessageLocator locator,
        CanonicalTargetPartition physical,
        TargetChannelIdentity channel,
        byte[] publishAttemptId,
        int attemptNo,
        byte[] claimDigest,
        byte[] claimedMessageDigest,
        long claimedMessageVersion,
        ProfileRef destinationProfile,
        ProfileRef capabilityProfile,
        long payloadLength,
        byte[] payloadSha256,
        AdapterMetadata adapterMetadata,
        ReservedPublishMetadata reservedMetadata,
        long deliverAtEpochMs,
        long expireAtEpochMs,
        long retryEligibilityAtEpochMs,
        Long eventTimeEpochMs,
        byte[] artifactGenerationSetDigest) {
    public static final int VERSION = 1;
    public static final int MAX_CANONICAL_BYTES = TargetMessageLocator.MAX_CANONICAL_BYTES
            + CanonicalTargetPartition.MAX_CANONICAL_BYTES
            + TargetChannelIdentity.MAX_CANONICAL_BYTES
            + TargetScheduleBinding.MAX_BODY_BYTES
            + 4096;
    private static final byte[] HASH_DOMAIN = Bytes.utf8("nereus-delay-target-ordinary-publication\0");

    public TargetOrdinaryPublicationBinding {
        Objects.requireNonNull(locator, "locator");
        Objects.requireNonNull(physical, "physical");
        Objects.requireNonNull(channel, "channel");
        Objects.requireNonNull(destinationProfile, "destinationProfile");
        Objects.requireNonNull(capabilityProfile, "capabilityProfile");
        Objects.requireNonNull(adapterMetadata, "adapterMetadata");
        Objects.requireNonNull(reservedMetadata, "reservedMetadata");
        publishAttemptId = TargetCompatibilityCodec.assigned(publishAttemptId, 32, "publishAttemptId");
        claimDigest = TargetCompatibilityCodec.assigned(claimDigest, 32, "claimDigest");
        claimedMessageDigest = TargetCompatibilityCodec.assigned(claimedMessageDigest, 32, "claimedMessageDigest");
        Bytes.requireLength(payloadSha256, 32, "payloadSha256");
        payloadSha256 = Bytes.copy(payloadSha256);
        artifactGenerationSetDigest =
                TargetCompatibilityCodec.assigned(artifactGenerationSetDigest, 32, "artifactGenerationSetDigest");
        final var context = channel.context();
        final var shard = locator.messageId().routingId().shardId();
        if (attemptNo <= 0
                || claimedMessageVersion == 0
                || payloadLength < 0
                || deliverAtEpochMs < 0
                || expireAtEpochMs <= deliverAtEpochMs
                || retryEligibilityAtEpochMs < 0
                || retryEligibilityAtEpochMs >= expireAtEpochMs
                || eventTimeEpochMs != null && eventTimeEpochMs < 0
                || destinationProfile.canonicalBytes().length > TargetChannelIdentity.MAX_PROFILE_REF_BYTES
                || capabilityProfile.canonicalBytes().length > TargetChannelIdentity.MAX_PROFILE_REF_BYTES
                || destinationProfile.profileKind() != ProfileKind.DESTINATION
                || capabilityProfile.profileKind() != ProfileKind.DELIVERY_CAPABILITY
                || adapterMetadata.canonicalBytes().length > TargetScheduleBinding.MAX_BODY_BYTES
                || (adapterMetadata.kind() == AdapterMetadata.Kind.KAFKA
                        ? adapterMetadata.kafka().headers().size()
                        : adapterMetadata.pulsar().properties().size()) > TargetScheduleBinding.MAX_METADATA_ENTRIES
                || !locator.target().equals(physical.id())
                || !shard.equals(context.sourceShard())
                || !locator.target().equals(context.target())
                || !locator.domain().equals(context.domain())
                || !Arrays.equals(locator.accountingIncarnation(), context.accountingIncarnation())
                || (adapterMetadata.kind() == AdapterMetadata.Kind.KAFKA)
                        != (physical.resource().kind() == BrokerResourceIdentity.Kind.KAFKA)
                || !reservedMetadata.routeIncarnation().equals(shard.routeIncarnation())
                || reservedMetadata.shardPartition() != Integer.toUnsignedLong(shard.partition())
                || !reservedMetadata.messageId().equals(locator.messageId())
                || reservedMetadata.generation() != Integer.toUnsignedLong(locator.generation())
                || !Arrays.equals(reservedMetadata.publishAttemptId(), publishAttemptId)
                || !Arrays.equals(reservedMetadata.destinationProfileSemanticHash(), destinationProfile.semanticHash())
                || !Arrays.equals(reservedMetadata.capabilityProfileSemanticHash(), capabilityProfile.semanticHash())
                || reservedMetadata.deliverAtEpochMs() != deliverAtEpochMs) {
            throw new IllegalArgumentException("Target ordinary publication changes its exact identity/record/window");
        }
    }

    /** Freezes actual claimed Message facts; activation and live channel/membership guards remain external. */
    public static TargetOrdinaryPublicationBinding fromClaim(
            final TargetClaimRecord claim,
            final TargetMessageRecord message,
            final TargetScheduleBinding binding,
            final CanonicalTargetPartition physical,
            final TargetChannelIdentity channel,
            final ProfileRef capabilityProfile,
            final byte[] artifactGenerationSetDigest) {
        claim.requireCurrent(message);
        binding.requireLocator(message.locator());
        binding.requireMessageSource(message.scheduleSource());
        if (claim.selected().nativeCandidate()
                || message.runtime().currentWorkKind() != CurrentSendWorkKind.CLAIMED
                || !Arrays.equals(binding.offeredDispatchRef(), channel.context().dispatchCompatibilityRef())
                || !Arrays.equals(binding.controlScopeRef(), channel.context().controlScopeRef())) {
            throw new IllegalArgumentException("ordinary publication requires its exact non-Native Claim/channel refs");
        }
        final byte[] attempt = SystemMutation.computePublishAttemptLogicalIdentity(
                claim.claimId(), message.locator().messageId(), Integer.toUnsignedLong(message.locator().generation()),
                Integer.toUnsignedLong(claim.work().candidateAttemptNo()));
        final var destination = binding.intent().profile();
        final var shard = message.locator().messageId().routingId().shardId();
        final var reserved = new ReservedPublishMetadata(
                shard.routeIncarnation(), Integer.toUnsignedLong(shard.partition()), message.locator().messageId(),
                Integer.toUnsignedLong(message.locator().generation()), attempt,
                destination.semanticHash(), capabilityProfile.semanticHash(), message.deliverAtEpochMs(),
                binding.intent().deliveryMode());
        return new TargetOrdinaryPublicationBinding(
                message.locator(), physical, channel, attempt, claim.work().candidateAttemptNo(),
                claim.digest(), message.digest(), message.stateVersion(), destination, capabilityProfile,
                message.payloadLength(), payloadHash(message), binding.intent().adapterMetadata(), reserved,
                message.deliverAtEpochMs(), message.expireAtEpochMs(), message.retryEligibilityAtEpochMs(),
                binding.intent().eventTimeEpochMs(), artifactGenerationSetDigest);
    }

    public void requireClaim(
            final TargetClaimRecord claim, final TargetMessageRecord message, final TargetScheduleBinding binding) {
        final var expected = fromClaim(
                claim, message, binding, physical, channel, capabilityProfile, artifactGenerationSetDigest);
        if (!Arrays.equals(preparedPublishHash(), expected.preparedPublishHash())) {
            throw new IllegalArgumentException(
                    "Target frozen publication differs from the actual Claim/Message/binding");
        }
    }

    public void requirePayload(final byte[] resolvedPayload) {
        Objects.requireNonNull(resolvedPayload, "resolvedPayload");
        if (resolvedPayload.length != payloadLength || !Arrays.equals(Bytes.sha256(resolvedPayload), payloadSha256)) {
            throw new IllegalArgumentException(
                    "Target resolved payload differs from its frozen publication commitment");
        }
    }

    private static byte[] payloadHash(final TargetMessageRecord message) {
        final byte[] inline = message.inlinePayload();
        return inline == null
                ? message.payloadReference().payloadSha256()
                : Bytes.sha256(inline);
    }

    @Override
    public byte[] publishAttemptId() { return Bytes.copy(publishAttemptId); }

    @Override
    public byte[] claimDigest() { return Bytes.copy(claimDigest); }

    @Override
    public byte[] claimedMessageDigest() { return Bytes.copy(claimedMessageDigest); }

    @Override
    public byte[] payloadSha256() { return Bytes.copy(payloadSha256); }

    @Override
    public byte[] artifactGenerationSetDigest() { return Bytes.copy(artifactGenerationSetDigest); }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, VERSION);
            CanonicalProtobuf.bytes(out, 2, locator.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, physical.canonicalBytes());
            CanonicalProtobuf.bytes(out, 4, channel.canonicalBytes());
            CanonicalProtobuf.bytes(out, 5, publishAttemptId);
            CanonicalProtobuf.uint32(out, 6, attemptNo);
            CanonicalProtobuf.bytes(out, 7, claimDigest);
            CanonicalProtobuf.bytes(out, 8, claimedMessageDigest);
            CanonicalProtobuf.uint64Bits(out, 9, claimedMessageVersion);
            CanonicalProtobuf.bytes(out, 10, destinationProfile.canonicalBytes());
            CanonicalProtobuf.bytes(out, 11, capabilityProfile.canonicalBytes());
            CanonicalProtobuf.uint64(out, 12, payloadLength);
            CanonicalProtobuf.bytes(out, 13, payloadSha256);
            CanonicalProtobuf.bytes(out, 14, adapterMetadata.canonicalBytes());
            CanonicalProtobuf.bytes(out, 15, reservedMetadata.canonicalBytes());
            CanonicalProtobuf.uint64(out, 16, deliverAtEpochMs);
            CanonicalProtobuf.uint64(out, 17, expireAtEpochMs);
            CanonicalProtobuf.uint64(out, 18, retryEligibilityAtEpochMs);
            if (eventTimeEpochMs != null) {
                CanonicalProtobuf.uint64(out, 20, eventTimeEpochMs);
            }
            CanonicalProtobuf.bytes(out, 21, artifactGenerationSetDigest);
        });
    }

    public byte[] preparedPublishHash() { return Bytes.sha256(HASH_DOMAIN, fields()); }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetOrdinaryPublicationBinding that
                && Arrays.equals(preparedPublishHash(), that.preparedPublishHash());
    }

    @Override
    public int hashCode() { return Arrays.hashCode(preparedPublishHash()); }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 22, preparedPublishHash());
        });
    }

    public static TargetOrdinaryPublicationBinding decode(final byte[] encoded) {
        final var f = TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 21, false, "Target publication");
        final boolean event = f.size() == 21;
        QueryCodecSupport.requireNumbers(f, event
                ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21, 22}
                : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 21, 22},
                "Target ordinary publication");
        if (QueryCodecSupport.uint32(f.get(0), 1) != VERSION) {
            throw new IllegalArgumentException("unsupported Target ordinary publication version");
        }
        final var result = new TargetOrdinaryPublicationBinding(
                TargetMessageLocator.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                CanonicalTargetPartition.decode(QueryCodecSupport.bytes(f.get(2), 3)),
                TargetChannelIdentity.decode(QueryCodecSupport.bytes(f.get(3), 4)),
                QueryCodecSupport.fixed(f.get(4), 5, 32), QueryCodecSupport.uint32Bits(f.get(5), 6),
                QueryCodecSupport.fixed(f.get(6), 7, 32), QueryCodecSupport.fixed(f.get(7), 8, 32),
                QueryCodecSupport.uint64Bits(f.get(8), 9),
                ProfileRef.decode(QueryCodecSupport.bytes(f.get(9), 10)),
                ProfileRef.decode(QueryCodecSupport.bytes(f.get(10), 11)),
                QueryCodecSupport.uint(f.get(11), 12), QueryCodecSupport.fixed(f.get(12), 13, 32),
                AdapterMetadata.decode(QueryCodecSupport.bytes(f.get(13), 14)),
                ReservedPublishMetadata.decode(QueryCodecSupport.bytes(f.get(14), 15)),
                QueryCodecSupport.uint(f.get(15), 16), QueryCodecSupport.uint(f.get(16), 17),
                QueryCodecSupport.uint(f.get(17), 18), event ? QueryCodecSupport.uint(f.get(18), 20) : null,
                QueryCodecSupport.fixed(f.get(event ? 19 : 18), 21, 32));
        if (!Arrays.equals(result.preparedPublishHash(), QueryCodecSupport.fixed(f.getLast(), 22, 32))) {
            throw new IllegalArgumentException("Target publication prepared hash mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "Target ordinary publication");
        return result;
    }
}
