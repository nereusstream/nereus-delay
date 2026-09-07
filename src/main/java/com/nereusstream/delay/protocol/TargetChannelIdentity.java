package com.nereusstream.delay.protocol;

import com.nereusstream.delay.store.TargetKeyCodec;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/** Immutable Target channel generation. Source authority, fencing and transport ownership are separate gates. */
public final class TargetChannelIdentity {
    public static final int VERSION = 1;
    public static final int VALUE_TYPE = 20;
    public static final int MAX_PROFILE_ID_BYTES = 256;
    public static final int MAX_TIME_SOURCE_ID_BYTES = 256;
    public static final int MAX_PROFILE_REF_BYTES = 259 + 11 + 34 + 2;
    public static final int MAX_TIME_EVIDENCE_BYTES = 11 + 11 + 2 + 259 + 11 + 11 + 11 + 34 + 6 + 66;
    public static final int MAX_LEASE_BYTES =
            2 + 3 + MAX_PROFILE_REF_BYTES + 2 + 34 + 11 + 34 + 34 + 3 + MAX_TIME_EVIDENCE_BYTES + 11 + 11 + 34;
    public static final int MAX_CONTEXT_BYTES = 2 + 22 + 34 + 4 + 11 + 18 + 34 + 34 + 2 + 6 + 11 + 76 + 34 + 11 + 34;
    public static final int MAX_CANONICAL_BYTES = MAX_CONTEXT_BYTES + 4 + MAX_LEASE_BYTES + 35;
    private static final byte[] DIGEST_DOMAIN = Bytes.utf8("nereus-delay-target-channel-identity\0");
    private static final byte[] HOLDER_DOMAIN = Bytes.utf8("nereus-delay-credential-holder-target-channel\0");
    private static final byte[] PRODUCER_DOMAIN = Bytes.utf8("nereus-delay-target-producer\0");

    /** Stable producer scope plus a particular credential/resource-attestation generation. */
    public record Context(
            ShardId sourceShard,
            TargetPartitionId target,
            TargetKeyCodec.Domain domain,
            byte[] accountingIncarnation,
            byte[] dispatchCompatibilityRef,
            byte[] controlScopeRef,
            ChannelKind kind,
            long channelSlot,
            long channelGeneration,
            Long evidenceGeneration,
            byte[] resourceGuardAttestationDigest) {
        public Context {
            Objects.requireNonNull(sourceShard, "sourceShard");
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(domain, "domain");
            Objects.requireNonNull(kind, "kind");
            if (domain.slot() >= TargetQueueState.MAX_DOMAIN_SLOTS
                    || channelSlot < 0
                    || channelSlot > 0xffff_ffffL
                    || channelGeneration == 0) {
                throw new IllegalArgumentException("Target channel slot/generation is outside bounds");
            }
            if (kind != ChannelKind.BASELINE_PRODUCER
                    && kind != ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT
                    && kind != ChannelKind.PULSAR_DEDUP_PRODUCER) {
                throw new IllegalArgumentException("Target managed channel kind is not supported");
            }
            if (kind.requiresEvidenceResource() != (evidenceGeneration != null)
                    || (evidenceGeneration != null && evidenceGeneration == 0)) {
                throw new IllegalArgumentException("Target channel evidence generation presence mismatch");
            }
            accountingIncarnation =
                    TargetCompatibilityCodec.assigned(accountingIncarnation, 16, "accountingIncarnation");
            dispatchCompatibilityRef = TargetCompatibilityCodec.assigned(dispatchCompatibilityRef, 32, "dispatchRef");
            controlScopeRef = TargetCompatibilityCodec.assigned(controlScopeRef, 32, "controlScopeRef");
            resourceGuardAttestationDigest =
                    TargetCompatibilityCodec.assigned(resourceGuardAttestationDigest, 32, "resourceGuardDigest");
        }

        @Override
        public byte[] accountingIncarnation() {
            return Bytes.copy(accountingIncarnation);
        }

        @Override
        public byte[] dispatchCompatibilityRef() {
            return Bytes.copy(dispatchCompatibilityRef);
        }

        @Override
        public byte[] controlScopeRef() {
            return Bytes.copy(controlScopeRef);
        }

        @Override
        public byte[] resourceGuardAttestationDigest() {
            return Bytes.copy(resourceGuardAttestationDigest);
        }

        /** Renewal keeps this producer/transactional identity and its durable sequence domain. */
        public byte[] producerIdentity() {
            final byte[] scope = CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.bytes(out, 1, shardBytes(sourceShard));
                CanonicalProtobuf.bytes(out, 2, target.bytes());
                CanonicalProtobuf.uint32(out, 3, domain.slot());
                CanonicalProtobuf.uint64Bits(out, 4, domain.generation());
                CanonicalProtobuf.bytes(out, 5, accountingIncarnation);
                CanonicalProtobuf.uint32(out, 6, kind.wireValue());
                CanonicalProtobuf.uint32(out, 7, channelSlot);
                CanonicalProtobuf.bytes(out, 8, dispatchCompatibilityRef);
                CanonicalProtobuf.bytes(out, 9, controlScopeRef);
            });
            return Bytes.utf8("nd-target-" + HexFormat.of().formatHex(Bytes.sha256(PRODUCER_DOMAIN, scope)));
        }

        public byte[] canonicalBytes() {
            return CanonicalProtobuf.message(out -> {
                CanonicalProtobuf.uint32(out, 1, VERSION);
                CanonicalProtobuf.bytes(out, 2, shardBytes(sourceShard));
                CanonicalProtobuf.bytes(out, 3, target.bytes());
                CanonicalProtobuf.uint32(out, 4, domain.slot());
                CanonicalProtobuf.uint64Bits(out, 5, domain.generation());
                CanonicalProtobuf.bytes(out, 6, accountingIncarnation);
                CanonicalProtobuf.bytes(out, 7, dispatchCompatibilityRef);
                CanonicalProtobuf.bytes(out, 8, controlScopeRef);
                CanonicalProtobuf.uint32(out, 9, kind.wireValue());
                CanonicalProtobuf.uint32(out, 10, channelSlot);
                CanonicalProtobuf.uint64Bits(out, 11, channelGeneration);
                final byte[] producer = producerIdentity();
                CanonicalProtobuf.bytes(out, 12, producer);
                CanonicalProtobuf.bytes(out, 13, Bytes.sha256(producer));
                if (evidenceGeneration != null) {
                    CanonicalProtobuf.uint64Bits(out, 14, evidenceGeneration);
                }
                CanonicalProtobuf.bytes(out, 15, resourceGuardAttestationDigest);
            });
        }

        public byte[] credentialHolderScope() {
            return Bytes.sha256(HOLDER_DOMAIN, canonicalBytes());
        }

        private boolean sameProducerDomain(final Context prior) {
            return sourceShard.equals(prior.sourceShard)
                    && target.equals(prior.target)
                    && domain.equals(prior.domain)
                    && Arrays.equals(accountingIncarnation, prior.accountingIncarnation)
                    && Arrays.equals(dispatchCompatibilityRef, prior.dispatchCompatibilityRef)
                    && Arrays.equals(controlScopeRef, prior.controlScopeRef)
                    && kind == prior.kind
                    && channelSlot == prior.channelSlot
                    && Objects.equals(evidenceGeneration, prior.evidenceGeneration);
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Context that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(canonicalBytes());
        }
    }

    private final Context context;
    private final CredentialUseLease credentialLease;
    private final byte[] digest;

    public TargetChannelIdentity(final Context context, final CredentialUseLease credentialLease) {
        this.context = Objects.requireNonNull(context, "context");
        this.credentialLease = Objects.requireNonNull(credentialLease, "credentialLease");
        if (credentialLease.kind() != CredentialUseKind.DESTINATION_CHANNEL
                || credentialLease.profile().profileId().length > MAX_PROFILE_ID_BYTES
                || credentialLease.issuedAt().sourceId().length > MAX_TIME_SOURCE_ID_BYTES
                || !Arrays.equals(context.credentialHolderScope(), credentialLease.holderScopeDigest())) {
            throw new IllegalArgumentException("Target channel credential lease holder/kind/bounds mismatch");
        }
        TargetCompatibilityCodec.assigned(credentialLease.profile().semanticHash(), 32, "credentialProfileHash");
        TargetCompatibilityCodec.assigned(credentialLease.credentialBindingDigest(), 32, "credentialBindingDigest");
        TargetCompatibilityCodec.assigned(
                credentialLease.resolvedCredentialFingerprintDigest(), 32, "credentialFingerprint");
        digest = Bytes.sha256(DIGEST_DOMAIN, fields());
    }

    public Context context() {
        return context;
    }

    public CredentialUseLease credentialLease() {
        return credentialLease;
    }

    public byte[] digest() {
        return Bytes.copy(digest);
    }

    public byte[] encodedKey() {
        return TargetKeyCodec.channelIdentity(digest);
    }

    private byte[] fields() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(context.canonicalBytes());
            CanonicalProtobuf.bytes(out, 16, credentialLease.canonicalBytes());
        });
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            out.writeBytes(fields());
            CanonicalProtobuf.bytes(out, 17, digest);
        });
    }

    public static TargetChannelIdentity decode(final byte[] encoded) {
        final var fields =
                TargetCompatibilityCodec.read(encoded, MAX_CANONICAL_BYTES, 17, false, "TargetChannelIdentity");
        final boolean evidence = fields.size() == 17;
        QueryCodecSupport.requireNumbers(
                fields,
                evidence
                        ? new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}
                        : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17},
                "TargetChannelIdentity");
        if (QueryCodecSupport.uint32(fields.getFirst(), 1) != VERSION) {
            throw new IllegalArgumentException("unknown Target channel schema");
        }
        final byte[] shard = QueryCodecSupport.fixed(fields.get(1), 2, 20);
        final var context = new Context(
                new ShardId(new RouteIncarnation(Arrays.copyOf(shard, 16)), (int) Bytes.readU32be(shard, 16)),
                new TargetPartitionId(QueryCodecSupport.fixed(fields.get(2), 3, 32)),
                new TargetKeyCodec.Domain(
                        QueryCodecSupport.uint32(fields.get(3), 4), QueryCodecSupport.uint64Bits(fields.get(4), 5)),
                QueryCodecSupport.fixed(fields.get(5), 6, 16),
                QueryCodecSupport.fixed(fields.get(6), 7, 32),
                QueryCodecSupport.fixed(fields.get(7), 8, 32),
                ChannelKind.fromWire(QueryCodecSupport.uint(fields.get(8), 9)),
                QueryCodecSupport.uint(fields.get(9), 10),
                QueryCodecSupport.uint64Bits(fields.get(10), 11),
                evidence ? QueryCodecSupport.uint64Bits(fields.get(13), 14) : null,
                QueryCodecSupport.fixed(fields.get(evidence ? 14 : 13), 15, 32));
        final byte[] leaseBytes = QueryCodecSupport.bytes(fields.get(evidence ? 15 : 14), 16);
        final var leaseFields =
                TargetCompatibilityCodec.read(leaseBytes, MAX_LEASE_BYTES, 11, false, "Target channel lease");
        QueryCodecSupport.requireNumbers(
                leaseFields, new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, "Target channel lease");
        TargetCompatibilityCodec.read(
                QueryCodecSupport.bytes(leaseFields.get(1), 2),
                MAX_PROFILE_REF_BYTES,
                4,
                false,
                "Target credential ProfileRef");
        TargetCompatibilityCodec.read(
                QueryCodecSupport.bytes(leaseFields.get(7), 8),
                MAX_TIME_EVIDENCE_BYTES,
                10,
                false,
                "Target credential time evidence");
        final var result = new TargetChannelIdentity(context, CredentialUseLease.decode(leaseBytes));
        if (!Bytes.constantTimeEquals(result.digest, QueryCodecSupport.fixed(fields.getLast(), 17, 32))) {
            throw new IllegalArgumentException("Target channel digest mismatch");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "TargetChannelIdentity");
        return result;
    }

    /**
     * Resolves immutable refs and the credential provider.
     * Live membership, protection and resource guards still apply.
     */
    public void requireProjection(
            final CanonicalTargetPartition physical,
            final TargetDispatchCompatibility dispatch,
            final TargetControlScope controls,
            final ProfileSemanticEnvelope credentialProvider) {
        dispatch.requireTargetProjection(physical);
        if (!physical.id().equals(context.target)
                || !Arrays.equals(dispatch.digest(), context.dispatchCompatibilityRef)
                || !Arrays.equals(controls.digest(), context.controlScopeRef)
                || !controls.target().equals(context.target)
                || !controls.sourceShard().equals(context.sourceShard)
                || !credentialProvider.ref().equals(credentialLease.profile())
                || !(credentialProvider.body() instanceof DestinationProfileSemantic dest)
                || !dest.targetResource().equals(physical.resource())
                || !Arrays.equals(dest.credentialAuthorizationScopeDigest(), dispatch.authorizationScope())) {
            throw new IllegalArgumentException(
                    "Target channel resource/control/credential provider projection mismatch");
        }
        final ChannelKind requiredKind =
                switch (dispatch.capability().outcomeCapability()) {
                    case AT_LEAST_ONCE -> ChannelKind.BASELINE_PRODUCER;
                    case KAFKA_TRANSACTIONAL_RECEIPT -> ChannelKind.KAFKA_TRANSACTIONAL_RECEIPT;
                    case PULSAR_BROKER_DEDUP -> ChannelKind.PULSAR_DEDUP_PRODUCER;
                };
        if (context.kind != requiredKind) {
            throw new IllegalArgumentException("Target channel cannot replace its declared outcome capability");
        }
    }

    public static TargetChannelIdentity decodeReferenced(final byte[] reference, final byte[] encoded) {
        final var result = decode(encoded);
        if (!Bytes.constantTimeEquals(reference, result.digest)) {
            throw new IllegalArgumentException("Target frozen channel reference mismatch");
        }
        return result;
    }

    public static TargetChannelIdentity decodeForStore(
            final byte[] key, final byte[] encoded, final ShardId expectedSourceShard) {
        final var result = decode(encoded);
        if (!Arrays.equals(key, result.encodedKey()) || !result.context.sourceShard.equals(expectedSourceShard)) {
            throw new IllegalArgumentException("Target channel Store key/Shard mismatch");
        }
        return result;
    }

    public void requireQueueProjection(final TargetQueueState queue) {
        if (!context.target.equals(queue.targetId())
                || !Arrays.equals(context.accountingIncarnation, queue.accountingIncarnation())
                || context.domain.slot() >= queue.domains().size()) {
            throw new IllegalArgumentException("Target channel queue/accounting/slot mismatch");
        }
        final var bound = queue.domains().get(context.domain.slot());
        if (!context.domain.equals(bound.domain())
                || bound.lifecycle() == TargetDomainState.Lifecycle.VACANT
                || !Arrays.equals(context.dispatchCompatibilityRef, bound.dispatchCompatibilityRef())
                || !Arrays.equals(context.controlScopeRef, bound.controlScopeRef())) {
            throw new IllegalArgumentException("Target channel queue domain/ref mismatch");
        }
    }

    /** Static snapshot checks; C1/C2 must still bind live Owner, limits and physical admission to the same decision. */
    public void requireNewWorkProjection(final TargetQueueState queue, final int activatedChannelSlots) {
        requireQueueProjection(queue);
        if (queue.admissionState() != TargetQueueState.AdmissionState.OPEN
                || queue.domains().get(context.domain.slot()).lifecycle() != TargetDomainState.Lifecycle.ACTIVE
                || activatedChannelSlots <= 0
                || context.channelSlot >= activatedChannelSlots) {
            throw new IllegalArgumentException("Target channel is not eligible for new work in this snapshot");
        }
    }

    /** Renewal cannot change producer/sequence scope and never rewrites a retained attempt's exact identity. */
    public void requireSuccessorOf(final TargetChannelIdentity prior) {
        if (!context.sameProducerDomain(prior.context)
                || context.channelGeneration != TargetQueueState.nextRevision(prior.context.channelGeneration)) {
            throw new IllegalArgumentException("Target channel renewal changed domain or skipped/wrapped generation");
        }
    }

    public void requireExactFrozenIdentity(final TargetChannelIdentity frozen) {
        if (!equals(frozen)) {
            throw new IllegalArgumentException("Target attempt channel identity changed after Admission");
        }
    }

    private static byte[] shardBytes(final ShardId shard) {
        return Bytes.concat(shard.routeIncarnation().bytes(), Bytes.u32beBits(shard.partition()));
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof TargetChannelIdentity that && Arrays.equals(canonicalBytes(), that.canonicalBytes());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(digest);
    }
}
