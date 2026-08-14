package io.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Signed immutable Ingress Route snapshot.  The canonical bytes and signature
 * are the Registry §6.6 authority; this type deliberately contains no
 * credential material or mutable endpoint lookup.
 */
public final class RouteSnapshotV1 {
    public static final int VERSION = 1;
    private static final int HASH_LENGTH = 32;
    private static final int SIGNATURE_LENGTH = 64;
    private static final String DIGEST_DOMAIN = "nereus-delay-ingress-route-snapshot-v1\0";
    private static final String SIGNATURE_DOMAIN = "nereus-delay-ingress-route-snapshot-signature-v1\0";

    private final RouteIncarnation routeIncarnation;
    private final byte[] authenticatedTenantScopeHash;
    private final byte[] tenantRoutingScope;
    private final RouteLifecycleV1 lifecycle;
    private final long newScheduleAcceptUntilEpochMs;
    private final IngressRouteResourceV1 ingress;
    private final RoutingHashVersionV1 routingHashVersion;
    private final ProtocolTupleV1 protocolTuple;
    private final long controlVersion;
    private final List<RoutePartitionPolicyV1> partitions;
    private final long queuedReceiptQueryWindowMs;
    private final long fullCommandResultRetentionMs;
    private final long maxInlinePayloadBytes;
    private final long maxCommandBytes;
    private final int maxBatchCommands;
    private final long maxBatchBytes;
    private final long maximumPreparationAgeMs;
    private final long validFromEpochMs;
    private final long validUntilEpochMs;
    private final IngressCredentialBindingRefV1 credentialBinding;
    private final byte[] routePrerequisiteDigest;
    private final TrustedUtcIntervalEvidence issuedAt;
    private final long signingKeyVersion;
    private final byte[] snapshotDigest;
    private final byte[] signature;

    private RouteSnapshotV1(final RouteIncarnation routeIncarnation, final byte[] authenticatedTenantScopeHash,
                            final byte[] tenantRoutingScope, final RouteLifecycleV1 lifecycle,
                            final long newScheduleAcceptUntilEpochMs, final IngressRouteResourceV1 ingress,
                            final RoutingHashVersionV1 routingHashVersion, final ProtocolTupleV1 protocolTuple,
                            final long controlVersion, final List<RoutePartitionPolicyV1> partitions,
                            final long queuedReceiptQueryWindowMs, final long fullCommandResultRetentionMs,
                            final long maxInlinePayloadBytes, final long maxCommandBytes, final int maxBatchCommands,
                            final long maxBatchBytes, final long maximumPreparationAgeMs,
                            final long validFromEpochMs, final long validUntilEpochMs,
                            final IngressCredentialBindingRefV1 credentialBinding,
                            final byte[] routePrerequisiteDigest, final TrustedUtcIntervalEvidence issuedAt,
                            final long signingKeyVersion, final byte[] snapshotDigest, final byte[] signature,
                            final boolean validateDigest) {
        this.routeIncarnation = Objects.requireNonNull(routeIncarnation, "routeIncarnation");
        this.authenticatedTenantScopeHash = nonZero(authenticatedTenantScopeHash,
                "authenticatedTenantScopeHash");
        this.tenantRoutingScope = nonZero(tenantRoutingScope, "tenantRoutingScope");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        this.newScheduleAcceptUntilEpochMs = nonNegative(newScheduleAcceptUntilEpochMs,
                "newScheduleAcceptUntilEpochMs");
        this.ingress = Objects.requireNonNull(ingress, "ingress");
        this.routingHashVersion = Objects.requireNonNull(routingHashVersion, "routingHashVersion");
        this.protocolTuple = Objects.requireNonNull(protocolTuple, "protocolTuple");
        this.controlVersion = nonZero(controlVersion, "controlVersion");
        this.partitions = validatePartitions(partitions, ingress);
        this.queuedReceiptQueryWindowMs = positive(queuedReceiptQueryWindowMs,
                "queuedReceiptQueryWindowMs");
        this.fullCommandResultRetentionMs = positive(fullCommandResultRetentionMs,
                "fullCommandResultRetentionMs");
        this.maxInlinePayloadBytes = positive(maxInlinePayloadBytes, "maxInlinePayloadBytes");
        this.maxCommandBytes = positive(maxCommandBytes, "maxCommandBytes");
        if (maxCommandBytes < maxInlinePayloadBytes) {
            throw new IllegalArgumentException("maxCommandBytes must cover maxInlinePayloadBytes");
        }
        if (maxBatchCommands <= 0) {
            throw new IllegalArgumentException("maxBatchCommands must be positive");
        }
        this.maxBatchCommands = maxBatchCommands;
        this.maxBatchBytes = positive(maxBatchBytes, "maxBatchBytes");
        if (maxBatchBytes < maxCommandBytes) {
            throw new IllegalArgumentException("maxBatchBytes must cover maxCommandBytes");
        }
        this.maximumPreparationAgeMs = positive(maximumPreparationAgeMs, "maximumPreparationAgeMs");
        this.validFromEpochMs = nonNegative(validFromEpochMs, "validFromEpochMs");
        this.validUntilEpochMs = nonNegative(validUntilEpochMs, "validUntilEpochMs");
        if (validUntilEpochMs <= validFromEpochMs
                || newScheduleAcceptUntilEpochMs > validUntilEpochMs
                || issuedAt == null
                || issuedAt.earliestEpochMs() < validFromEpochMs
                || issuedAt.latestEpochMs() >= validUntilEpochMs) {
            throw new IllegalArgumentException("Route validity interval does not contain issuedAt");
        }
        this.credentialBinding = Objects.requireNonNull(credentialBinding, "credentialBinding");
        this.routePrerequisiteDigest = nonZero(routePrerequisiteDigest, "routePrerequisiteDigest");
        this.issuedAt = issuedAt;
        if (signingKeyVersion <= 0 || signingKeyVersion > 0xffff_ffffL) {
            throw new IllegalArgumentException("signingKeyVersion must be positive");
        }
        this.signingKeyVersion = signingKeyVersion;
        final byte[] calculatedDigest = computeDigest();
        if (snapshotDigest == null) {
            this.snapshotDigest = calculatedDigest;
        } else {
            Bytes.requireLength(snapshotDigest, HASH_LENGTH, "snapshotDigest");
            if (validateDigest && !Bytes.constantTimeEquals(calculatedDigest, snapshotDigest)) {
                throw new IllegalArgumentException("Route snapshot digest mismatch");
            }
            this.snapshotDigest = Bytes.copy(snapshotDigest);
        }
        if (signature == null) {
            this.signature = new byte[0];
        } else {
            Bytes.requireLength(signature, SIGNATURE_LENGTH, "signature");
            this.signature = Bytes.copy(signature);
        }
    }

    public static RouteSnapshotV1 create(final RouteIncarnation routeIncarnation,
                                         final byte[] authenticatedTenantScopeHash,
                                         final byte[] tenantRoutingScope, final RouteLifecycleV1 lifecycle,
                                         final long newScheduleAcceptUntilEpochMs,
                                         final IngressRouteResourceV1 ingress,
                                         final RoutingHashVersionV1 routingHashVersion,
                                         final ProtocolTupleV1 protocolTuple, final long controlVersion,
                                         final List<RoutePartitionPolicyV1> partitions,
                                         final long queuedReceiptQueryWindowMs,
                                         final long fullCommandResultRetentionMs, final long maxInlinePayloadBytes,
                                         final long maxCommandBytes, final int maxBatchCommands,
                                         final long maxBatchBytes, final long maximumPreparationAgeMs,
                                         final long validFromEpochMs, final long validUntilEpochMs,
                                         final IngressCredentialBindingRefV1 credentialBinding,
                                         final byte[] routePrerequisiteDigest,
                                         final TrustedUtcIntervalEvidence issuedAt, final long signingKeyVersion,
                                         final PrivateKey signingKey) {
        Objects.requireNonNull(signingKey, "signingKey");
        final RouteSnapshotV1 unsigned = new RouteSnapshotV1(routeIncarnation, authenticatedTenantScopeHash,
                tenantRoutingScope, lifecycle, newScheduleAcceptUntilEpochMs, ingress, routingHashVersion,
                protocolTuple, controlVersion, partitions, queuedReceiptQueryWindowMs,
                fullCommandResultRetentionMs, maxInlinePayloadBytes, maxCommandBytes, maxBatchCommands,
                maxBatchBytes, maximumPreparationAgeMs, validFromEpochMs, validUntilEpochMs, credentialBinding,
                routePrerequisiteDigest, issuedAt, signingKeyVersion, null, null, false);
        final byte[] signature = sign(unsigned.signaturePreimage(), signingKey);
        return new RouteSnapshotV1(routeIncarnation, authenticatedTenantScopeHash, tenantRoutingScope, lifecycle,
                newScheduleAcceptUntilEpochMs, ingress, routingHashVersion, protocolTuple, controlVersion,
                partitions, queuedReceiptQueryWindowMs, fullCommandResultRetentionMs, maxInlinePayloadBytes,
                maxCommandBytes, maxBatchCommands, maxBatchBytes, maximumPreparationAgeMs, validFromEpochMs,
                validUntilEpochMs, credentialBinding, routePrerequisiteDigest, issuedAt, signingKeyVersion,
                unsigned.snapshotDigest, signature, true);
    }

    public static RouteSnapshotV1 decode(final byte[] encoded, final PublicKey verificationKey) {
        Objects.requireNonNull(verificationKey, "verificationKey");
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "RouteSnapshotV1", true);
        if (fields.size() < 26) {
            throw new IllegalArgumentException("RouteSnapshotV1 fields are incomplete");
        }
        for (int index = 0; index < 10; index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("RouteSnapshotV1 field order mismatch before partitions");
            }
        }
        int index = 10;
        final List<RoutePartitionPolicyV1> partitions = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 11) {
            partitions.add(RoutePartitionPolicyV1.decode(QueryCodecSupport.nested(fields.get(index), 11)));
            index++;
        }
        if (index + 15 != fields.size()) {
            throw new IllegalArgumentException("RouteSnapshotV1 field count is invalid");
        }
        for (int fieldNumber = 12; fieldNumber <= 26; fieldNumber++) {
            if (fields.get(index + fieldNumber - 12).number() != fieldNumber) {
                throw new IllegalArgumentException("RouteSnapshotV1 field order mismatch at " + fieldNumber);
            }
        }
        final RouteSnapshotV1 result = new RouteSnapshotV1(
                new RouteIncarnation(QueryCodecSupport.fixed(fields.get(1), 2, 16)),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH),
                RouteLifecycleV1.fromWire(QueryCodecSupport.uint(fields.get(4), 5)),
                signed(QueryCodecSupport.uint(fields.get(5), 6), 6),
                IngressRouteResourceV1.decode(QueryCodecSupport.nested(fields.get(6), 7)),
                RoutingHashVersionV1.fromWire(QueryCodecSupport.uint(fields.get(7), 8)),
                ProtocolTupleV1.decode(QueryCodecSupport.nested(fields.get(8), 9)),
                QueryCodecSupport.uint64Bits(fields.get(9), 10), partitions,
                positive(QueryCodecSupport.uint(fields.get(index), 12), "queuedReceiptQueryWindowMs"),
                positive(QueryCodecSupport.uint(fields.get(index + 1), 13), "fullCommandResultRetentionMs"),
                positive(QueryCodecSupport.uint(fields.get(index + 2), 14), "maxInlinePayloadBytes"),
                positive(QueryCodecSupport.uint(fields.get(index + 3), 15), "maxCommandBytes"),
                QueryCodecSupport.uint32(fields.get(index + 4), 16),
                positive(QueryCodecSupport.uint(fields.get(index + 5), 17), "maxBatchBytes"),
                positive(QueryCodecSupport.uint(fields.get(index + 6), 18), "maximumPreparationAgeMs"),
                signed(QueryCodecSupport.uint(fields.get(index + 7), 19), 19),
                signed(QueryCodecSupport.uint(fields.get(index + 8), 20), 20),
                IngressCredentialBindingRefV1.decode(QueryCodecSupport.nested(fields.get(index + 9), 21)),
                QueryCodecSupport.fixed(fields.get(index + 10), 22, HASH_LENGTH),
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(fields.get(index + 11), 23)),
                QueryCodecSupport.uint(fields.get(index + 12), 24),
                QueryCodecSupport.fixed(fields.get(index + 13), 25, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(index + 14), 26, SIGNATURE_LENGTH), true);
        if (!verify(result.signaturePreimage(), result.signature, verificationKey)) {
            throw new IllegalArgumentException("Route snapshot signature verification failed");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RouteSnapshotV1");
        return result;
    }

    public RouteIncarnation routeIncarnation() {
        return routeIncarnation;
    }

    public int version() {
        return VERSION;
    }

    public byte[] authenticatedTenantScopeHash() {
        return Bytes.copy(authenticatedTenantScopeHash);
    }

    public byte[] tenantRoutingScope() {
        return Bytes.copy(tenantRoutingScope);
    }

    public RouteLifecycleV1 lifecycle() {
        return lifecycle;
    }

    public long newScheduleAcceptUntilEpochMs() {
        return newScheduleAcceptUntilEpochMs;
    }

    public IngressRouteResourceV1 ingress() {
        return ingress;
    }

    public RoutingHashVersionV1 routingHashVersion() {
        return routingHashVersion;
    }

    public ProtocolTupleV1 protocolTuple() {
        return protocolTuple;
    }

    public long controlVersion() {
        return controlVersion;
    }

    public List<RoutePartitionPolicyV1> partitions() {
        return partitions;
    }

    public RoutePartitionPolicyV1 partitionPolicy(final int partition) {
        if (partition < 0 || partition >= partitions.size()) {
            throw new IllegalArgumentException("partition outside Route snapshot");
        }
        return partitions.get(partition);
    }

    public long queuedReceiptQueryWindowMs() {
        return queuedReceiptQueryWindowMs;
    }

    public long fullCommandResultRetentionMs() {
        return fullCommandResultRetentionMs;
    }

    public long maxInlinePayloadBytes() {
        return maxInlinePayloadBytes;
    }

    public long maxCommandBytes() {
        return maxCommandBytes;
    }

    public int maxBatchCommands() {
        return maxBatchCommands;
    }

    public long maxBatchBytes() {
        return maxBatchBytes;
    }

    public long maximumPreparationAgeMs() {
        return maximumPreparationAgeMs;
    }

    public long validFromEpochMs() {
        return validFromEpochMs;
    }

    public long validUntilEpochMs() {
        return validUntilEpochMs;
    }

    public IngressCredentialBindingRefV1 credentialBinding() {
        return credentialBinding;
    }

    public byte[] routePrerequisiteDigest() {
        return Bytes.copy(routePrerequisiteDigest);
    }

    public TrustedUtcIntervalEvidence issuedAt() {
        return issuedAt;
    }

    public long signingKeyVersion() {
        return signingKeyVersion;
    }

    public byte[] snapshotDigest() {
        return Bytes.copy(snapshotDigest);
    }

    public byte[] signature() {
        return Bytes.copy(signature);
    }

    /** Checks the local, zero-I/O admission predicates for a new Schedule. */
    public void requireUsableForNewSchedule(final byte[] tenantScopeHash, final byte[] routingScope,
                                            final long trustedNowEpochMs) {
        requireTenantScope(tenantScopeHash, routingScope);
        if (lifecycle != RouteLifecycleV1.ACTIVE_FOR_NEW || trustedNowEpochMs < validFromEpochMs
                || trustedNowEpochMs > validUntilEpochMs
                || trustedNowEpochMs > newScheduleAcceptUntilEpochMs) {
            throw new IllegalArgumentException("Route snapshot is not active for new schedules");
        }
    }

    /** Checks only tenant authorization for historical control/query routing. */
    public void requireTenantScope(final byte[] tenantScopeHash, final byte[] routingScope) {
        if (!Bytes.constantTimeEquals(authenticatedTenantScopeHash, tenantScopeHash)
                || !Bytes.constantTimeEquals(tenantRoutingScope, routingScope)) {
            throw new IllegalArgumentException("Route snapshot tenant scope mismatch");
        }
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(output -> {
            writeFields1To24(output);
            CanonicalProtobuf.bytes(output, 25, snapshotDigest);
            CanonicalProtobuf.bytes(output, 26, signature);
        });
    }

    public byte[] canonicalBytesWithoutIntegrity() {
        return CanonicalProtobuf.message(this::writeFields1To24);
    }

    @Override
    public boolean equals(final Object other) {
        return other instanceof RouteSnapshotV1 that
                && routeIncarnation.equals(that.routeIncarnation)
                && Arrays.equals(authenticatedTenantScopeHash, that.authenticatedTenantScopeHash)
                && Arrays.equals(tenantRoutingScope, that.tenantRoutingScope)
                && lifecycle == that.lifecycle
                && newScheduleAcceptUntilEpochMs == that.newScheduleAcceptUntilEpochMs
                && ingress.equals(that.ingress) && routingHashVersion == that.routingHashVersion
                && protocolTuple.equals(that.protocolTuple) && controlVersion == that.controlVersion
                && partitions.equals(that.partitions)
                && queuedReceiptQueryWindowMs == that.queuedReceiptQueryWindowMs
                && fullCommandResultRetentionMs == that.fullCommandResultRetentionMs
                && maxInlinePayloadBytes == that.maxInlinePayloadBytes && maxCommandBytes == that.maxCommandBytes
                && maxBatchCommands == that.maxBatchCommands && maxBatchBytes == that.maxBatchBytes
                && maximumPreparationAgeMs == that.maximumPreparationAgeMs
                && validFromEpochMs == that.validFromEpochMs && validUntilEpochMs == that.validUntilEpochMs
                && credentialBinding.equals(that.credentialBinding)
                && Arrays.equals(routePrerequisiteDigest, that.routePrerequisiteDigest)
                && issuedAt.equals(that.issuedAt) && signingKeyVersion == that.signingKeyVersion
                && Arrays.equals(snapshotDigest, that.snapshotDigest) && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(routeIncarnation, Arrays.hashCode(authenticatedTenantScopeHash),
                Arrays.hashCode(tenantRoutingScope), lifecycle, newScheduleAcceptUntilEpochMs, ingress,
                routingHashVersion, protocolTuple, controlVersion, partitions, queuedReceiptQueryWindowMs,
                fullCommandResultRetentionMs, maxInlinePayloadBytes, maxCommandBytes, maxBatchCommands,
                maxBatchBytes, maximumPreparationAgeMs, validFromEpochMs, validUntilEpochMs, credentialBinding,
                Arrays.hashCode(routePrerequisiteDigest), issuedAt, signingKeyVersion, Arrays.hashCode(snapshotDigest),
                Arrays.hashCode(signature));
    }

    private void writeFields1To24(final java.io.ByteArrayOutputStream output) {
        CanonicalProtobuf.uint32(output, 1, VERSION);
        CanonicalProtobuf.bytes(output, 2, routeIncarnation.bytes());
        CanonicalProtobuf.bytes(output, 3, authenticatedTenantScopeHash);
        CanonicalProtobuf.bytes(output, 4, tenantRoutingScope);
        CanonicalProtobuf.uint32(output, 5, lifecycle.wireValue());
        CanonicalProtobuf.int64(output, 6, newScheduleAcceptUntilEpochMs);
        CanonicalProtobuf.bytes(output, 7, ingress.canonicalBytes());
        CanonicalProtobuf.uint32(output, 8, routingHashVersion.wireValue());
        CanonicalProtobuf.bytes(output, 9, protocolTuple.canonicalBytes());
        CanonicalProtobuf.uint64Bits(output, 10, controlVersion);
        for (RoutePartitionPolicyV1 partition : partitions) {
            CanonicalProtobuf.bytes(output, 11, partition.canonicalBytes());
        }
        CanonicalProtobuf.uint64(output, 12, queuedReceiptQueryWindowMs);
        CanonicalProtobuf.uint64(output, 13, fullCommandResultRetentionMs);
        CanonicalProtobuf.uint64(output, 14, maxInlinePayloadBytes);
        CanonicalProtobuf.uint64(output, 15, maxCommandBytes);
        CanonicalProtobuf.uint32(output, 16, maxBatchCommands);
        CanonicalProtobuf.uint64(output, 17, maxBatchBytes);
        CanonicalProtobuf.uint64(output, 18, maximumPreparationAgeMs);
        CanonicalProtobuf.int64(output, 19, validFromEpochMs);
        CanonicalProtobuf.int64(output, 20, validUntilEpochMs);
        CanonicalProtobuf.bytes(output, 21, credentialBinding.canonicalBytes());
        CanonicalProtobuf.bytes(output, 22, routePrerequisiteDigest);
        CanonicalProtobuf.bytes(output, 23, issuedAt.canonicalBytes());
        CanonicalProtobuf.uint32(output, 24, signingKeyVersion);
    }

    private byte[] computeDigest() {
        return Bytes.sha256(Bytes.utf8(DIGEST_DOMAIN), canonicalBytesWithoutIntegrity());
    }

    private byte[] signaturePreimage() {
        return Bytes.sha256(Bytes.utf8(SIGNATURE_DOMAIN), snapshotDigest,
                Bytes.u32be(signingKeyVersion));
    }

    private static List<RoutePartitionPolicyV1> validatePartitions(
            final List<RoutePartitionPolicyV1> values, final IngressRouteResourceV1 ingress) {
        Objects.requireNonNull(values, "partitions");
        if (values.size() != ingress.partitionCount()) {
            throw new IllegalArgumentException("Route partition policy set is incomplete");
        }
        final List<RoutePartitionPolicyV1> copy = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            final RoutePartitionPolicyV1 value = Objects.requireNonNull(values.get(index), "partition policy");
            if (value.partition() != index || value.activationBarrier().partition() != index
                    || !value.activationBarrier().resource().equals(resourceIdentity(ingress, index))) {
                throw new IllegalArgumentException("Route partition policy resource/partition mismatch");
            }
            copy.add(value);
        }
        return List.copyOf(copy);
    }

    private static BrokerResourceIdentityV1 resourceIdentity(final IngressRouteResourceV1 ingress,
                                                              final int partition) {
        if (ingress instanceof KafkaIngressRouteResourceV1 kafka) {
            return BrokerResourceIdentityV1.kafka(new KafkaBrokerResourceIdentityV1(
                    kafka.authenticatedClusterId(), kafka.nativeTopicUuid()));
        }
        final PulsarPhysicalPartitionIdentityV1 physical = ((PulsarIngressRouteResourceV1) ingress)
                .partition(partition);
        return BrokerResourceIdentityV1.pulsar(new PulsarBrokerResourceIdentityV1(
                ingress.authenticatedClusterId(), physical.resourceIncarnation(), physical.physicalTopic(),
                physical.physicalTopicCreationTimestamp()));
    }

    private static byte[] sign(final byte[] preimage, final PrivateKey key) {
        try {
            final Signature signer = Signature.getInstance("Ed25519");
            signer.initSign(key);
            signer.update(preimage);
            return signer.sign();
        } catch (GeneralSecurityException exception) {
            throw new IllegalStateException("Ed25519 signing is unavailable", exception);
        }
    }

    private static boolean verify(final byte[] preimage, final byte[] signature, final PublicKey key) {
        try {
            final Signature verifier = Signature.getInstance("Ed25519");
            verifier.initVerify(key);
            verifier.update(preimage);
            return verifier.verify(signature);
        } catch (GeneralSecurityException exception) {
            throw new IllegalArgumentException("Ed25519 verification is unavailable", exception);
        }
    }

    private static byte[] nonZero(final byte[] value, final String name) {
        Bytes.requireLength(value, HASH_LENGTH, name);
        for (byte item : value) {
            if (item != 0) {
                return Bytes.copy(value);
            }
        }
        throw new IllegalArgumentException(name + " must be non-zero");
    }

    private static long nonNegative(final long value, final String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be non-negative");
        }
        return value;
    }

    private static long positive(final long value, final String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static long nonZero(final long value, final String name) {
        if (value == 0) {
            throw new IllegalArgumentException(name + " must be non-zero");
        }
        return value;
    }

    private static long signed(final long value, final int field) {
        if (value < 0) {
            throw new IllegalArgumentException("Route snapshot field " + field + " must be non-negative");
        }
        return value;
    }
}
