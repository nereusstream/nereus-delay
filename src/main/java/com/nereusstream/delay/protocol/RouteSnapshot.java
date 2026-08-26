package com.nereusstream.delay.protocol;

import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Signed immutable Ingress Route snapshot. The canonical bytes and signature
 * are the Registry §6.6 authority; this type deliberately contains no
 * credential material or mutable endpoint lookup.
 */
public final class RouteSnapshot {
    public static final int VERSION = 1;
    private static final int HASH_LENGTH = 32;
    private static final int SIGNATURE_LENGTH = 64;
    private static final String DIGEST_DOMAIN = "nereus-delay-ingress-route-snapshot\0";
    private static final String SIGNATURE_DOMAIN = "nereus-delay-ingress-route-snapshot-signature\0";

    private final RouteIncarnation routeIncarnation;
    private final byte[] authenticatedTenantScopeHash;
    private final byte[] tenantRoutingScope;
    private final RouteLifecycle lifecycle;
    private final long newScheduleAcceptUntilEpochMs;
    private final IngressRouteResource ingress;
    private final RoutingHashVersion routingHashVersion;
    private final ProtocolTuple protocolTuple;
    private final long controlVersion;
    private final List<RoutePartitionPolicy> partitions;
    private final long queuedReceiptQueryWindowMs;
    private final long fullCommandResultRetentionMs;
    private final long maxInlinePayloadBytes;
    private final long maxCommandBytes;
    private final int maxBatchCommands;
    private final long maxBatchBytes;
    private final long maximumPreparationAgeMs;
    private final long validFromEpochMs;
    private final long validUntilEpochMs;
    private final IngressCredentialBindingRef credentialBinding;
    private final byte[] routePrerequisiteDigest;
    private final TrustedUtcIntervalEvidence issuedAt;
    private final long signingKeyVersion;
    private final byte[] snapshotDigest;
    private final byte[] signature;

    private RouteSnapshot(
            final RouteIncarnation routeIncarnation,
            final byte[] authenticatedTenantScopeHash,
            final byte[] tenantRoutingScope,
            final RouteLifecycle lifecycle,
            final long newScheduleAcceptUntilEpochMs,
            final IngressRouteResource ingress,
            final RoutingHashVersion routingHashVersion,
            final ProtocolTuple protocolTuple,
            final long controlVersion,
            final List<RoutePartitionPolicy> partitions,
            final long queuedReceiptQueryWindowMs,
            final long fullCommandResultRetentionMs,
            final long maxInlinePayloadBytes,
            final long maxCommandBytes,
            final int maxBatchCommands,
            final long maxBatchBytes,
            final long maximumPreparationAgeMs,
            final long validFromEpochMs,
            final long validUntilEpochMs,
            final IngressCredentialBindingRef credentialBinding,
            final byte[] routePrerequisiteDigest,
            final TrustedUtcIntervalEvidence issuedAt,
            final long signingKeyVersion,
            final byte[] snapshotDigest,
            final byte[] signature,
            final boolean validateDigest) {
        this.routeIncarnation = Objects.requireNonNull(routeIncarnation, "routeIncarnation");
        this.authenticatedTenantScopeHash = nonZero(authenticatedTenantScopeHash, "authenticatedTenantScopeHash");
        this.tenantRoutingScope = nonZero(tenantRoutingScope, "tenantRoutingScope");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle");
        this.newScheduleAcceptUntilEpochMs =
                nonNegative(newScheduleAcceptUntilEpochMs, "newScheduleAcceptUntilEpochMs");
        this.ingress = Objects.requireNonNull(ingress, "ingress");
        this.routingHashVersion = Objects.requireNonNull(routingHashVersion, "routingHashVersion");
        this.protocolTuple = Objects.requireNonNull(protocolTuple, "protocolTuple");
        this.controlVersion = nonZero(controlVersion, "controlVersion");
        this.partitions = validatePartitions(partitions, ingress);
        this.queuedReceiptQueryWindowMs = positive(queuedReceiptQueryWindowMs, "queuedReceiptQueryWindowMs");
        this.fullCommandResultRetentionMs = positive(fullCommandResultRetentionMs, "fullCommandResultRetentionMs");
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

    public static RouteSnapshot create(
            final RouteIncarnation routeIncarnation,
            final byte[] authenticatedTenantScopeHash,
            final byte[] tenantRoutingScope,
            final RouteLifecycle lifecycle,
            final long newScheduleAcceptUntilEpochMs,
            final IngressRouteResource ingress,
            final RoutingHashVersion routingHashVersion,
            final ProtocolTuple protocolTuple,
            final long controlVersion,
            final List<RoutePartitionPolicy> partitions,
            final long queuedReceiptQueryWindowMs,
            final long fullCommandResultRetentionMs,
            final long maxInlinePayloadBytes,
            final long maxCommandBytes,
            final int maxBatchCommands,
            final long maxBatchBytes,
            final long maximumPreparationAgeMs,
            final long validFromEpochMs,
            final long validUntilEpochMs,
            final IngressCredentialBindingRef credentialBinding,
            final byte[] routePrerequisiteDigest,
            final TrustedUtcIntervalEvidence issuedAt,
            final long signingKeyVersion,
            final PrivateKey signingKey) {
        Objects.requireNonNull(signingKey, "signingKey");
        final RouteSnapshot unsigned = new RouteSnapshot(
                routeIncarnation,
                authenticatedTenantScopeHash,
                tenantRoutingScope,
                lifecycle,
                newScheduleAcceptUntilEpochMs,
                ingress,
                routingHashVersion,
                protocolTuple,
                controlVersion,
                partitions,
                queuedReceiptQueryWindowMs,
                fullCommandResultRetentionMs,
                maxInlinePayloadBytes,
                maxCommandBytes,
                maxBatchCommands,
                maxBatchBytes,
                maximumPreparationAgeMs,
                validFromEpochMs,
                validUntilEpochMs,
                credentialBinding,
                routePrerequisiteDigest,
                issuedAt,
                signingKeyVersion,
                null,
                null,
                false);
        final byte[] signature = sign(unsigned.signaturePreimage(), signingKey);
        return new RouteSnapshot(
                routeIncarnation,
                authenticatedTenantScopeHash,
                tenantRoutingScope,
                lifecycle,
                newScheduleAcceptUntilEpochMs,
                ingress,
                routingHashVersion,
                protocolTuple,
                controlVersion,
                partitions,
                queuedReceiptQueryWindowMs,
                fullCommandResultRetentionMs,
                maxInlinePayloadBytes,
                maxCommandBytes,
                maxBatchCommands,
                maxBatchBytes,
                maximumPreparationAgeMs,
                validFromEpochMs,
                validUntilEpochMs,
                credentialBinding,
                routePrerequisiteDigest,
                issuedAt,
                signingKeyVersion,
                unsigned.snapshotDigest,
                signature,
                true);
    }

    public static RouteSnapshot decode(final byte[] encoded, final PublicKey verificationKey) {
        Objects.requireNonNull(verificationKey, "verificationKey");
        final List<CanonicalProtobuf.Reader.Field> fields = QueryCodecSupport.read(encoded, "RouteSnapshot", true);
        if (fields.size() < 26) {
            throw new IllegalArgumentException("RouteSnapshot fields are incomplete");
        }
        for (int index = 0; index < 10; index++) {
            if (fields.get(index).number() != index + 1) {
                throw new IllegalArgumentException("RouteSnapshot field order mismatch before partitions");
            }
        }
        int index = 10;
        final List<RoutePartitionPolicy> partitions = new ArrayList<>();
        while (index < fields.size() && fields.get(index).number() == 11) {
            partitions.add(RoutePartitionPolicy.decode(QueryCodecSupport.nested(fields.get(index), 11)));
            index++;
        }
        if (index + 15 != fields.size()) {
            throw new IllegalArgumentException("RouteSnapshot field count is invalid");
        }
        for (int fieldNumber = 12; fieldNumber <= 26; fieldNumber++) {
            if (fields.get(index + fieldNumber - 12).number() != fieldNumber) {
                throw new IllegalArgumentException("RouteSnapshot field order mismatch at " + fieldNumber);
            }
        }
        final RouteSnapshot result = new RouteSnapshot(
                new RouteIncarnation(QueryCodecSupport.fixed(fields.get(1), 2, 16)),
                QueryCodecSupport.fixed(fields.get(2), 3, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(3), 4, HASH_LENGTH),
                RouteLifecycle.fromWire(QueryCodecSupport.uint(fields.get(4), 5)),
                signed(QueryCodecSupport.uint(fields.get(5), 6), 6),
                IngressRouteResource.decode(QueryCodecSupport.nested(fields.get(6), 7)),
                RoutingHashVersion.fromWire(QueryCodecSupport.uint(fields.get(7), 8)),
                ProtocolTuple.decode(QueryCodecSupport.nested(fields.get(8), 9)),
                QueryCodecSupport.uint64Bits(fields.get(9), 10),
                partitions,
                positive(QueryCodecSupport.uint(fields.get(index), 12), "queuedReceiptQueryWindowMs"),
                positive(QueryCodecSupport.uint(fields.get(index + 1), 13), "fullCommandResultRetentionMs"),
                positive(QueryCodecSupport.uint(fields.get(index + 2), 14), "maxInlinePayloadBytes"),
                positive(QueryCodecSupport.uint(fields.get(index + 3), 15), "maxCommandBytes"),
                QueryCodecSupport.uint32(fields.get(index + 4), 16),
                positive(QueryCodecSupport.uint(fields.get(index + 5), 17), "maxBatchBytes"),
                positive(QueryCodecSupport.uint(fields.get(index + 6), 18), "maximumPreparationAgeMs"),
                signed(QueryCodecSupport.uint(fields.get(index + 7), 19), 19),
                signed(QueryCodecSupport.uint(fields.get(index + 8), 20), 20),
                IngressCredentialBindingRef.decode(QueryCodecSupport.nested(fields.get(index + 9), 21)),
                QueryCodecSupport.fixed(fields.get(index + 10), 22, HASH_LENGTH),
                TrustedUtcIntervalEvidence.decode(QueryCodecSupport.nested(fields.get(index + 11), 23)),
                QueryCodecSupport.uint(fields.get(index + 12), 24),
                QueryCodecSupport.fixed(fields.get(index + 13), 25, HASH_LENGTH),
                QueryCodecSupport.fixed(fields.get(index + 14), 26, SIGNATURE_LENGTH),
                true);
        if (!verify(result.signaturePreimage(), result.signature, verificationKey)) {
            throw new IllegalArgumentException("Route snapshot signature verification failed");
        }
        QueryCodecSupport.requireCanonical(encoded, result.canonicalBytes(), "RouteSnapshot");
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

    public RouteLifecycle lifecycle() {
        return lifecycle;
    }

    public long newScheduleAcceptUntilEpochMs() {
        return newScheduleAcceptUntilEpochMs;
    }

    public IngressRouteResource ingress() {
        return ingress;
    }

    public RoutingHashVersion routingHashVersion() {
        return routingHashVersion;
    }

    public ProtocolTuple protocolTuple() {
        return protocolTuple;
    }

    public long controlVersion() {
        return controlVersion;
    }

    public List<RoutePartitionPolicy> partitions() {
        return partitions;
    }

    public RoutePartitionPolicy partitionPolicy(final int partition) {
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

    public IngressCredentialBindingRef credentialBinding() {
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
    public void requireUsableForNewSchedule(
            final byte[] tenantScopeHash, final byte[] routingScope, final long trustedNowEpochMs) {
        requireTenantScope(tenantScopeHash, routingScope);
        if (lifecycle != RouteLifecycle.ACTIVE_FOR_NEW
                || trustedNowEpochMs < validFromEpochMs
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
        return other instanceof RouteSnapshot that
                && routeIncarnation.equals(that.routeIncarnation)
                && Arrays.equals(authenticatedTenantScopeHash, that.authenticatedTenantScopeHash)
                && Arrays.equals(tenantRoutingScope, that.tenantRoutingScope)
                && lifecycle == that.lifecycle
                && newScheduleAcceptUntilEpochMs == that.newScheduleAcceptUntilEpochMs
                && ingress.equals(that.ingress)
                && routingHashVersion == that.routingHashVersion
                && protocolTuple.equals(that.protocolTuple)
                && controlVersion == that.controlVersion
                && partitions.equals(that.partitions)
                && queuedReceiptQueryWindowMs == that.queuedReceiptQueryWindowMs
                && fullCommandResultRetentionMs == that.fullCommandResultRetentionMs
                && maxInlinePayloadBytes == that.maxInlinePayloadBytes
                && maxCommandBytes == that.maxCommandBytes
                && maxBatchCommands == that.maxBatchCommands
                && maxBatchBytes == that.maxBatchBytes
                && maximumPreparationAgeMs == that.maximumPreparationAgeMs
                && validFromEpochMs == that.validFromEpochMs
                && validUntilEpochMs == that.validUntilEpochMs
                && credentialBinding.equals(that.credentialBinding)
                && Arrays.equals(routePrerequisiteDigest, that.routePrerequisiteDigest)
                && issuedAt.equals(that.issuedAt)
                && signingKeyVersion == that.signingKeyVersion
                && Arrays.equals(snapshotDigest, that.snapshotDigest)
                && Arrays.equals(signature, that.signature);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                routeIncarnation,
                Arrays.hashCode(authenticatedTenantScopeHash),
                Arrays.hashCode(tenantRoutingScope),
                lifecycle,
                newScheduleAcceptUntilEpochMs,
                ingress,
                routingHashVersion,
                protocolTuple,
                controlVersion,
                partitions,
                queuedReceiptQueryWindowMs,
                fullCommandResultRetentionMs,
                maxInlinePayloadBytes,
                maxCommandBytes,
                maxBatchCommands,
                maxBatchBytes,
                maximumPreparationAgeMs,
                validFromEpochMs,
                validUntilEpochMs,
                credentialBinding,
                Arrays.hashCode(routePrerequisiteDigest),
                issuedAt,
                signingKeyVersion,
                Arrays.hashCode(snapshotDigest),
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
        for (RoutePartitionPolicy partition : partitions) {
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
        return Bytes.sha256(Bytes.utf8(SIGNATURE_DOMAIN), snapshotDigest, Bytes.u32be(signingKeyVersion));
    }

    private static List<RoutePartitionPolicy> validatePartitions(
            final List<RoutePartitionPolicy> values, final IngressRouteResource ingress) {
        Objects.requireNonNull(values, "partitions");
        if (values.size() != ingress.partitionCount()) {
            throw new IllegalArgumentException("Route partition policy set is incomplete");
        }
        final List<RoutePartitionPolicy> copy = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            final RoutePartitionPolicy value = Objects.requireNonNull(values.get(index), "partition policy");
            if (value.partition() != index
                    || value.activationBarrier().partition() != index
                    || !value.activationBarrier().resource().equals(resourceIdentity(ingress, index))) {
                throw new IllegalArgumentException("Route partition policy resource/partition mismatch");
            }
            copy.add(value);
        }
        return List.copyOf(copy);
    }

    private static BrokerResourceIdentity resourceIdentity(final IngressRouteResource ingress, final int partition) {
        if (ingress instanceof KafkaIngressRouteResource kafka) {
            return BrokerResourceIdentity.kafka(
                    new KafkaBrokerResourceIdentity(kafka.authenticatedClusterId(), kafka.nativeTopicUuid()));
        }
        final PulsarPhysicalPartitionIdentity physical = ((PulsarIngressRouteResource) ingress).partition(partition);
        return BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                ingress.authenticatedClusterId(),
                physical.resourceIncarnation(),
                physical.physicalTopic(),
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
