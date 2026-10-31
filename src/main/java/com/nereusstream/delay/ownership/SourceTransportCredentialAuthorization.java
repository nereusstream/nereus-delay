package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.BrokerResourceIdentity;
import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.KafkaActivationBarrier;
import com.nereusstream.delay.protocol.KafkaBrokerResourceIdentity;
import com.nereusstream.delay.protocol.KafkaIngressRouteResource;
import com.nereusstream.delay.protocol.KafkaSourcePosition;
import com.nereusstream.delay.protocol.PulsarActivationBarrier;
import com.nereusstream.delay.protocol.PulsarBrokerResourceIdentity;
import com.nereusstream.delay.protocol.PulsarIngressRouteResource;
import com.nereusstream.delay.protocol.PulsarSourcePosition;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.RouteIncarnation;
import com.nereusstream.delay.protocol.RouteSnapshot;
import com.nereusstream.delay.protocol.ShardId;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TrustedUtcIntervalEvidence;
import com.nereusstream.delay.runtime.CredentialAttestationTrustSet;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.PublicKey;
import java.security.Signature;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Private signed deployment input. Neither a Source command nor Destination/Owner authority. */
public final class SourceTransportCredentialAuthorization {
    public enum Operation {
        CONSUME(1),
        ACK(2),
        SYSTEM_APPEND(4),
        HISTORY(8),
        OBSERVATION(16),
        RECOVERY(32),
        LOOKUP(64);
        private final int bit;

        Operation(int bit) {
            this.bit = bit;
        }
    }

    private static final byte[] DOMAIN = Bytes.utf8("nereus-delay-source-transport-credential\0");
    private static final byte[] SIGNATURE_DOMAIN = Bytes.utf8("nereus-delay-source-transport-credential-signature\0");
    public static final int MAX_BYTES = 16 * 1024;
    private final List<CanonicalProtobuf.Reader.Field> fields;
    private final byte[] encoded, core, digest, reference;
    private final BrokerResourceIdentity resource;
    private final ShardId shard;
    private final TrustedUtcIntervalEvidence verifiedAt;
    private final int operations;

    private SourceTransportCredentialAuthorization(byte[] bytes) {
        if (bytes.length == 0 || bytes.length > MAX_BYTES) {
            throw unavailable();
        }
        fields = QueryCodecSupport.read(bytes, "private Source credential authorization");
        final int[] numbers = new int[33];
        for (int i = 0; i < numbers.length; i++) {
            numbers[i] = i + 1;
        }
        QueryCodecSupport.requireNumbers(fields, numbers, "private Source credential authorization");
        if (number(1) != 1
                || number(4) == 0
                || number(13) == 0
                || number(30) != LocalBrokerMtlsResolver.VERIFIER_VERSION
                || number(31) == 0
                || !Arrays.equals(value(29), Bytes.utf8(LocalBrokerMtlsResolver.VERIFIER_ID))) {
            throw unavailable();
        }
        for (int n : new int[] {2, 3, 5, 6, 12, 15, 16, 17, 25, 28, 32}) {
            Bytes.requireLength(value(n), 32, "private Source digest");
        }
        nonzero(value(2));
        nonzero(value(3));
        nonzero(value(12));
        reference = value(14);
        if (reference.length > 128) {
            throw unavailable();
        }
        resource = BrokerResourceIdentity.decode(value(9));
        shard = new ShardId(
                new RouteIncarnation(QueryCodecSupport.fixed(fields.get(6), 7, RouteIncarnation.LENGTH)),
                QueryCodecSupport.uint32(fields.get(10), 11));
        if (!Arrays.equals(value(8), shardBytes(shard))) {
            throw unavailable();
        }
        text(10, 1024, false);
        operations = QueryCodecSupport.uint32(fields.get(17), 18);
        if (operations == 0 || (operations & ~127) != 0) {
            throw unavailable();
        }
        for (int n : new int[] {19, 20, 22, 23, 24}) {
            name(n, true);
        }
        final int mode = QueryCodecSupport.uint32(fields.get(20), 21);
        if (resource.kind() == BrokerResourceIdentity.Kind.KAFKA) {
            if (!text(20, 256, true).isEmpty() || mode != 0) {
                throw unavailable();
            }
            if (allows(Operation.ACK) && name(19, false).isEmpty()) {
                throw unavailable();
            }
            if (!allows(Operation.CONSUME)
                    && !allows(Operation.ACK)
                    && !name(19, true).isEmpty()) {
                throw unavailable();
            }
        } else {
            final boolean activeConsumer = allows(Operation.CONSUME) || allows(Operation.ACK);
            if (!text(19, 256, true).isEmpty()
                    || (activeConsumer
                            ? mode != 1 || name(20, false).isEmpty()
                            : mode != 0 || !name(20, true).isEmpty())) {
                throw unavailable(); // Active Source is Exclusive/Durable; readers only receive their named read role.
            }
        }
        if (allows(Operation.HISTORY)) {
            name(22, false);
        }
        if (allows(Operation.OBSERVATION)) {
            name(23, false);
        }
        if (allows(Operation.RECOVERY)) {
            name(24, false);
        }
        verifiedAt = TrustedUtcIntervalEvidence.decode(value(26));
        if (number(27) <= verifiedAt.latestEpochMs()) {
            throw unavailable();
        }
        core = canonical(27);
        digest = Bytes.sha256(DOMAIN, canonical(31));
        if (!Arrays.equals(value(32), digest) || value(33).length != 64 || !Arrays.equals(bytes, canonical(33))) {
            throw unavailable();
        }
        encoded = Bytes.copy(bytes);
    }

    /** Decoding alone grants nothing; the resolver verifies deployment trust, Route and actual private material. */
    public static SourceTransportCredentialAuthorization decode(byte[] bytes) {
        try {
            return new SourceTransportCredentialAuthorization(bytes);
        } catch (RuntimeException failure) {
            throw unavailable();
        }
    }

    private byte[] canonical(int through) {
        return CanonicalProtobuf.message(out -> {
            for (int n = 1; n <= through; n++) {
                if (Set.of(1, 11, 18, 21, 30, 31).contains(n)) {
                    CanonicalProtobuf.uint32Bits(out, n, QueryCodecSupport.uint32(fields.get(n - 1), n));
                } else if (n == 4 || n == 13) {
                    CanonicalProtobuf.uint64Bits(out, n, number(n));
                } else if (n == 27) {
                    CanonicalProtobuf.int64(out, n, number(n));
                } else {
                    CanonicalProtobuf.bytes(out, n, value(n));
                }
            }
        });
    }

    private long number(int field) {
        return QueryCodecSupport.uint64Bits(fields.get(field - 1), field);
    }

    private byte[] value(int field) {
        return QueryCodecSupport.bytes(fields.get(field - 1), field);
    }

    private String text(int field, int maximum, boolean empty) {
        final byte[] bytes = value(field);
        final String text = new String(bytes, StandardCharsets.UTF_8);
        if (bytes.length > maximum
                || !Arrays.equals(bytes, text.getBytes(StandardCharsets.UTF_8))
                || !Normalizer.isNormalized(text, Normalizer.Form.NFC)
                || text.indexOf('\0') >= 0
                || !text.equals(text.trim())
                || !empty && text.isBlank()) {
            throw unavailable();
        }
        return text;
    }

    private String name(int field, boolean empty) {
        final String name = text(field, 256, empty);
        if (!name.isEmpty() && !name.matches("[A-Za-z0-9._-]{1,256}")) {
            throw unavailable();
        }
        return name;
    }

    private static void nonzero(byte[] bytes) {
        for (byte value : bytes) {
            if (value != 0) {
                return;
            }
        }
        throw unavailable();
    }

    static byte[] shardBytes(ShardId shard) {
        return ByteBuffer.allocate(RouteIncarnation.LENGTH + 4)
                .put(shard.routeIncarnation().bytes())
                .putInt(shard.partition())
                .array();
    }

    public ShardId shard() {
        return shard;
    }

    public byte[] deploymentId() {
        return Bytes.copy(value(2));
    }

    public BrokerResourceIdentity resource() {
        return resource;
    }

    public String physicalTopic() {
        return text(10, 1024, false);
    }

    public long credentialGeneration() {
        return number(13);
    }

    public byte[] bindingId() {
        return Bytes.copy(value(12));
    }

    public byte[] reference() {
        return Bytes.copy(reference);
    }

    public byte[] fingerprint() {
        return Bytes.copy(value(16));
    }

    public byte[] referenceDigest() {
        return Bytes.copy(value(15));
    }

    public byte[] evidenceDigest() {
        return Bytes.copy(value(28));
    }

    public byte[] policyDigest() {
        return Bytes.copy(value(25));
    }

    public byte[] scopeImageDigest() {
        return Bytes.sha256(core);
    }

    public byte[] identityDigest() {
        return Bytes.sha256(
                DOMAIN,
                value(2),
                value(3),
                Bytes.u64beBits(number(4)),
                value(8),
                value(12),
                Bytes.u64beBits(credentialGeneration()));
    }

    public long notBeforeEpochMs() {
        return verifiedAt.earliestEpochMs();
    }

    public long notAfterEpochMs() {
        return number(27);
    }

    public boolean allows(Operation operation) {
        return (operations & operation.bit) != 0;
    }

    public void requireOperation(Operation operation) {
        if (!allows(operation)) {
            throw unavailable();
        }
    }

    public void requireTime(TrustedUtcIntervalEvidence time) {
        if (time.earliestEpochMs() < verifiedAt.latestEpochMs() || time.latestEpochMs() >= notAfterEpochMs()) {
            throw unavailable();
        }
    }

    public void requireName(Operation operation, String name) {
        requireOperation(operation);
        final int field = operation == Operation.HISTORY
                ? 22
                : operation == Operation.OBSERVATION
                        ? 23
                        : operation == Operation.RECOVERY
                                ? 24
                                : resource.kind() == BrokerResourceIdentity.Kind.KAFKA ? 19 : 20;
        final String expected = name(field, false);
        if (field >= 22
                ? !name.startsWith(expected + ".") || !name.matches("[A-Za-z0-9._-]{1,256}")
                : !expected.equals(name)) {
            throw unavailable();
        }
    }

    public String newReaderName(Operation operation) {
        final int field = operation == Operation.HISTORY
                ? 22
                : operation == Operation.OBSERVATION ? 23 : operation == Operation.RECOVERY ? 24 : 0;
        if (field == 0) {
            throw unavailable();
        }
        requireOperation(operation);
        final String prefix = name(field, false);
        if (prefix.length() > 219) {
            throw unavailable();
        }
        return prefix + '.' + java.util.UUID.randomUUID();
    }

    public void requirePosition(SourcePosition position, String topic) {
        if (!shard.equals(position.shardId()) || !physicalTopic().equals(topic)) {
            throw unavailable();
        }
        if (position instanceof KafkaSourcePosition kafka) {
            if (resource.kind() != BrokerResourceIdentity.Kind.KAFKA
                    || !resource.kafka().authenticatedClusterId().equals(kafka.authenticatedClusterId())
                    || !resource.kafka().nativeTopicUuid().equals(kafka.nativeTopicUuid())) {
                throw unavailable();
            }
        } else if (!(position instanceof PulsarSourcePosition pulsar)
                || resource.kind() != BrokerResourceIdentity.Kind.PULSAR
                || !physicalTopic().equals(pulsar.physicalTopic())
                || !Arrays.equals(resource.pulsar().resourceIncarnation(), pulsar.brokerResourceIncarnation())) {
            throw unavailable();
        }
    }

    public void requireAssignment(SourceAssignment assignment) {
        if (!shard.equals(assignment.shardId())) {
            throw unavailable();
        }
        final var barrier = assignment.activationBarrier();
        if (barrier instanceof KafkaActivationBarrier kafka) {
            if (resource.kind() != BrokerResourceIdentity.Kind.KAFKA
                    || !resource.kafka().authenticatedClusterId().equals(kafka.authenticatedClusterId())
                    || !resource.kafka().nativeTopicUuid().equals(kafka.nativeTopicUuid())) {
                throw unavailable();
            }
        } else if (!(barrier instanceof PulsarActivationBarrier pulsar)
                || resource.kind() != BrokerResourceIdentity.Kind.PULSAR
                || !physicalTopic().equals(pulsar.physicalTopic())
                || !Arrays.equals(resource.pulsar().resourceIncarnation(), pulsar.brokerResourceIncarnation())) {
            throw unavailable();
        }
    }

    private void requireRoute(RouteSnapshot route, byte[] signedRoute) {
        if (!Arrays.equals(value(5), Bytes.sha256(signedRoute))
                || !Arrays.equals(value(6), route.snapshotDigest())
                || !route.routeIncarnation().equals(shard.routeIncarnation())) {
            throw unavailable();
        }
        route.partitionPolicy(shard.partition());
        final String topic;
        final BrokerResourceIdentity original;
        if (route.ingress() instanceof KafkaIngressRouteResource kafka) {
            topic = kafka.canonicalPhysicalTopic();
            original = BrokerResourceIdentity.kafka(
                    new KafkaBrokerResourceIdentity(kafka.authenticatedClusterId(), kafka.nativeTopicUuid()));
        } else {
            final var physical = ((PulsarIngressRouteResource) route.ingress()).partition(shard.partition());
            topic = physical.physicalTopic();
            original = BrokerResourceIdentity.pulsar(new PulsarBrokerResourceIdentity(
                    route.ingress().authenticatedClusterId(),
                    physical.resourceIncarnation(),
                    topic,
                    physical.physicalTopicCreationTimestamp()));
        }
        if (!physicalTopic().equals(topic) || !resource.equals(original)) {
            throw unavailable();
        }
    }

    public byte[] canonicalBytes() {
        return Bytes.copy(encoded);
    }

    @Override
    public String toString() {
        return "PrivateSourceTransportAuthorization";
    }

    private static IllegalStateException unavailable() {
        return new IllegalStateException("Source transport credential unavailable");
    }

    /** Explicit Source-use permission over existing verifier keys; invalidation only closes this deployment generation. */
    public static final class Trust {
        private final byte[] deployment, configuration;
        private final long generation;
        private final PublicKey routeKey;
        private final CredentialAttestationTrustSet keys;
        private final Set<Integer> sourceKeyVersions;
        private volatile boolean active = true;

        public Trust(
                byte[] deployment,
                byte[] configuration,
                long generation,
                PublicKey originalRouteKey,
                CredentialAttestationTrustSet keys,
                Set<Integer> sourceKeyVersions) {
            Bytes.requireLength(deployment, 32, "deployment identity");
            nonzero(deployment);
            Bytes.requireLength(configuration, 32, "Source deployment configuration identity");
            nonzero(configuration);
            if (generation == 0
                    || sourceKeyVersions.isEmpty()
                    || sourceKeyVersions.size() > 1024
                    || sourceKeyVersions.contains(0)) {
                throw unavailable();
            }
            this.deployment = Bytes.copy(deployment);
            this.configuration = Bytes.copy(configuration);
            this.generation = generation;
            routeKey = Objects.requireNonNull(originalRouteKey);
            this.keys = Objects.requireNonNull(keys);
            this.sourceKeyVersions = Set.copyOf(sourceKeyVersions);
        }

        public void invalidate() {
            active = false;
        }

        public void requireActive() {
            if (!active) {
                throw unavailable();
            }
        }

        void verify(SourceTransportCredentialAuthorization image, byte[] signedRoute, TrustedUtcIntervalEvidence time)
                throws Exception {
            requireActive();
            image.requireTime(time);
            final int keyVersion = QueryCodecSupport.uint32(image.fields.get(30), 31);
            if (!Arrays.equals(deployment, image.value(2))
                    || !Arrays.equals(configuration, image.value(3))
                    || generation != image.number(4)
                    || !sourceKeyVersions.contains(keyVersion)) {
                throw unavailable();
            }
            final var key = keys.keys().stream()
                    .filter(candidate -> candidate.verifierVersion() == image.number(30)
                            && candidate.signingKeyVersion() == keyVersion
                            && Arrays.equals(candidate.verifierId(), image.value(29)))
                    .findFirst()
                    .orElseThrow(SourceTransportCredentialAuthorization::unavailable);
            if (image.verifiedAt.earliestEpochMs() < key.verifyNotBeforeEpochMs()
                    || image.notAfterEpochMs() > key.verifyNotAfterEpochMs()) {
                throw unavailable();
            }
            final var signature = Signature.getInstance("Ed25519");
            signature.initVerify(key.publicKey());
            signature.update(Bytes.sha256(SIGNATURE_DOMAIN, image.digest, Bytes.u32beBits(key.signingKeyVersion())));
            if (!signature.verify(image.value(33))) {
                throw unavailable();
            }
            image.requireRoute(RouteSnapshot.decode(signedRoute, routeKey), signedRoute);
            requireActive();
        }
    }
}
