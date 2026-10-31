package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.ControlAuthorizationContext;
import com.nereusstream.delay.protocol.ControlOperationAuthorization;
import com.nereusstream.delay.protocol.ControlRole;
import com.nereusstream.delay.protocol.ControlRoleSet;
import com.nereusstream.delay.protocol.KafkaReceiptSlotResource;
import com.nereusstream.delay.protocol.PreparedControlOperation;
import com.nereusstream.delay.protocol.SourcePosition;
import com.nereusstream.delay.protocol.TargetChannelControlRequest;
import com.nereusstream.delay.protocol.TargetKafkaReceiptBinding;
import com.nereusstream.delay.protocol.TargetKafkaReceiptControlArtifact;
import com.nereusstream.delay.protocol.TargetKafkaReceiptManagementImage;
import com.nereusstream.delay.protocol.TargetKafkaReceiptProtection;
import com.nereusstream.delay.protocol.TargetSourcePosition;
import com.nereusstream.delay.runtime.TargetChannelControlVerifier;
import com.nereusstream.delay.runtime.TargetKafkaReceiptProtectionTrust;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Controlled immutable original CONTROL inputs supplied by the trusted process root, never a client role claim. */
public final class TargetReceiptControlHistory
        implements TargetChannelControlVerifier.KeyAuthority,
                TargetKafkaReceiptProtectionTrust.HistoricalActor,
                TargetKafkaReceiptProtectionTrust.ReceiptScope {
    private record Entry(
            TargetKafkaReceiptControlArtifact original,
            TargetKafkaReceiptManagementImage management,
            ControlAuthorizationContext actor,
            KafkaReceiptSlotResource resource,
            int slots,
            int partition) {}

    private final Map<String, Entry> entries;
    private final Map<String, PublicKey> keys;
    private final long retainedBytes;

    private TargetReceiptControlHistory(Map<String, Entry> entries, Map<String, PublicKey> keys, long bytes) {
        this.entries = Map.copyOf(entries);
        this.keys = Map.copyOf(keys);
        retainedBytes = bytes;
    }

    /**
     * Captures only the small root-owned manifest before reservation; actual history files load on original owned I/O.
     */
    public static final class Inputs {
        private final List<Path> entries;
        private final long maximumBytes;
        private final long pathBytes;

        private Inputs(List<Path> entries, long maximumBytes, long pathBytes) {
            this.entries = List.copyOf(entries);
            this.maximumBytes = maximumBytes;
            this.pathBytes = pathBytes;
        }

        public long maximumRetainedBytes() {
            return maximumBytes;
        }
    }

    public static Inputs prepare(Path path) throws IOException {
        final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
        final int count = f.integer("history.entries");
        final long maximumBytes = f.number("history.maximumInputBytes");
        if (count < 1 || count > 510 || maximumBytes < 1 || maximumBytes > 16L * 1024 * 1024) {
            throw new IllegalArgumentException("receipt CONTROL history requires finite original input bounds");
        }
        final var paths = new ArrayList<Path>();
        long bytes = 16;
        for (int i = 0; i < count; i++) {
            final var input = f.absolutePath("history." + i + ".inputFile");
            paths.add(input);
            bytes = Math.addExact(bytes, Bytes.utf8(input.toString()).length);
        }
        if (bytes > maximumBytes) {
            throw new IllegalArgumentException("original CONTROL manifest exceeds its retained input bound");
        }
        f.requireComplete();
        return new Inputs(paths, maximumBytes, bytes);
    }

    /** The manifest/key/actor/scope files are root-owned original authorization data, not caller-selected grants. */
    public static TargetReceiptControlHistory load(Inputs inputs, Runnable guard) throws IOException {
        Objects.requireNonNull(inputs);
        Objects.requireNonNull(guard);
        final var entries = new HashMap<String, Entry>();
        final var keys = new HashMap<String, PublicKey>();
        long bytes = inputs.pathBytes;
        for (var path : inputs.entries) {
            guard.run();
            final var f = TargetWorkerRuntimeConfigLoader.loadFields(path);
            final var original = TargetKafkaReceiptControlArtifact.decode(f.image("authorizationFile", 16384));
            if (original.kind() != TargetKafkaReceiptControlArtifact.MANAGEMENT
                    || !Arrays.equals(original.digest(), f.hex("authorizationDigest", 32, 32))) {
                throw new IllegalArgumentException(
                        "receipt history changes its original pinned management authorization");
            }
            final var management = TargetKafkaReceiptManagementImage.decode(f.image("managementImageFile", 8192));
            if (!Arrays.equals(original.image(), management.canonicalBytes())) {
                throw new IllegalArgumentException(
                        "receipt management is outside the independently configured root scope");
            }
            final byte[] encodedKey = f.image("publicKeyFile", 1024);
            final PublicKey key;
            try {
                key = KeyFactory.getInstance("Ed25519").generatePublic(new X509EncodedKeySpec(encodedKey));
            } catch (GeneralSecurityException failure) {
                throw new IllegalArgumentException("invalid original CONTROL public key", failure);
            }
            if (!original.verifySignature(key)) {
                throw new IllegalArgumentException("original CONTROL artifact/key signature mismatch");
            }
            final var roles = EnumSet.noneOf(ControlRole.class);
            for (var role : f.text("roles").split(",", -1)) {
                if (!roles.add(ControlRole.valueOf(role))) {
                    throw new IllegalArgumentException("duplicate original CONTROL role");
                }
            }
            if (!roles.contains(ControlRole.PLATFORM_OPERATOR)) {
                throw new IllegalArgumentException("receipt history lacks its original PLATFORM_OPERATOR");
            }
            final var actor = new ControlAuthorizationContext(
                    f.hex("actorIdHash", 32, 32), new ControlRoleSet(roles), f.hex("tenantResourceScopeHash", 32, 32));
            ControlOperationAuthorization.authorize(
                    original.control(),
                    actor,
                    prepared -> Arrays.equals(
                            prepared.canonicalBytes(), original.control().canonicalBytes()));
            final var resource = KafkaReceiptSlotResource.decode(f.image("receiptResourceFile", 1024));
            final int slots = f.integer("slotsPerShard"), partition = f.integer("physicalPartition");
            final var channel =
                    ((TargetChannelControlRequest) original.control().request().branch()).channel();
            final var kafka = management.topic().kafka();
            if (!resource.authenticatedClusterId().equals(kafka.authenticatedClusterId())
                    || !resource.receiptTopicUuid().equals(kafka.nativeTopicUuid())
                    || !resource.routeIncarnation()
                            .equals(channel.context().sourceShard().routeIncarnation())
                    || resource.shardPartition()
                            != channel.context().sourceShard().partition()
                    || !Arrays.equals(
                            management.controlScopeRef(), channel.context().controlScopeRef())) {
                throw new IllegalArgumentException("receipt history changes its exact topic/Source/CONTROL scope");
            }
            new TargetKafkaReceiptBinding(
                    management.physicalTopic(),
                    resource,
                    management.physicalTopic(),
                    slots,
                    partition,
                    0,
                    original.digest(),
                    original.digest());
            final var entry = new Entry(original, management, actor, resource, slots, partition);
            if (entries.putIfAbsent(Bytes.hex(original.control().operationId()), entry) != null) {
                throw new IllegalArgumentException("duplicate original CONTROL operation history");
            }
            final String keyId = keyId(original.control().signingKeyVersion(), original.authorizedAt());
            final var prior = keys.putIfAbsent(keyId, key);
            if (prior != null && !Arrays.equals(prior.getEncoded(), key.getEncoded())) {
                throw new IllegalArgumentException("conflicting original CONTROL key at the same Source identity");
            }
            bytes = Math.addExact(
                    bytes,
                    Math.addExact(
                            original.canonicalBytes().length,
                            Math.addExact(
                                    management.canonicalBytes().length,
                                    Math.addExact(
                                            encodedKey.length,
                                            Math.addExact(
                                                    resource.canonicalBytes().length,
                                                    96L + actor.roleSet().canonicalBytes().length)))));
            if (bytes > inputs.maximumBytes) {
                throw new IllegalArgumentException("original CONTROL history exceeds its retained input bound");
            }
            f.requireComplete();
            guard.run();
        }
        guard.run();
        return new TargetReceiptControlHistory(entries, keys, bytes);
    }

    public long retainedBytes() {
        return retainedBytes;
    }
    /** Public original CONTROL scope/key facts; the consumer must still authenticate the Source-approved copy. */
    public byte[] historicalPublicInputs(
            TargetKafkaReceiptControlArtifact artifact, TargetKafkaReceiptControlArtifact protection) {
        final var p = protection.protection();
        final var original = entry(protection.control(), protection.authorizedAt());
        requireCovered(p, protection.control(), original.actor(), protection.authorizedAt());
        final var key = resolve(artifact.control().signingKeyVersion(), artifact.authorizedAt());
        if (key == null
                || !artifact.verifySignature(key)
                || !protection.verifySignature(key)
                || !Arrays.equals(
                        artifact.control().canonicalBytes(),
                        protection.control().canonicalBytes())
                || !Arrays.equals(
                        artifact.authorizedAt().canonicalBytes(),
                        protection.authorizedAt().canonicalBytes())
                || artifact.kind() == TargetKafkaReceiptControlArtifact.MANAGEMENT
                || artifact.kind() == TargetKafkaReceiptControlArtifact.PROTECTION
                        && !Arrays.equals(artifact.canonicalBytes(), protection.canonicalBytes())
                || artifact.kind() == TargetKafkaReceiptControlArtifact.ANCHOR
                        && !Arrays.equals(artifact.protectionDigest(), protection.digest())) {
            throw new IllegalArgumentException("retirement copy changes original CONTROL artifact history");
        }
        return com.nereusstream.delay.protocol.CanonicalProtobuf.message(out -> {
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 1, 1);
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(
                    out, 2, original.original().canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(
                    out, 3, com.nereusstream.delay.protocol.CanonicalProtobuf.message(actor -> {
                        com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(
                                actor, 1, original.actor().actorIdHash());
                        com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(
                                actor, 2, original.actor().roleSet().canonicalBytes());
                        com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(
                                actor, 3, original.actor().tenantResourceScopeHash());
                    }));
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(
                    out, 4, original.resource().canonicalBytes());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 5, original.slots());
            com.nereusstream.delay.protocol.CanonicalProtobuf.uint32(out, 6, original.partition());
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 7, key.getEncoded());
            com.nereusstream.delay.protocol.CanonicalProtobuf.bytes(out, 8, protection.canonicalBytes());
        });
    }

    /** Configuration shape preflight, before an Admin is created or its whole-topic installation can start. */
    public void requireReceipt(
            TargetKafkaReceiptControlArtifact authorization,
            KafkaReceiptSlotResource resource,
            int slots,
            int partition) {
        final var original = entry(authorization.control(), authorization.authorizedAt());
        if (authorization.kind() != TargetKafkaReceiptControlArtifact.MANAGEMENT
                || !Arrays.equals(
                        authorization.canonicalBytes(), original.original().canonicalBytes())
                || !original.resource().equals(resource)
                || original.slots() != slots
                || original.partition() != partition) {
            throw new IllegalArgumentException(
                    "configured receipt differs from its original independent root authorization");
        }
    }

    /** Only the exact original full Source point is covered; a current key or another Shard is never substituted. */
    @Override
    public PublicKey resolve(long version, SourcePosition source) {
        return keys.get(keyId(version, source));
    }

    @Override
    public ControlAuthorizationContext resolve(PreparedControlOperation operation, SourcePosition at) {
        return entry(operation, at).actor();
    }

    @Override
    public void requireManagementCovered(
            TargetKafkaReceiptManagementImage image,
            PreparedControlOperation operation,
            ControlAuthorizationContext actor,
            SourcePosition at) {
        final var original = entry(operation, at);
        requireActor(original, actor);
        if (!Arrays.equals(original.management().canonicalBytes(), image.canonicalBytes())) {
            throw new IllegalArgumentException("whole-topic management exceeds its original independent root scope");
        }
    }

    @Override
    public void requireCovered(
            TargetKafkaReceiptProtection image,
            PreparedControlOperation operation,
            ControlAuthorizationContext actor,
            SourcePosition at) {
        final var original = entry(operation, at);
        requireActor(original, actor);
        final var management = original.management();
        final var embedded = TargetKafkaReceiptControlArtifact.decode(image.managementAuthorityImage());
        if (embedded.kind() != TargetKafkaReceiptControlArtifact.MANAGEMENT
                || !Arrays.equals(embedded.image(), management.canonicalBytes())
                || !image.resource().equals(original.resource())
                || image.slotsPerShard() != original.slots()
                || image.physicalPartition() != original.partition()
                || !image.physicalTopic().equals(management.physicalTopic())
                || !image.capability().equals(management.capability().ref())
                || !Arrays.equals(image.controlScopeRef(), management.controlScopeRef())
                || !Arrays.equals(image.managementEpoch(), management.managementEpoch())) {
            throw new IllegalArgumentException(
                    "receipt protection exceeds its original exact deployment/resource scope");
        }
        image.requireChannel(((TargetChannelControlRequest) operation.request().branch()).channel());
    }

    private Entry entry(PreparedControlOperation operation, SourcePosition at) {
        final var value =
                entries.get(Bytes.hex(Objects.requireNonNull(operation).operationId()));
        if (value == null
                || !Arrays.equals(value.original().control().canonicalBytes(), operation.canonicalBytes())
                || !Arrays.equals(
                        value.original().authorizedAt().canonicalBytes(),
                        Objects.requireNonNull(at).canonicalBytes())) {
            throw new IllegalArgumentException("missing original CONTROL operation/Source authorization history");
        }
        return value;
    }

    private static void requireActor(Entry entry, ControlAuthorizationContext actor) {
        ControlOperationAuthorization.authorize(
                entry.original().control(),
                Objects.requireNonNull(actor),
                operation -> Arrays.equals(
                        operation.canonicalBytes(), entry.original().control().canonicalBytes()));
        if (!actor.roleSet().contains(ControlRole.PLATFORM_OPERATOR)) {
            throw new IllegalArgumentException("receipt scope requires PLATFORM_OPERATOR");
        }
    }

    private static String keyId(long version, SourcePosition source) {
        return Long.toUnsignedString(version) + "/"
                + Bytes.hex(Bytes.sha256(
                        Bytes.utf8("nereus-delay-receipt-control-key-source\0"),
                        TargetSourcePosition.requireBounded(source).canonicalBytes()));
    }
}
