package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CanonicalProtobuf;
import com.nereusstream.delay.protocol.ProfileKind;
import com.nereusstream.delay.protocol.ProfileRef;
import com.nereusstream.delay.protocol.QueryCodecSupport;
import com.nereusstream.delay.protocol.TargetQuotaScope;
import com.nereusstream.delay.store.S3CompatibleCheckpointObjectStoreAdapter;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Arrays;
import java.util.Objects;

/** Literal deployment references and finite costs; this document grants no checkpoint or HISTORY rights. */
public record TargetWorkerCheckpointConfiguration(
        TargetQuotaScope scope,
        byte[] recoveryLineageId,
        String shardPrefix,
        Path checkpointDirectory,
        String objectPrefix,
        String objectNamePrefix,
        ProfileRef objectStoreProfile,
        URI endpoint,
        String region,
        String bucket,
        long requestTimeoutMillis,
        long uploadTtlMillis,
        long credentialLeaseTtlMillis,
        long maximumAttestationAgeMillis,
        long renewBeforeMillis,
        long retainedHeapBytes,
        long sdkNativeBytes,
        int maximumHttpClients,
        int maximumCandidates,
        Path historyAuthorizationFile) {
    public static final int MAX_CANONICAL_BYTES = 32_768;

    public TargetWorkerCheckpointConfiguration {
        Objects.requireNonNull(scope, "scope");
        if (scope.target() != null) {
            throw new IllegalArgumentException("checkpoint requires the complete Source scope");
        }
        recoveryLineageId = TargetScopeResourceConfiguration.assigned(recoveryLineageId, 16, "recovery lineage");
        shardPrefix = TargetScopeResourceConfiguration.text(shardPrefix, 2048, "checkpoint Shard prefix");
        if (!shardPrefix.startsWith("/")
                || shardPrefix.endsWith("/")
                || shardPrefix.contains("//")
                || shardPrefix.contains("/../")
                || shardPrefix.contains("/./")) {
            throw new IllegalArgumentException("checkpoint Shard prefix must be a literal canonical absolute prefix");
        }
        checkpointDirectory = literalPath(checkpointDirectory, "checkpoint directory");
        historyAuthorizationFile = literalPath(historyAuthorizationFile, "HISTORY authorization file");
        objectPrefix = TargetScopeResourceConfiguration.text(objectPrefix, 256, "checkpoint object prefix");
        if (!objectPrefix.equals("checkpoints/" + Bytes.hex(recoveryLineageId))) {
            throw new IllegalArgumentException("checkpoint object prefix differs from the existing S3 layout");
        }
        objectNamePrefix = TargetScopeResourceConfiguration.text(objectNamePrefix, 256, "checkpoint object name");
        if (objectNamePrefix.contains("/")
                || objectNamePrefix.contains("\\")
                || objectNamePrefix.equals(".")
                || objectNamePrefix.equals("..")) {
            throw new IllegalArgumentException("invalid checkpoint object name");
        }
        Objects.requireNonNull(objectStoreProfile, "objectStoreProfile");
        if (objectStoreProfile.profileKind() != ProfileKind.OBJECT_STORE) {
            throw new IllegalArgumentException("checkpoint requires an OBJECT_STORE Profile reference");
        }
        Objects.requireNonNull(endpoint, "endpoint");
        S3CompatibleCheckpointObjectStoreAdapter.endpointConfigDigest(endpoint, region, bucket);
        if (!endpoint.toString().equals(endpoint.toASCIIString())) {
            throw new IllegalArgumentException("checkpoint endpoint must be a literal ASCII URI");
        }
        TargetScopeResourceConfiguration.positive(requestTimeoutMillis, "checkpoint request timeout");
        TargetScopeResourceConfiguration.positive(uploadTtlMillis, "checkpoint upload TTL");
        TargetScopeResourceConfiguration.positive(credentialLeaseTtlMillis, "checkpoint credential TTL");
        TargetScopeResourceConfiguration.positive(maximumAttestationAgeMillis, "checkpoint attestation age");
        TargetScopeResourceConfiguration.positive(renewBeforeMillis, "checkpoint renewal window");
        TargetScopeResourceConfiguration.positive(retainedHeapBytes, "checkpoint retained heap");
        TargetScopeResourceConfiguration.positive(sdkNativeBytes, "checkpoint SDK native bytes");
        if (renewBeforeMillis >= credentialLeaseTtlMillis
                || requestTimeoutMillis >= credentialLeaseTtlMillis
                || maximumHttpClients != 1
                || maximumCandidates < 2
                || maximumCandidates >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "checkpoint requires one bounded actual HTTP client and finite candidates");
        }
        try {
            Duration.ofMillis(requestTimeoutMillis).toNanos();
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException("checkpoint timeout overflows", overflow);
        }
    }

    @Override
    public byte[] recoveryLineageId() {
        return recoveryLineageId.clone();
    }

    public Duration requestTimeout() {
        return Duration.ofMillis(requestTimeoutMillis);
    }

    public byte[] canonicalBytes() {
        return CanonicalProtobuf.message(out -> {
            CanonicalProtobuf.uint32(out, 1, 1);
            CanonicalProtobuf.bytes(out, 2, scope.canonicalBytes());
            CanonicalProtobuf.bytes(out, 3, recoveryLineageId);
            CanonicalProtobuf.bytes(out, 4, Bytes.utf8(shardPrefix));
            CanonicalProtobuf.bytes(out, 5, Bytes.utf8(checkpointDirectory.toString()));
            CanonicalProtobuf.bytes(out, 6, Bytes.utf8(objectPrefix));
            CanonicalProtobuf.bytes(out, 7, Bytes.utf8(objectNamePrefix));
            CanonicalProtobuf.bytes(out, 8, objectStoreProfile.canonicalBytes());
            CanonicalProtobuf.bytes(out, 9, Bytes.utf8(endpoint.toString()));
            CanonicalProtobuf.bytes(out, 10, Bytes.utf8(region));
            CanonicalProtobuf.bytes(out, 11, Bytes.utf8(bucket));
            CanonicalProtobuf.uint64(out, 12, requestTimeoutMillis);
            CanonicalProtobuf.uint64(out, 13, uploadTtlMillis);
            CanonicalProtobuf.uint64(out, 14, credentialLeaseTtlMillis);
            CanonicalProtobuf.uint64(out, 15, maximumAttestationAgeMillis);
            CanonicalProtobuf.uint64(out, 16, renewBeforeMillis);
            CanonicalProtobuf.uint64(out, 17, retainedHeapBytes);
            CanonicalProtobuf.uint64(out, 18, sdkNativeBytes);
            CanonicalProtobuf.uint32(out, 19, maximumHttpClients);
            CanonicalProtobuf.uint32(out, 20, maximumCandidates);
            CanonicalProtobuf.bytes(out, 21, Bytes.utf8(historyAuthorizationFile.toString()));
        });
    }

    public static TargetWorkerCheckpointConfiguration decode(byte[] encoded) {
        final var f = TargetScopeResourceConfiguration.fields(
                encoded, MAX_CANONICAL_BYTES, 21, "checkpoint deployment configuration");
        if (QueryCodecSupport.uint32(f.get(0), 1) != 1) {
            throw new IllegalArgumentException("unknown checkpoint deployment configuration schema");
        }
        final var value = new TargetWorkerCheckpointConfiguration(
                TargetQuotaScope.decode(QueryCodecSupport.bytes(f.get(1), 2)),
                QueryCodecSupport.bytes(f.get(2), 3),
                utf8(f.get(3), 4),
                Path.of(utf8(f.get(4), 5)),
                utf8(f.get(5), 6),
                utf8(f.get(6), 7),
                ProfileRef.decode(QueryCodecSupport.bytes(f.get(7), 8)),
                URI.create(utf8(f.get(8), 9)),
                utf8(f.get(9), 10),
                utf8(f.get(10), 11),
                QueryCodecSupport.uint64Bits(f.get(11), 12),
                QueryCodecSupport.uint64Bits(f.get(12), 13),
                QueryCodecSupport.uint64Bits(f.get(13), 14),
                QueryCodecSupport.uint64Bits(f.get(14), 15),
                QueryCodecSupport.uint64Bits(f.get(15), 16),
                QueryCodecSupport.uint64Bits(f.get(16), 17),
                QueryCodecSupport.uint64Bits(f.get(17), 18),
                QueryCodecSupport.uint32(f.get(18), 19),
                QueryCodecSupport.uint32(f.get(19), 20),
                Path.of(utf8(f.get(20), 21)));
        TargetScopeResourceConfiguration.canonical(encoded, value.canonicalBytes());
        return value;
    }

    public static TargetWorkerCheckpointConfiguration load(Path path) throws IOException {
        final var file = Objects.requireNonNull(path).toAbsolutePath().normalize();
        final var before = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!before.isRegularFile() || before.size() < 1 || before.size() > MAX_CANONICAL_BYTES) {
            throw new IOException("checkpoint configuration requires a bounded ordinary canonical file");
        }
        final byte[] image;
        try (var input = Files.newInputStream(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            image = input.readNBytes(MAX_CANONICAL_BYTES + 1);
        }
        final var after = Files.readAttributes(file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (image.length > MAX_CANONICAL_BYTES
                || !after.isRegularFile()
                || image.length != before.size()
                || before.size() != after.size()
                || !Objects.equals(before.fileKey(), after.fileKey())
                || !before.lastModifiedTime().equals(after.lastModifiedTime())) {
            throw new IOException("checkpoint configuration changed during bounded loading");
        }
        return decode(image);
    }

    private static String utf8(CanonicalProtobuf.Reader.Field field, int number) {
        final var bytes = QueryCodecSupport.bytes(field, number);
        final var value = new String(bytes, StandardCharsets.UTF_8);
        if (!Arrays.equals(bytes, value.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("invalid checkpoint configuration UTF-8");
        }
        return value;
    }

    private static Path literalPath(Path value, String name) {
        Objects.requireNonNull(value, name);
        TargetScopeResourceConfiguration.text(value.toString(), 4096, name);
        if (!value.isAbsolute()
                || !value.equals(value.normalize())
                || value.toString().contains("${")
                || value.toString().contains("~")) {
            throw new IllegalArgumentException(name + " must be a literal normalized absolute path");
        }
        return value;
    }
}
