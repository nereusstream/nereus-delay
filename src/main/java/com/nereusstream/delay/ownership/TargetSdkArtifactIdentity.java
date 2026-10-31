package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.Bytes;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** Loaded SDK artifact verification against the caller's actually activated immutable Artifact, without a cache. */
public final class TargetSdkArtifactIdentity {
    private TargetSdkArtifactIdentity() {}

    public static void requireExact(Class<?> guardedApi, byte[] activatedJarSha256) {
        Bytes.requireLength(activatedJarSha256, 32, "activated SDK JAR SHA256");
        try {
            final var code =
                    Objects.requireNonNull(guardedApi.getProtectionDomain().getCodeSource(), "loaded guarded SDK");
            final var path = Path.of(code.getLocation().toURI());
            final long size = Files.size(path);
            if (!Files.isRegularFile(path) || size <= 0 || size > 64L * 1024 * 1024) {
                throw new IllegalStateException("destination requires a bounded actually loaded SDK JAR");
            }
            final var digest = MessageDigest.getInstance("SHA-256");
            final byte[] buffer = new byte[65536];
            long bytes = 0;
            try (var stream = Files.newInputStream(path)) {
                for (int count; (count = stream.read(buffer)) != -1; ) {
                    bytes = Math.addExact(bytes, count);
                    if (bytes > 64L * 1024 * 1024) {
                        throw new IllegalStateException("SDK JAR exceeded its input bound");
                    }
                    digest.update(buffer, 0, count);
                }
            }
            if (bytes != size || !Arrays.equals(digest.digest(), activatedJarSha256)) {
                throw new IllegalStateException("loaded SDK differs from activated Artifact");
            }
        } catch (java.io.IOException | java.net.URISyntaxException | java.security.NoSuchAlgorithmException failure) {
            throw new IllegalStateException("actual SDK artifact verification failed", failure);
        }
    }
}
