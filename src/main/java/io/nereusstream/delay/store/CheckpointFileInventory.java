package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.Normalizer;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Immutable checksum inventory of the complete physical RocksDB checkpoint. */
public record CheckpointFileInventory(String name, long length, byte[] checksum) {
    public CheckpointFileInventory {
        name = canonicalName(name);
        if (length < 0) {
            throw new IllegalArgumentException("checkpoint file length must be non-negative");
        }
        Bytes.requireLength(checksum, 32, "checksum");
        checksum = Bytes.copy(checksum);
    }

    @Override
    public byte[] checksum() {
        return Bytes.copy(checksum);
    }

    public static List<CheckpointFileInventory> collect(final Path checkpointRoot) {
        return collect(checkpointRoot, CheckpointManifestLimits.unbounded());
    }

    /** Collects an inventory while enforcing the activated physical limits. */
    public static List<CheckpointFileInventory> collect(final Path checkpointRoot,
                                                        final CheckpointManifestLimits limits) {
        if (Files.isSymbolicLink(checkpointRoot)
                || !Files.isDirectory(checkpointRoot, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("checkpoint root is not a real directory: " + checkpointRoot);
        }
        Objects.requireNonNull(limits, "limits");
        try (var paths = Files.walk(checkpointRoot)) {
            final List<CheckpointFileInventory> result = new java.util.ArrayList<>();
            long totalBytes = 0;
            final var iterator = paths.iterator();
            while (iterator.hasNext()) {
                final Path path = iterator.next();
                if (Files.isSymbolicLink(path)) {
                    throw new IllegalArgumentException("checkpoint contains a symbolic link: " + path);
                }
                if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                    continue;
                }
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalArgumentException("checkpoint contains a non-regular file: " + path);
                }
                if (result.size() >= limits.maxFiles()) {
                    throw new IllegalArgumentException("checkpoint file count exceeds configured bound");
                }
                final String name = checkpointRoot.relativize(path).toString()
                        .replace(path.getFileSystem().getSeparator(), "/");
                final long length = Files.size(path);
                limits.validateFile(name, length);
                totalBytes = Math.addExact(totalBytes, length);
                if (totalBytes > limits.maxTotalFileBytes()) {
                    throw new IllegalArgumentException("checkpoint total file bytes exceed configured bound");
                }
                result.add(new CheckpointFileInventory(name, length, sha256(path)));
            }
            return result.stream().sorted(Comparator.comparing(CheckpointFileInventory::name)).toList();
        } catch (IOException exception) {
            throw new IllegalStateException("cannot inventory checkpoint files", exception);
        }
    }

    /** Streams the file so inventory creation cannot allocate an SST-sized byte array. */
    private static byte[] sha256(final Path path) throws IOException {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
        final byte[] buffer = new byte[64 * 1024];
        try (InputStream input = Files.newInputStream(path)) {
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read != 0) {
                    digest.update(buffer, 0, read);
                }
            }
        }
        return digest.digest();
    }

    private static String canonicalName(final String value) {
        if (value == null || value.isEmpty() || !Normalizer.isNormalized(value, Normalizer.Form.NFC)
                || value.startsWith("/") || value.contains("\\") || value.contains("\0")) {
            throw new IllegalArgumentException("checkpoint file name is not canonical");
        }
        final String[] segments = value.split("/", -1);
        for (String segment : segments) {
            if (segment.isEmpty() || segment.equals(".") || segment.equals("..")) {
                throw new IllegalArgumentException("checkpoint file name contains an invalid path segment");
            }
        }
        return value;
    }
}
