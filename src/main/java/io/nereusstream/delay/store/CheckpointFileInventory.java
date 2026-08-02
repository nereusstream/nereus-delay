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
        if (Files.isSymbolicLink(checkpointRoot)
                || !Files.isDirectory(checkpointRoot, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("checkpoint root is not a real directory: " + checkpointRoot);
        }
        try (var paths = Files.walk(checkpointRoot)) {
            return paths.filter(path -> {
                if (Files.isSymbolicLink(path)) {
                    throw new IllegalArgumentException("checkpoint contains a symbolic link: " + path);
                }
                if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
                    return false;
                }
                if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalArgumentException("checkpoint contains a non-regular file: " + path);
                }
                return true;
            })
                    .map(path -> create(checkpointRoot, path))
                    .sorted(Comparator.comparing(CheckpointFileInventory::name))
                    .toList();
        } catch (IOException exception) {
            throw new IllegalStateException("cannot inventory checkpoint files", exception);
        }
    }

    private static CheckpointFileInventory create(final Path root, final Path path) {
        try {
            return new CheckpointFileInventory(root.relativize(path).toString().replace(path.getFileSystem().getSeparator(), "/"),
                    Files.size(path), sha256(path));
        } catch (IOException exception) {
            throw new IllegalStateException("cannot read checkpoint file: " + path, exception);
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
