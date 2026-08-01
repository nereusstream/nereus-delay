package io.nereusstream.delay.store;

import io.nereusstream.delay.protocol.Bytes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
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
        try (var paths = Files.walk(checkpointRoot)) {
            return paths.filter(Files::isRegularFile)
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
                    Files.size(path), Bytes.sha256(Files.readAllBytes(path)));
        } catch (IOException exception) {
            throw new IllegalStateException("cannot read checkpoint file: " + path, exception);
        }
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
