package com.nereusstream.delay.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/** Bounded durable companion inside an unpublished immutable checkpoint candidate. */
public final class TargetCheckpointSemanticSnapshotFile {
    public static final String NAME = "TARGET-SEMANTICS.pb";

    private TargetCheckpointSemanticSnapshotFile() {}

    static void writeNew(Path directory, TargetCheckpointSemanticSnapshot snapshot,
            TargetCheckpointSemanticSnapshot.Limits limits) {
        final byte[] bytes = snapshot.canonicalBytes();
        TargetCheckpointSemanticSnapshot.decode(bytes, limits);
        requireDirectory(directory);
        try (var channel = FileChannel.open(directory.resolve(NAME), StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
            final var buffer = ByteBuffer.wrap(bytes);
            while (buffer.hasRemaining()) { channel.write(buffer); }
            channel.force(true);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot persist Target semantic companion", failure);
        }
    }

    public static TargetCheckpointSemanticSnapshot read(Path directory,
            TargetCheckpointSemanticSnapshot.Limits limits) {
        requireDirectory(directory);
        final Path path = directory.resolve(NAME);
        if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(path)) {
            throw new IllegalArgumentException("Target checkpoint lacks a regular semantic companion");
        }
        try (var channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            final long length = channel.size();
            if (length <= 0 || length > limits.maximumBytes()) {
                throw new IllegalArgumentException("Target semantic companion exceeds its byte limit");
            }
            final var buffer = ByteBuffer.allocate(Math.toIntExact(length));
            while (buffer.hasRemaining()) {
                if (channel.read(buffer) < 0) { throw new IOException("Target semantic companion was truncated"); }
            }
            if (channel.read(ByteBuffer.allocate(1)) != -1 || channel.size() != length) {
                throw new IOException("Target semantic companion changed during read");
            }
            return TargetCheckpointSemanticSnapshot.decode(buffer.array(), limits);
        } catch (IOException failure) {
            throw new IllegalStateException("cannot read Target semantic companion", failure);
        }
    }

    static TargetCheckpointSemanticSnapshot requireExact(Path directory, TargetCheckpointSemanticSnapshot expected,
            TargetCheckpointSemanticSnapshot.Limits limits) {
        final var actual = read(directory, limits);
        if (!Arrays.equals(expected.canonicalBytes(), actual.canonicalBytes())) {
            throw new IllegalArgumentException("existing Target semantic companion differs from the exact request");
        }
        return actual;
    }

    private static void requireDirectory(Path directory) {
        if (Files.isSymbolicLink(directory) || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("Target semantic companion requires a real checkpoint directory");
        }
    }
}
