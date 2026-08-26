package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;

/**
 * Crash-durable local provider seam for checkpoint uploads.
 *
 * <p>Each manifest file is copied to a deterministic object path derived from
 * its opaque object-key/version identity. Existing objects are accepted only
 * when their complete bytes and checksum match; a different value is an
 * immutable-object conflict. The manifest object is written last through a
 * temporary file and an atomic create-new rename. This models the local
 * if-absent and response-loss behavior required by the current designwithout claiming remote
 * credentials, provider quiescence, object-store consistency, or catalog
 * authority.</p>
 */
public final class FilesystemCheckpointUploadAdapter implements CheckpointUploadAdapter {
    private static final byte[] OBJECT_PATH_DOMAIN = Bytes.utf8("nereus-delay-filesystem-checkpoint-object\0");
    private static final int BUFFER_BYTES = 64 * 1024;

    private final Path root;
    private final byte[] container;
    private final CheckpointManifestLimits limits;

    public FilesystemCheckpointUploadAdapter(final Path root, final String container) {
        this(root, Bytes.utf8(Objects.requireNonNull(container, "container")), CheckpointManifestLimits.unbounded());
    }

    public FilesystemCheckpointUploadAdapter(
            final Path root, final String container, final CheckpointManifestLimits limits) {
        this(root, Bytes.utf8(Objects.requireNonNull(container, "container")), limits);
    }

    public FilesystemCheckpointUploadAdapter(
            final Path root, final byte[] container, final CheckpointManifestLimits limits) {
        this.root = normalizeRoot(root);
        Objects.requireNonNull(container, "container");
        if (container.length == 0) {
            throw new IllegalArgumentException("container must not be empty");
        }
        this.container = Bytes.copy(container);
        this.limits = Objects.requireNonNull(limits, "limits");
        ensureDirectory(this.root);
    }

    @Override
    public synchronized CheckpointResource upload(final CheckpointUploadRequest request) {
        Objects.requireNonNull(request, "request");
        final CheckpointManifest manifest = request.manifest();
        manifest.validateLimits(limits);
        if (!java.util.Arrays.equals(request.manifestBytes(), manifest.canonicalJsonBytes())) {
            throw new IllegalArgumentException("checkpoint manifest bytes are not canonical");
        }
        final Path checkpointDirectory = normalizeCheckpointDirectory(request.checkpointDirectory());
        final List<CheckpointFileInventory> actual = CheckpointFileInventory.collect(checkpointDirectory, limits);
        validateInventory(actual, manifest.files());

        final Path prefix = root.resolve("checkpoints")
                .resolve(Bytes.hex(manifest.recoveryLineageId()))
                .resolve(Bytes.hex(manifest.checkpointId()));
        ensureDirectoryWithinRoot(prefix);
        for (CheckpointManifest.FileEntry file : manifest.files()) {
            final Path source = checkpointDirectory.resolve(file.name()).normalize();
            if (!source.startsWith(checkpointDirectory)) {
                throw new IllegalArgumentException("checkpoint file escapes its source directory");
            }
            putImmutable(
                    prefix.resolve("objects").resolve(objectFileName(file.objectKey(), file.objectVersion())),
                    source,
                    file.length(),
                    file.checksum());
        }

        final byte[] manifestBytes = request.manifestBytes();
        final byte[] manifestHash = manifest.manifestSha256();
        final String manifestKey = "checkpoints/" + Bytes.hex(manifest.recoveryLineageId()) + "/"
                + Bytes.hex(manifest.checkpointId()) + "/manifest.json";
        putImmutableBytes(prefix.resolve("manifest.json"), manifestBytes, manifestHash);
        return new CheckpointResource(
                manifest.recoveryLineageId(),
                manifest.checkpointId(),
                request.intent().objectStoreProfile(),
                container,
                Bytes.utf8(manifestKey),
                Bytes.utf8("sha256-" + Bytes.hex(manifestHash)),
                manifestBytes.length,
                manifestHash);
    }

    private static void validateInventory(
            final List<CheckpointFileInventory> actual, final List<CheckpointManifest.FileEntry> expected) {
        if (actual.size() != expected.size()) {
            throw new IllegalArgumentException("checkpoint file inventory differs from manifest");
        }
        for (int index = 0; index < actual.size(); index++) {
            final CheckpointFileInventory left = actual.get(index);
            final CheckpointManifest.FileEntry right = expected.get(index);
            if (!left.name().equals(right.name())
                    || left.length() != right.length()
                    || !Bytes.constantTimeEquals(left.checksum(), right.checksum())) {
                throw new IllegalArgumentException("checkpoint file inventory differs from manifest: " + left.name());
            }
        }
    }

    static String objectFileName(final byte[] objectKey, final byte[] objectVersion) {
        return Bytes.hex(Bytes.sha256(OBJECT_PATH_DOMAIN, objectKey, Bytes.lp32(objectVersion)));
    }

    private void putImmutable(
            final Path target, final Path source, final long expectedLength, final byte[] expectedChecksum) {
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            verifyExisting(target, expectedLength, expectedChecksum);
            return;
        }
        ensureDirectoryWithinRoot(Objects.requireNonNull(target.getParent(), "target parent"));
        Path temporary = null;
        try {
            temporary = Files.createTempFile(target.getParent(), ".checkpoint-object-", ".tmp");
            copyAndHash(source, temporary, expectedLength, expectedChecksum);
            moveCreateNewOrVerify(temporary, target, expectedLength, expectedChecksum);
            temporary = null;
        } catch (IOException failure) {
            throw new IllegalStateException("cannot write checkpoint object", failure);
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException cleanupFailure) {
                    // Preserve the original provider failure; the object was
                    // never made visible at its immutable target path.
                }
            }
        }
    }

    private void putImmutableBytes(final Path target, final byte[] bytes, final byte[] expectedChecksum) {
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            verifyExisting(target, bytes.length, expectedChecksum);
            return;
        }
        ensureDirectoryWithinRoot(Objects.requireNonNull(target.getParent(), "target parent"));
        Path temporary = null;
        try {
            temporary = Files.createTempFile(target.getParent(), ".checkpoint-manifest-", ".tmp");
            try (FileChannel output =
                    FileChannel.open(temporary, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                final ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) {
                    output.write(buffer);
                }
                output.force(true);
            }
            moveCreateNewOrVerify(temporary, target, bytes.length, expectedChecksum);
            temporary = null;
        } catch (IOException failure) {
            throw new IllegalStateException("cannot write checkpoint manifest object", failure);
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException cleanupFailure) {
                    // Preserve the original provider failure; the object was
                    // never made visible at its immutable target path.
                }
            }
        }
    }

    private static void copyAndHash(
            final Path source, final Path temporary, final long expectedLength, final byte[] expectedChecksum)
            throws IOException {
        if (Files.isSymbolicLink(source) || !Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("checkpoint source is not a regular file: " + source);
        }
        final MessageDigest digest = sha256();
        long length = 0;
        try (FileChannel input = FileChannel.open(source, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
                FileChannel output =
                        FileChannel.open(temporary, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_BYTES);
            while (true) {
                buffer.clear();
                final int read = input.read(buffer);
                if (read < 0) {
                    break;
                }
                if (read == 0) {
                    continue;
                }
                digest.update(buffer.array(), 0, read);
                buffer.flip();
                while (buffer.hasRemaining()) {
                    output.write(buffer);
                }
                length = Math.addExact(length, read);
            }
            output.force(true);
        }
        if (length != expectedLength || !Bytes.constantTimeEquals(digest.digest(), expectedChecksum)) {
            throw new IllegalArgumentException("checkpoint source changed while uploading");
        }
    }

    private static void moveCreateNewOrVerify(
            final Path temporary, final Path target, final long expectedLength, final byte[] expectedChecksum)
            throws IOException {
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
            forceDirectory(target.getParent());
        } catch (AtomicMoveNotSupportedException unsupported) {
            throw new IOException("checkpoint object requires atomic rename", unsupported);
        } catch (java.nio.file.FileAlreadyExistsException race) {
            verifyExisting(target, expectedLength, expectedChecksum);
            Files.deleteIfExists(temporary);
        }
    }

    private static void verifyExisting(final Path target, final long expectedLength, final byte[] expectedChecksum) {
        if (Files.isSymbolicLink(target) || !Files.isRegularFile(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("immutable checkpoint object is not a regular file: " + target);
        }
        try {
            final HashedFile actual = hash(target);
            if (actual.length() != expectedLength || !Bytes.constantTimeEquals(actual.checksum(), expectedChecksum)) {
                throw new IllegalStateException("immutable checkpoint object identity conflict: " + target);
            }
        } catch (IOException failure) {
            throw new IllegalStateException("cannot verify immutable checkpoint object", failure);
        }
    }

    private static HashedFile hash(final Path path) throws IOException {
        final MessageDigest digest = sha256();
        long length = 0;
        try (FileChannel input = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_BYTES);
            while (true) {
                buffer.clear();
                final int read = input.read(buffer);
                if (read < 0) {
                    break;
                }
                if (read == 0) {
                    continue;
                }
                digest.update(buffer.array(), 0, read);
                length = Math.addExact(length, read);
            }
        }
        return new HashedFile(length, digest.digest());
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static Path normalizeRoot(final Path value) {
        Objects.requireNonNull(value, "root");
        if (Files.isSymbolicLink(value)) {
            throw new IllegalArgumentException("checkpoint Object Store root must not be a symbolic link");
        }
        return value.toAbsolutePath().normalize();
    }

    private static Path normalizeCheckpointDirectory(final Path value) {
        Objects.requireNonNull(value, "checkpointDirectory");
        final Path normalized = value.toAbsolutePath().normalize();
        if (Files.isSymbolicLink(normalized) || !Files.isDirectory(normalized, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalArgumentException("checkpoint root is not a real directory: " + value);
        }
        return normalized;
    }

    private static void ensureDirectory(final Path directory) {
        try {
            if (Files.isSymbolicLink(directory)) {
                throw new IllegalArgumentException("checkpoint Object Store path must not contain a symbolic link");
            }
            Files.createDirectories(directory);
            if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalArgumentException("checkpoint Object Store path is not a directory");
            }
        } catch (IOException failure) {
            throw new IllegalStateException("cannot create checkpoint Object Store directory", failure);
        }
    }

    /** Creates only descendants of the configured root and rejects child symlinks. */
    private void ensureDirectoryWithinRoot(final Path directory) {
        final Path normalized =
                Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
        if (!normalized.startsWith(root)) {
            throw new IllegalArgumentException("checkpoint Object Store path escapes its root");
        }
        if (Files.isSymbolicLink(root)) {
            throw new IllegalArgumentException("checkpoint Object Store root must not be a symbolic link");
        }
        Path current = root;
        for (Path component : root.relativize(normalized)) {
            current = current.resolve(component);
            try {
                if (Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
                    if (Files.isSymbolicLink(current) || !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
                        throw new IllegalArgumentException(
                                "checkpoint Object Store path must contain only real directories: " + current);
                    }
                } else {
                    Files.createDirectory(current);
                }
            } catch (java.nio.file.FileAlreadyExistsException race) {
                if (Files.isSymbolicLink(current) || !Files.isDirectory(current, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IllegalArgumentException(
                            "checkpoint Object Store path must contain only real directories: " + current, race);
                }
            } catch (IOException failure) {
                throw new IllegalStateException("cannot create checkpoint Object Store directory", failure);
            }
        }
    }

    private static void forceDirectory(final Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private record HashedFile(long length, byte[] checksum) {}
}
