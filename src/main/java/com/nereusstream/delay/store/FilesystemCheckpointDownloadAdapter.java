package com.nereusstream.delay.store;

import com.nereusstream.delay.protocol.Bytes;
import com.nereusstream.delay.protocol.CheckpointResource;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Objects;
import java.util.UUID;

/**
 * Crash-durable local/test download boundary for one immutable checkpoint.
 *
 * <p>The adapter verifies the catalog-supplied manifest object before reading
 * any file object, streams each object through a no-follow file handle while
 * checking its length and checksum, inventories the complete staged tree, and
 * publishes the target directory only through an atomic rename. It models the
 * local provider/recovery ordering without claiming remote credentials,
 * provider consistency, catalog authority, or source replay.</p>
 */
public final class FilesystemCheckpointDownloadAdapter implements CheckpointDownloadAdapter {
    private static final int BUFFER_BYTES = 64 * 1024;

    private final Path root;
    private final CheckpointManifestLimits limits;

    public FilesystemCheckpointDownloadAdapter(final Path root) {
        this(root, CheckpointManifestLimits.unbounded());
    }

    public FilesystemCheckpointDownloadAdapter(final Path root, final CheckpointManifestLimits limits) {
        this.root = normalizeRoot(root);
        this.limits = Objects.requireNonNull(limits, "limits");
        ensureDirectory(this.root);
    }

    /** Downloads one exact checkpoint into a new directory and returns it. */
    @Override
    public synchronized Path download(final CheckpointDownloadRequest request, final Path targetDirectory) {
        Objects.requireNonNull(request, "request");
        final CheckpointManifest manifest = request.manifest();
        manifest.validateLimits(limits);
        final CheckpointResource resource = request.resource();
        final Path target = normalizeTarget(targetDirectory);
        final Path targetParent = Objects.requireNonNull(target.getParent(), "target parent");
        ensureDirectory(targetParent);
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new IllegalStateException("checkpoint download target already exists: " + target);
        }

        final Path manifestObject = resolveObjectPath(resource.objectKey(), "manifest object key");
        rejectSymbolicPath(root, manifestObject, "checkpoint manifest object");
        verifyManifestObject(manifestObject, manifest, resource);
        final Path prefix = root.resolve("checkpoints")
                .resolve(Bytes.hex(manifest.recoveryLineageId()))
                .resolve(Bytes.hex(manifest.checkpointId()))
                .normalize();
        ensureWithinRoot(prefix);
        final Path temporary = targetParent
                .resolve("." + target.getFileName() + ".checkpoint-download-" + UUID.randomUUID())
                .normalize();
        ensureWithin(targetParent, temporary);
        boolean published = false;
        try {
            ensureDirectory(temporary);
            for (CheckpointManifest.FileEntry file : manifest.files()) {
                final Path source = prefix.resolve("objects")
                        .resolve(FilesystemCheckpointUploadAdapter.objectFileName(
                                file.objectKey(), file.objectVersion()))
                        .normalize();
                ensureWithinRoot(source);
                rejectSymbolicPath(root, source, "checkpoint object");
                final Path destination = temporary.resolve(file.name()).normalize();
                ensureWithin(temporary, destination);
                final Path parent = Objects.requireNonNull(destination.getParent(), "checkpoint file parent");
                ensureDirectory(parent);
                final Path fileTemporary = Files.createTempFile(parent, ".checkpoint-file-", ".tmp");
                boolean moved = false;
                try {
                    copyAndHash(root, source, fileTemporary, file.length(), file.checksum());
                    moveCreateNew(fileTemporary, destination);
                    moved = true;
                } finally {
                    if (!moved) {
                        Files.deleteIfExists(fileTemporary);
                    }
                }
            }
            final var actual = CheckpointFileInventory.collect(temporary, limits);
            if (actual.size() != manifest.files().size()) {
                throw new IllegalStateException("downloaded checkpoint inventory differs from manifest");
            }
            for (int index = 0; index < actual.size(); index++) {
                final var left = actual.get(index);
                final var right = manifest.files().get(index);
                if (!left.name().equals(right.name())
                        || left.length() != right.length()
                        || !Bytes.constantTimeEquals(left.checksum(), right.checksum())) {
                    throw new IllegalStateException(
                            "downloaded checkpoint inventory differs from manifest: " + left.name());
                }
            }
            forceDirectory(temporary);
            moveCreateNew(temporary, target);
            forceDirectory(targetParent);
            published = true;
            return target;
        } catch (IOException failure) {
            throw new IllegalStateException("cannot download checkpoint", failure);
        } finally {
            if (!published) {
                deleteTreeSafely(temporary);
            }
        }
    }

    private void verifyManifestObject(
            final Path manifestObject, final CheckpointManifest expected, final CheckpointResource resource) {
        try {
            final byte[] bytes = LocalStatePathGuard.readRegularFileNoFollow(
                    manifestObject, limits.maxManifestBytes(), "checkpoint manifest object");
            if (bytes == null
                    || bytes.length != resource.manifestLength()
                    || !Bytes.constantTimeEquals(Bytes.sha256(bytes), resource.manifestSha256())) {
                throw new IllegalStateException("checkpoint manifest object identity does not match catalog");
            }
            final CheckpointManifest downloaded = CheckpointManifest.decodeCanonicalJson(bytes, limits);
            if (!java.util.Arrays.equals(bytes, downloaded.canonicalJsonBytes())
                    || !java.util.Arrays.equals(bytes, expected.canonicalJsonBytes())) {
                throw new IllegalStateException("checkpoint manifest object is not the catalog manifest");
            }
        } catch (IOException | RuntimeException failure) {
            if (failure instanceof IllegalStateException state) {
                throw state;
            }
            throw new IllegalStateException("cannot verify checkpoint manifest object", failure);
        }
    }

    private Path resolveObjectPath(final byte[] encodedKey, final String description) {
        Objects.requireNonNull(encodedKey, description);
        final String key = new String(encodedKey, StandardCharsets.UTF_8);
        if (!java.util.Arrays.equals(encodedKey, Bytes.utf8(key))
                || key.isEmpty()
                || key.indexOf('\0') >= 0
                || key.indexOf('\\') >= 0
                || key.startsWith("/")
                || key.contains("..")) {
            throw new IllegalArgumentException(description + " is not a canonical relative UTF-8 path");
        }
        final Path resolved = root.resolve(key).normalize();
        ensureWithinRoot(resolved);
        return resolved;
    }

    private void ensureWithinRoot(final Path path) {
        ensureWithin(root, path);
    }

    private static void ensureWithin(final Path parent, final Path child) {
        final Path normalizedParent = parent.toAbsolutePath().normalize();
        final Path normalizedChild = child.toAbsolutePath().normalize();
        if (!normalizedChild.startsWith(normalizedParent)) {
            throw new IllegalArgumentException("checkpoint path escapes its boundary: " + child);
        }
    }

    private static Path normalizeRoot(final Path value) {
        Objects.requireNonNull(value, "root");
        final Path normalized = value.toAbsolutePath().normalize();
        if (Files.isSymbolicLink(normalized)) {
            throw new IllegalArgumentException("checkpoint Object Store root must not be a symbolic link");
        }
        return normalized;
    }

    private static Path normalizeTarget(final Path value) {
        Objects.requireNonNull(value, "targetDirectory");
        final Path normalized = value.toAbsolutePath().normalize();
        if (normalized.getFileName() == null || Files.isSymbolicLink(normalized)) {
            throw new IllegalArgumentException("checkpoint download target must be a real directory path");
        }
        return normalized;
    }

    private static void ensureDirectory(final Path directory) {
        try {
            LocalStatePathGuard.ensureRealDirectoryPath(directory, "checkpoint download directory");
            if (Files.isSymbolicLink(directory) || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)) {
                throw new IllegalArgumentException("checkpoint download path is not a real directory: " + directory);
            }
        } catch (IOException failure) {
            throw new IllegalStateException("cannot create checkpoint download directory", failure);
        }
    }

    private static void rejectSymbolicPath(final Path boundary, final Path path, final String description) {
        final Path absoluteBoundary = boundary.toAbsolutePath().normalize();
        final Path absolute = path.toAbsolutePath().normalize();
        if (!absolute.startsWith(absoluteBoundary)) {
            throw new IllegalArgumentException(description + " escapes its boundary: " + path);
        }
        if (Files.isSymbolicLink(absoluteBoundary)) {
            throw new IllegalStateException(description + " boundary is a symbolic link: " + absoluteBoundary);
        }
        Path cursor = absoluteBoundary;
        for (Path component : absoluteBoundary.relativize(absolute)) {
            cursor = cursor.resolve(component);
            if (Files.isSymbolicLink(cursor)) {
                throw new IllegalStateException(description + " contains a symbolic link: " + cursor);
            }
        }
    }

    private static void copyAndHash(
            final Path boundary,
            final Path source,
            final Path destination,
            final long expectedLength,
            final byte[] expectedChecksum)
            throws IOException {
        rejectSymbolicPath(boundary, source, "checkpoint object");
        if (!Files.isRegularFile(source, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("checkpoint object is not a regular file: " + source);
        }
        final java.security.MessageDigest digest;
        try {
            digest = java.security.MessageDigest.getInstance("SHA-256");
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
        long length = 0;
        try (FileChannel input = FileChannel.open(source, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
                FileChannel output =
                        FileChannel.open(destination, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
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
            throw new IllegalStateException("checkpoint object bytes differ from manifest");
        }
    }

    private static void moveCreateNew(final Path source, final Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException unsupported) {
            throw new IOException("checkpoint restore requires atomic rename", unsupported);
        } catch (FileAlreadyExistsException conflict) {
            throw new IllegalStateException(
                    "checkpoint restore target appeared during publication: " + target, conflict);
        }
    }

    private static void forceDirectory(final Path directory) throws IOException {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void deleteTreeSafely(final Path root) {
        if (root == null || !Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException failure) {
                    throw new IllegalStateException("cannot clean failed checkpoint download: " + root, failure);
                }
            });
        } catch (IOException failure) {
            throw new IllegalStateException("cannot enumerate failed checkpoint download: " + root, failure);
        }
    }
}
