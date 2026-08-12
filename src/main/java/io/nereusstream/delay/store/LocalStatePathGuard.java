package io.nereusstream.delay.store;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;

/**
 * Creates local crash-durable projection directories without following a
 * symlink in a caller-owned path component.
 *
 * <p>The nearest existing ancestor is treated as deployment-managed. Every
 * descendant created by this helper is checked as a real directory, including
 * after a concurrent {@link FileAlreadyExistsException}. This keeps local
 * state files and their temporary/lock siblings inside the intended physical
 * directory boundary.</p>
 */
public final class LocalStatePathGuard {
    private LocalStatePathGuard() {
    }

    public static void ensureRealDirectoryPath(final Path path, final String description) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(description, "description");
        final Path absolute = path.toAbsolutePath().normalize();
        final Path root = absolute.getRoot();
        if (root == null) {
            throw new IOException(description + " has no filesystem root: " + path);
        }
        Path cursor = root;
        requireRealDirectory(cursor, description);
        for (Path component : absolute) {
            cursor = cursor.resolve(component);
            if (Files.isSymbolicLink(cursor)) {
                final Path target;
                try {
                    target = cursor.toRealPath();
                } catch (IOException failure) {
                    throw new IOException(description + " contains an unresolved symbolic-link component: " + cursor,
                            failure);
                }
                final Path lexicalParent = cursor.getParent();
                if (lexicalParent == null || !target.startsWith(lexicalParent)) {
                    throw new IOException(description + " must not redirect outside its lexical parent: " + cursor);
                }
                if (!Files.isDirectory(target, LinkOption.NOFOLLOW_LINKS)) {
                    throw new IOException(description + " symbolic-link target must be a directory: " + cursor);
                }
                // This existing component is deployment-managed. Continue
                // checking descendants, but do not treat the symlink itself
                // as a newly-created state directory.
                continue;
            }
            if (!Files.exists(cursor, LinkOption.NOFOLLOW_LINKS)) {
                try {
                    Files.createDirectory(cursor);
                } catch (FileAlreadyExistsException racedCreate) {
                    // Re-check below; a concurrent creator is safe only when
                    // it installed a real directory rather than a symlink.
                }
            }
            requireRealDirectory(cursor, description);
        }
    }

    /**
     * Reads one bounded regular file through the same no-follow handle that
     * was opened for the read.  A missing path returns {@code null}; a raced
     * replacement, size change or short read fails closed.
     *
     * <p>The caller must hold any higher-level lock that serializes the
     * projection.  This helper only closes the check-then-open window for the
     * file itself; directory/path ownership is enforced by
     * {@link #ensureRealDirectoryPath(Path, String)}.</p>
     */
    public static byte[] readRegularFileNoFollow(final Path path, final long maxBytes,
                                                 final String description) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(description, "description");
        if (maxBytes < 0 || maxBytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("maxBytes must fit a non-negative byte-array length");
        }
        final Path absolute = path.toAbsolutePath().normalize();
        final FileChannel channel;
        try {
            channel = FileChannel.open(absolute, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
        } catch (NoSuchFileException missing) {
            if (Files.isSymbolicLink(absolute)) {
                throw new IOException(description + " must not be a symbolic link: " + path, missing);
            }
            return null;
        } catch (IOException failure) {
            if (Files.isSymbolicLink(absolute)) {
                throw new IOException(description + " must not be a symbolic link: " + path, failure);
            }
            throw failure;
        }
        try (channel) {
            if (Files.isSymbolicLink(absolute)
                    || !Files.isRegularFile(absolute, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException(description + " is not a regular file: " + path);
            }
            final long size = channel.size();
            if (size < 0 || size > maxBytes) {
                throw new IOException(description + " exceeds bounded size: " + path);
            }
            final ByteBuffer buffer = ByteBuffer.allocate((int) size);
            while (buffer.hasRemaining()) {
                final int read = channel.read(buffer);
                if (read < 0) {
                    throw new IOException(description + " changed while being read: " + path);
                }
            }
            if (channel.size() != size) {
                throw new IOException(description + " changed while being read: " + path);
            }
            return buffer.array();
        }
    }

    /**
     * Returns the size observed through one no-follow regular-file handle.
     * Missing, symbolic, non-regular or concurrently changing files fail
     * closed because a physical-usage probe cannot treat unknown bytes as
     * zero.
     */
    public static long sizeRegularFileNoFollow(final Path path, final String description) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(description, "description");
        final Path absolute = path.toAbsolutePath().normalize();
        final FileChannel channel;
        try {
            channel = FileChannel.open(absolute, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
        } catch (IOException failure) {
            if (Files.isSymbolicLink(absolute)) {
                throw new IOException(description + " must not be a symbolic link: " + path, failure);
            }
            throw failure;
        }
        try (channel) {
            if (Files.isSymbolicLink(absolute)
                    || !Files.isRegularFile(absolute, LinkOption.NOFOLLOW_LINKS)) {
                throw new IOException(description + " is not a regular file: " + path);
            }
            final long size = channel.size();
            if (size < 0 || channel.size() != size) {
                throw new IOException(description + " changed while being inspected: " + path);
            }
            return size;
        }
    }

    private static void requireRealDirectory(final Path path, final String description) throws IOException {
        if (Files.isSymbolicLink(path)
                || !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException(description + " must be a real directory: " + path);
        }
    }
}
