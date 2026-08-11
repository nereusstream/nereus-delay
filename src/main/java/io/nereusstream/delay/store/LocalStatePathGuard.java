package io.nereusstream.delay.store;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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
final class LocalStatePathGuard {
    private LocalStatePathGuard() {
    }

    static void ensureRealDirectoryPath(final Path path, final String description) throws IOException {
        Objects.requireNonNull(path, "path");
        Objects.requireNonNull(description, "description");
        final Path absolute = path.toAbsolutePath().normalize();
        final List<Path> missing = new ArrayList<>();
        Path cursor = absolute;
        while (!Files.exists(cursor, LinkOption.NOFOLLOW_LINKS)) {
            missing.add(cursor);
            cursor = cursor.getParent();
            if (cursor == null) {
                throw new IOException(description + " has no existing ancestor: " + path);
            }
        }
        requireRealDirectory(cursor, description);
        for (int index = missing.size() - 1; index >= 0; index--) {
            cursor = missing.get(index);
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

    private static void requireRealDirectory(final Path path, final String description) throws IOException {
        if (Files.isSymbolicLink(path)
                || !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException(description + " must be a real directory: " + path);
        }
    }
}
