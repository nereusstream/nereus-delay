package com.nereusstream.delay.store;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalStatePathGuardTest {
    @TempDir
    Path tempDirectory;

    @Test
    void boundedReadUsesRealNoFollowFileAndMissingPathIsEmpty() throws Exception {
        final Path state = tempDirectory.resolve("state.bin");
        assertNull(LocalStatePathGuard.readRegularFileNoFollow(state, 32, "state"));

        final byte[] expected = new byte[] {1, 2, 3, 4};
        Files.write(state, expected);
        assertArrayEquals(expected, LocalStatePathGuard.readRegularFileNoFollow(state, expected.length, "state"));
        assertThrows(
                IOException.class,
                () -> LocalStatePathGuard.readRegularFileNoFollow(state, expected.length - 1, "state"));
    }

    @Test
    void boundedReadRejectsSymlinkAndDirectoryTargets() throws Exception {
        final Path target = tempDirectory.resolve("target.bin");
        Files.write(target, new byte[] {9});
        final Path link = tempDirectory.resolve("link.bin");
        Files.createSymbolicLink(link, target);
        assertThrows(IOException.class, () -> LocalStatePathGuard.readRegularFileNoFollow(link, 32, "state"));
        final Path dangling = tempDirectory.resolve("dangling.bin");
        Files.createSymbolicLink(dangling, tempDirectory.resolve("missing.bin"));
        assertThrows(IOException.class, () -> LocalStatePathGuard.readRegularFileNoFollow(dangling, 32, "state"));

        final Path directory = tempDirectory.resolve("directory");
        Files.createDirectory(directory);
        assertThrows(IOException.class, () -> LocalStatePathGuard.readRegularFileNoFollow(directory, 32, "state"));
    }

    @Test
    void directoryPathRejectsExistingIntermediateSymlinkEvenWhenTargetExists() throws Exception {
        final Path outside = tempDirectory.resolve("outside");
        Files.createDirectories(outside.resolve("nested"));
        final Path linked = tempDirectory.resolve("linked");
        Files.createSymbolicLink(linked, outside);

        assertThrows(
                IOException.class,
                () -> LocalStatePathGuard.ensureRealDirectoryPath(linked.resolve("nested"), "state parent"));
    }
}
