package com.nereusstream.delay.assessment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.io.IOException;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RunningArtifactIdentityTest {
    private static final String COMMIT = "0123456789abcdef0123456789abcdef01234567";

    @TempDir
    Path tempDir;

    @Test
    void acceptsOnlyAnExactCleanGeneratedIdentity() throws Exception {
        assertEquals(
                COMMIT,
                read("sourceCommit=" + COMMIT + "\ntrackedSourceClean=true\n").requireCleanCommit());
        assertThrows(IOException.class, () -> read("sourceCommit=" + COMMIT + "\ntrackedSourceClean=false\n")
                .requireCleanCommit());
        assertThrows(IOException.class, () -> read("sourceCommit=" + COMMIT + "\ntrackedSourceClean=yes\n"));
        assertThrows(
                IOException.class, () -> read("sourceCommit=" + COMMIT + "\ntrackedSourceClean=true\nextra=value\n"));
    }

    private RunningArtifactIdentity.Identity read(final String value) throws Exception {
        final Path resource = tempDir.resolve("META-INF/nereus-delay-source-identity.properties");
        Files.createDirectories(resource.getParent());
        Files.writeString(resource, value, StandardCharsets.UTF_8);
        try (URLClassLoader loader =
                new URLClassLoader(new java.net.URL[] {tempDir.toUri().toURL()}, null)) {
            return RunningArtifactIdentity.read(loader);
        }
    }
}
