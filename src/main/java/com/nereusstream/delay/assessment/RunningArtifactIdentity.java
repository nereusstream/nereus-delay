package com.nereusstream.delay.assessment;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;
import java.util.regex.Pattern;

/** Immutable source identity generated into the exact runnable artifact by the build. */
public final class RunningArtifactIdentity {
    private static final String RESOURCE = "META-INF/nereus-delay-source-identity.properties";
    private static final Pattern COMMIT = Pattern.compile("[0-9a-f]{40}");

    private RunningArtifactIdentity() {}

    /** Returns the exact clean source commit or fails closed for dirty or unbound artifacts. */
    public static String requireCleanSourceCommit() throws IOException {
        return read(RunningArtifactIdentity.class.getClassLoader()).requireCleanCommit();
    }

    static Identity read(final ClassLoader classLoader) throws IOException {
        final Properties properties = new Properties();
        try (InputStream input =
                Objects.requireNonNull(classLoader, "classLoader").getResourceAsStream(RESOURCE)) {
            if (input == null) {
                throw new IOException("running artifact has no generated source identity");
            }
            properties.load(input);
        }
        if (properties.size() != 2) {
            throw new IOException("running artifact source identity has unknown or missing fields");
        }
        final String commit = properties.getProperty("sourceCommit");
        if (commit == null || !COMMIT.matcher(commit).matches()) {
            throw new IOException("running artifact source commit is not canonical");
        }
        final String cleanText = properties.getProperty("trackedSourceClean");
        final boolean clean;
        if ("true".equals(cleanText)) {
            clean = true;
        } else if ("false".equals(cleanText)) {
            clean = false;
        } else {
            throw new IOException("running artifact source cleanliness is not a closed boolean");
        }
        return new Identity(commit, clean);
    }

    record Identity(String sourceCommit, boolean trackedSourceClean) {
        Identity {
            Objects.requireNonNull(sourceCommit, "sourceCommit");
        }

        String requireCleanCommit() throws IOException {
            if (!trackedSourceClean) {
                throw new IOException("persistent activation rejects an artifact built from tracked dirty sources");
            }
            return sourceCommit;
        }
    }
}
