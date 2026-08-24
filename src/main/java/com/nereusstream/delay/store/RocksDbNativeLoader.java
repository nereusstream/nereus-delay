package com.nereusstream.delay.store;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import org.rocksdb.RocksDB;

/** Loads the JNI binary packaged with the selected rocksdbjni artifact. */
final class RocksDbNativeLoader {
    private static final AtomicBoolean LOADED = new AtomicBoolean();

    private RocksDbNativeLoader() {}

    static void load() {
        if (LOADED.get()) {
            return;
        }
        synchronized (LOADED) {
            if (LOADED.get()) {
                return;
            }
            final String resource = resourceName();
            final ClassLoader loader = RocksDbNativeLoader.class.getClassLoader();
            try (InputStream input = loader.getResourceAsStream(resource)) {
                if (input == null) {
                    throw new IllegalStateException("rocksdbjni resource is missing: " + resource);
                }
                final Path directory = Files.createTempDirectory("nereus-delay-rocksdb-");
                directory.toFile().deleteOnExit();
                final Path extracted = directory.resolve(jniFileName());
                Files.copy(input, extracted, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                extracted.toFile().deleteOnExit();
                RocksDB.loadLibrary(java.util.List.of(directory.toAbsolutePath().toString()));
                LOADED.set(true);
            } catch (IOException exception) {
                throw new ExceptionInInitializerError(exception);
            }
        }
    }

    private static String resourceName() {
        final String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        final String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        if (os.contains("mac") || os.contains("darwin")) {
            return arch.contains("aarch64") || arch.contains("arm64")
                    ? "librocksdbjni-osx-arm64.jnilib"
                    : "librocksdbjni-osx-x86_64.jnilib";
        }
        if (os.contains("win")) {
            return "librocksdbjni-win64.dll";
        }
        if (arch.contains("aarch64") || arch.contains("arm64")) {
            return "librocksdbjni-linux-aarch64.so";
        }
        return "librocksdbjni-linux64.so";
    }

    private static String jniFileName() {
        return resourceName().replaceFirst("librocksdbjni", "librocksdbjnijni");
    }
}
