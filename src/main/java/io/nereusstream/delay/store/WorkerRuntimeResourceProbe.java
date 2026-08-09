package io.nereusstream.delay.store;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Fail-closed probe for the runtime limits required by the Worker envelope.
 *
 * <p>The probe intentionally supports the Linux procfs/cgroup layouts used by
 * the service runtime.  It does not infer an unlimited value when a file is
 * absent, contains {@code max}, or reports a kernel unlimited sentinel.  The
 * package-private overloads and parsers are deterministic seams for tests and
 * for a platform adapter that supplies equivalent authoritative files.</p>
 */
public final class WorkerRuntimeResourceProbe {
    private static final Path PROC_STATUS = Path.of("/proc/self/status");
    private static final Path PROC_LIMITS = Path.of("/proc/self/limits");
    private static final List<Path> CGROUP_MEMORY_LIMITS = List.of(
            Path.of("/sys/fs/cgroup/memory.max"),
            Path.of("/sys/fs/cgroup/memory/memory.limit_in_bytes"));
    private static final long CGROUP_UNLIMITED_THRESHOLD = 1L << 60;

    private WorkerRuntimeResourceProbe() {
    }

    /** Reads the current JVM, procfs, cgroup and root filesystem limits. */
    public static WorkerRuntimeResourceObservation observe(final Path rootPath) {
        Objects.requireNonNull(rootPath, "rootPath");
        return observe(rootPath, PROC_STATUS, PROC_LIMITS, CGROUP_MEMORY_LIMITS,
                ManagementFactory.getRuntimeMXBean());
    }

    static WorkerRuntimeResourceObservation observe(final Path rootPath,
                                                    final Path procStatus,
                                                    final Path procLimits,
                                                    final List<Path> cgroupMemoryLimits,
                                                    final RuntimeMXBean runtimeMxBean) {
        Objects.requireNonNull(rootPath, "rootPath");
        Objects.requireNonNull(procStatus, "procStatus");
        Objects.requireNonNull(procLimits, "procLimits");
        Objects.requireNonNull(cgroupMemoryLimits, "cgroupMemoryLimits");
        Objects.requireNonNull(runtimeMxBean, "runtimeMxBean");
        final long maxHeap = Runtime.getRuntime().maxMemory();
        final long maxDirect = readMaxDirectMemory(runtimeMxBean);
        final long rss = readProcessRss(procStatus);
        final long cgroup = readCgroupMemoryLimit(cgroupMemoryLimits);
        final long openFiles = readMaxProcessOpenFiles(procLimits);
        final long filesystemBytes;
        final long usableBytes;
        try {
            final FileStore fileStore = Files.getFileStore(rootPath);
            filesystemBytes = positive(fileStore.getTotalSpace(), "filesystem capacity");
            usableBytes = positive(fileStore.getUsableSpace(), "filesystem usable space");
        } catch (IOException exception) {
            throw new IllegalStateException("cannot inspect filesystem for " + rootPath, exception);
        }
        return new WorkerRuntimeResourceObservation(maxHeap, maxDirect, rss, cgroup, openFiles,
                filesystemBytes, usableBytes);
    }

    static long readMaxDirectMemory(final RuntimeMXBean runtimeMxBean) {
        for (String argument : runtimeMxBean.getInputArguments()) {
            final String prefix = "-XX:MaxDirectMemorySize=";
            if (argument.startsWith(prefix)) {
                final long value = parseByteSize(argument.substring(prefix.length()), "MaxDirectMemorySize");
                if (value <= 0) {
                    throw new IllegalStateException("MaxDirectMemorySize is zero or unknown");
                }
                return value;
            }
        }
        throw new IllegalStateException("MaxDirectMemorySize is not explicitly bounded");
    }

    static long readProcessRss(final Path procStatus) {
        final String contents = readText(procStatus, "process status");
        for (String line : contents.split("\\R")) {
            if (line.startsWith("VmRSS:")) {
                return parseKernelMeasurement(line.substring("VmRSS:".length()), "VmRSS");
            }
        }
        throw new IllegalStateException("VmRSS is missing from " + procStatus);
    }

    static long readMaxProcessOpenFiles(final Path procLimits) {
        final String contents = readText(procLimits, "process limits");
        for (String line : contents.split("\\R")) {
            if (line.startsWith("Max open files")) {
                final String[] fields = line.substring("Max open files".length()).trim().split("\\s+");
                if (fields.length < 2 || fields[0].equalsIgnoreCase("unlimited")) {
                    throw new IllegalStateException("Max open files is unlimited or malformed");
                }
                return positive(parseUnsignedDecimal(fields[0], "Max open files"), "Max open files");
            }
        }
        throw new IllegalStateException("Max open files is missing from " + procLimits);
    }

    static long readCgroupMemoryLimit(final List<Path> candidates) {
        for (Path candidate : candidates) {
            if (Files.exists(candidate)) {
                final String value = readText(candidate, "cgroup memory limit").trim();
                return parseCgroupMemoryLimit(value);
            }
        }
        throw new IllegalStateException("no supported cgroup memory limit file is available");
    }

    static long parseCgroupMemoryLimit(final String value) {
        final String normalized = Objects.requireNonNull(value, "value").trim();
        if (normalized.isEmpty() || normalized.equalsIgnoreCase("max")) {
            throw new IllegalStateException("cgroup memory limit is unlimited or missing");
        }
        final long parsed = parseUnsignedDecimal(normalized, "cgroup memory limit");
        if (parsed >= CGROUP_UNLIMITED_THRESHOLD) {
            throw new IllegalStateException("cgroup memory limit is an unlimited sentinel");
        }
        return positive(parsed, "cgroup memory limit");
    }

    static long parseKernelMeasurement(final String value, final String field) {
        final String[] fields = Objects.requireNonNull(value, field).trim().split("\\s+");
        if (fields.length == 0 || fields[0].isEmpty()) {
            throw new IllegalArgumentException(field + " is empty");
        }
        final String encoded;
        if (fields.length == 1) {
            encoded = fields[0];
        } else {
            final String unit = fields[1].toLowerCase(Locale.ROOT);
            encoded = fields[0] + (unit.endsWith("b") ? unit.substring(0, unit.length() - 1) : unit);
        }
        return positive(parseByteSize(encoded, field), field);
    }

    static long parseByteSize(final String value, final String field) {
        String normalized = Objects.requireNonNull(value, field).trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException(field + " is empty");
        }
        if (normalized.endsWith("kib") || normalized.endsWith("mib") || normalized.endsWith("gib")
                || normalized.endsWith("tib")) {
            normalized = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("kb") || normalized.endsWith("mb") || normalized.endsWith("gb")
                || normalized.endsWith("tb")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        final int suffixIndex = normalized.length() - 1;
        final char suffix = normalized.charAt(suffixIndex);
        final long multiplier;
        final String digits;
        switch (suffix) {
            case 'k' -> {
                multiplier = 1024L;
                digits = normalized.substring(0, suffixIndex);
            }
            case 'm' -> {
                multiplier = 1024L * 1024;
                digits = normalized.substring(0, suffixIndex);
            }
            case 'g' -> {
                multiplier = 1024L * 1024 * 1024;
                digits = normalized.substring(0, suffixIndex);
            }
            case 't' -> {
                multiplier = 1024L * 1024 * 1024 * 1024;
                digits = normalized.substring(0, suffixIndex);
            }
            default -> {
                multiplier = 1;
                digits = normalized;
            }
        }
        final long parsed = parseUnsignedDecimal(digits, field);
        try {
            return Math.multiplyExact(parsed, multiplier);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(field + " overflows", overflow);
        }
    }

    private static long parseUnsignedDecimal(final String value, final String field) {
        try {
            if (value.isEmpty() || value.charAt(0) == '-') {
                throw new NumberFormatException("negative or empty");
            }
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(field + " is not a bounded decimal", exception);
        }
    }

    private static String readText(final Path path, final String description) {
        try {
            return Files.readString(path);
        } catch (IOException exception) {
            throw new IllegalStateException("cannot read " + description + " from " + path, exception);
        }
    }

    private static long positive(final long value, final String field) {
        if (value <= 0) {
            throw new IllegalStateException(field + " is unknown or non-positive");
        }
        return value;
    }
}
