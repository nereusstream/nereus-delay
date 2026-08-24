package com.nereusstream.delay.transport;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Raw TCP relay that holds real Broker responses without closing the channel. */
public final class KafkaClientArtifactHalfOpenProxy {
    private KafkaClientArtifactHalfOpenProxy() {}

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 11) {
            throw new IllegalArgumentException("usage: <listen-port> <target-host> <target-port> "
                    + "<hold-file> <release-file> <stop-file> <ready-file> <hold-ack-file> "
                    + "<release-observed-file> <post-release-forward-file> <channel-deadline-ms>");
        }
        final int listenPort = port(arguments[0], "listen-port");
        final String targetHost = requireText(arguments[1], "target-host");
        final int targetPort = port(arguments[2], "target-port");
        final Path holdFile = Path.of(arguments[3]);
        final Path releaseFile = Path.of(arguments[4]);
        final Path stopFile = Path.of(arguments[5]);
        final Path readyFile = Path.of(arguments[6]);
        final Path holdAckFile = Path.of(arguments[7]);
        final Path releaseObservedFile = Path.of(arguments[8]);
        final Path postReleaseForwardFile = Path.of(arguments[9]);
        final long channelDeadlineMs = positiveLong(arguments[10], "channel-deadline-ms");
        final AtomicBoolean stopped = new AtomicBoolean();
        final AtomicBoolean releaseRecorded = new AtomicBoolean();
        final AtomicBoolean postReleaseForwardRecorded = new AtomicBoolean();
        final AtomicLong holdStartedNanos = new AtomicLong();
        final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
            final Thread thread = new Thread(runnable, "nereus-delay-kafka-half-open-proxy");
            thread.setDaemon(true);
            return thread;
        });
        try (ServerSocket server = new ServerSocket()) {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", listenPort));
            writeMarker(readyFile, "listening=" + listenPort + " target=" + targetHost + ":" + targetPort + "\n");
            executor.submit(() -> watchFiles(
                    server,
                    stopped,
                    sockets,
                    holdFile,
                    releaseFile,
                    stopFile,
                    releaseObservedFile,
                    holdStartedNanos,
                    releaseRecorded,
                    channelDeadlineMs));
            while (!stopped.get()) {
                final Socket client;
                try {
                    client = server.accept();
                } catch (IOException closed) {
                    if (stopped.get()) {
                        break;
                    }
                    throw closed;
                }
                executor.submit(() -> relay(
                        client,
                        targetHost,
                        targetPort,
                        holdFile,
                        releaseFile,
                        holdAckFile,
                        postReleaseForwardFile,
                        holdStartedNanos,
                        postReleaseForwardRecorded,
                        sockets));
            }
        } finally {
            stopped.set(true);
            closeAll(sockets);
            executor.shutdownNow();
            try {
                Files.deleteIfExists(readyFile);
            } catch (IOException ignored) {
                // The harness owns cleanup of its temporary directory.
            }
        }
        System.out.println("Kafka half-open proxy stopped");
    }

    private static void relay(
            final Socket client,
            final String targetHost,
            final int targetPort,
            final Path holdFile,
            final Path releaseFile,
            final Path holdAckFile,
            final Path postReleaseForwardFile,
            final AtomicLong holdStartedNanos,
            final AtomicBoolean postReleaseForwardRecorded,
            final Set<Socket> sockets) {
        sockets.add(client);
        try (Socket target = new Socket()) {
            target.connect(new InetSocketAddress(targetHost, targetPort), 10_000);
            sockets.add(target);
            final Thread clientToTarget =
                    new Thread(() -> copy(client, target), "nereus-delay-kafka-half-open-client-to-target");
            final Thread targetToClient = new Thread(
                    () -> copyTargetToClient(
                            target,
                            client,
                            holdFile,
                            releaseFile,
                            holdAckFile,
                            postReleaseForwardFile,
                            holdStartedNanos,
                            postReleaseForwardRecorded),
                    "nereus-delay-kafka-half-open-target-to-client");
            clientToTarget.setDaemon(true);
            targetToClient.setDaemon(true);
            clientToTarget.start();
            targetToClient.start();
            while (clientToTarget.isAlive() && targetToClient.isAlive()) {
                clientToTarget.join(100);
                targetToClient.join(100);
            }
        } catch (IOException failure) {
            if (!stopped(holdFile, releaseFile)) {
                System.err.println("Kafka half-open proxy relay failed: " + failure.getMessage());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            sockets.remove(client);
            close(client);
        }
    }

    private static void copyTargetToClient(
            final Socket target,
            final Socket client,
            final Path holdFile,
            final Path releaseFile,
            final Path holdAckFile,
            final Path postReleaseForwardFile,
            final AtomicLong holdStartedNanos,
            final AtomicBoolean postReleaseForwardRecorded) {
        boolean held = false;
        try {
            final InputStream input = target.getInputStream();
            final OutputStream output = client.getOutputStream();
            final byte[] buffer = new byte[16 * 1024];
            while (true) {
                if (Files.exists(releaseFile) && postReleaseForwardRecorded.compareAndSet(false, true)) {
                    writeMarker(postReleaseForwardFile, "forwarded_after_release=true\n");
                }
                if (Files.exists(holdFile) && !Files.exists(releaseFile)) {
                    held = true;
                    final long started =
                            holdStartedNanos.updateAndGet(previous -> previous == 0 ? System.nanoTime() : previous);
                    writeMarker(
                            holdAckFile,
                            "hold_active=true\n" + "hold_started_epoch_ms="
                                    + (System.currentTimeMillis() - elapsedMs(started)) + "\n");
                    Thread.sleep(25);
                    continue;
                }
                if (held && postReleaseForwardRecorded.compareAndSet(false, true)) {
                    writeMarker(postReleaseForwardFile, "forwarded_after_release=true\n");
                }
                final int read = input.read(buffer);
                if (read < 0) {
                    return;
                }
                if (read > 0) {
                    output.write(buffer, 0, read);
                    output.flush();
                }
            }
        } catch (IOException ignored) {
            // Closing either side is the normal relay termination path.
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            close(target);
            close(client);
        }
    }

    private static void watchFiles(
            final ServerSocket server,
            final AtomicBoolean stopped,
            final Set<Socket> sockets,
            final Path holdFile,
            final Path releaseFile,
            final Path stopFile,
            final Path releaseObservedFile,
            final AtomicLong holdStartedNanos,
            final AtomicBoolean releaseRecorded,
            final long channelDeadlineMs) {
        while (!stopped.get()) {
            try {
                if (Files.exists(stopFile)) {
                    stopped.set(true);
                    closeAll(sockets);
                    server.close();
                    return;
                }
                final long started = holdStartedNanos.get();
                if (started > 0 && Files.exists(releaseFile) && releaseRecorded.compareAndSet(false, true)) {
                    writeMarker(
                            releaseObservedFile,
                            "release_observed=true\n"
                                    + "hold_duration_ms=" + elapsedMs(started) + "\n"
                                    + "channel_deadline_ms=" + channelDeadlineMs + "\n");
                }
                Thread.sleep(25);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException failure) {
                if (!stopped.get()) {
                    System.err.println("Kafka half-open proxy watcher failed: " + failure.getMessage());
                }
                return;
            }
        }
    }

    private static boolean stopped(final Path holdFile, final Path releaseFile) {
        return !Files.exists(holdFile) || Files.exists(releaseFile);
    }

    private static long elapsedMs(final long startedNanos) {
        return Math.max(0, (System.nanoTime() - startedNanos) / 1_000_000);
    }

    private static void copy(final Socket source, final Socket target) {
        try {
            final InputStream input = source.getInputStream();
            final OutputStream output = target.getOutputStream();
            final byte[] buffer = new byte[16 * 1024];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read > 0) {
                    output.write(buffer, 0, read);
                    output.flush();
                }
            }
        } catch (IOException ignored) {
            // Closing either side is the normal relay termination path.
        } finally {
            close(source);
            close(target);
        }
    }

    private static void closeAll(final Set<Socket> sockets) {
        for (Socket socket : sockets.toArray(Socket[]::new)) {
            close(socket);
        }
        sockets.clear();
    }

    private static void close(final Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // Best-effort fault injection and cleanup.
        }
    }

    private static void writeMarker(final Path path, final String content) {
        try {
            final Path parent = path.toAbsolutePath().normalize().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(path, content);
        } catch (IOException failure) {
            throw new IllegalStateException("Kafka half-open proxy could not write marker " + path, failure);
        }
    }

    private static int port(final String value, final String name) {
        try {
            final int result = Integer.parseInt(value);
            if (result <= 0 || result > 65_535) {
                throw new IllegalArgumentException(name + " must be 1..65535");
            }
            return result;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(name + " must be an integer", failure);
        }
    }

    private static long positiveLong(final String value, final String name) {
        try {
            final long result = Long.parseLong(value);
            if (result <= 0) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return result;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(name + " must be an integer", failure);
        }
    }

    private static String requireText(final String value, final String name) {
        if (value == null || value.isBlank() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }
}
