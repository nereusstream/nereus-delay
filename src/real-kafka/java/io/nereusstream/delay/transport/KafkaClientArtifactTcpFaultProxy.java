package io.nereusstream.delay.transport;

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

/**
 * Small raw TCP relay used by the focused Kafka endpoint-cut harness.
 *
 * <p>The relay is deliberately transport-only. It does not parse Kafka
 * frames, classify responses, or manufacture Broker evidence. Before the cut
 * file is created it forwards bytes to the real Broker-1 listener. While the
 * cut is active it closes existing sockets, rejects one new socket, and then
 * forwards later sockets to the explicitly supplied post-cut target. A release
 * file restores forwarding to Broker 1 so the harness can prove that the proxy
 * itself was the fault boundary.</p>
 */
public final class KafkaClientArtifactTcpFaultProxy {
    private KafkaClientArtifactTcpFaultProxy() {
    }

    public static void main(final String[] arguments) throws Exception {
        if (arguments.length != 13) {
            throw new IllegalArgumentException("usage: <listen-port> <target-host> <target-port> "
                    + "<post-cut-target-host> <post-cut-target-port> <cut-file> <release-file> "
                    + "<stop-file> <ready-file> <cut-ack-file> <pre-cut-connection-file> "
                    + "<post-cut-rejection-file> <post-cut-handoff-file>");
        }
        final int listenPort = port(arguments[0], "listen-port");
        final String targetHost = requireText(arguments[1], "target-host");
        final int targetPort = port(arguments[2], "target-port");
        final String postCutTargetHost = requireText(arguments[3], "post-cut-target-host");
        final int postCutTargetPort = port(arguments[4], "post-cut-target-port");
        final Path cutFile = Path.of(arguments[5]);
        final Path releaseFile = Path.of(arguments[6]);
        final Path stopFile = Path.of(arguments[7]);
        final Path readyFile = Path.of(arguments[8]);
        final Path cutAckFile = Path.of(arguments[9]);
        final Path preCutConnectionFile = Path.of(arguments[10]);
        final Path postCutRejectionFile = Path.of(arguments[11]);
        final Path postCutHandoffFile = Path.of(arguments[12]);
        final AtomicBoolean stopped = new AtomicBoolean();
        final AtomicBoolean postCutRejectionRecorded = new AtomicBoolean();
        final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
            final Thread thread = new Thread(runnable, "nereus-delay-kafka-tcp-proxy");
            thread.setDaemon(true);
            return thread;
        });
        try (ServerSocket server = new ServerSocket()) {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress("127.0.0.1", listenPort));
            writeMarker(readyFile, "listening=" + listenPort + " target=" + targetHost + ":" + targetPort
                    + " postCutTarget=" + postCutTargetHost + ":" + postCutTargetPort + "\n");
            executor.submit(() -> watchFiles(server, stopped, sockets, cutFile, releaseFile, stopFile,
                    cutAckFile));
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
                executor.submit(() -> relay(client, targetHost, targetPort, postCutTargetHost, postCutTargetPort,
                        cutFile, releaseFile, sockets, preCutConnectionFile, postCutRejectionFile,
                        postCutHandoffFile, postCutRejectionRecorded));
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
        System.out.println("Kafka raw TCP fault proxy stopped");
    }

    private static void relay(final Socket client, final String targetHost, final int targetPort,
                               final String postCutTargetHost, final int postCutTargetPort,
                               final Path cutFile, final Path releaseFile, final Set<Socket> sockets,
                               final Path preCutConnectionFile, final Path postCutRejectionFile,
                               final Path postCutHandoffFile, final AtomicBoolean postCutRejectionRecorded) {
        sockets.add(client);
        try {
            if (cutActive(cutFile, releaseFile)) {
                if (postCutRejectionRecorded.compareAndSet(false, true)) {
                    writeMarker(postCutRejectionFile, "rejected\n");
                    close(client);
                    return;
                }
                writeMarker(postCutHandoffFile, "forwarded-to=" + postCutTargetHost + ":"
                        + postCutTargetPort + "\n");
                relayToTarget(client, postCutTargetHost, postCutTargetPort, sockets);
                return;
            }
            writeMarker(preCutConnectionFile, "forwarded\n");
            relayToTarget(client, targetHost, targetPort, sockets);
        } catch (IOException failure) {
            if (!cutActive(cutFile, releaseFile)) {
                System.err.println("Kafka raw TCP fault proxy relay failed: " + failure.getMessage());
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        } finally {
            sockets.remove(client);
            close(client);
        }
    }

    private static void relayToTarget(final Socket client, final String targetHost, final int targetPort,
                                      final Set<Socket> sockets) throws IOException, InterruptedException {
        try (Socket target = new Socket()) {
            target.connect(new InetSocketAddress(targetHost, targetPort), 10_000);
            sockets.add(target);
            final Thread clientToTarget = new Thread(() -> copy(client, target),
                    "nereus-delay-kafka-tcp-client-to-target");
            final Thread targetToClient = new Thread(() -> copy(target, client),
                    "nereus-delay-kafka-tcp-target-to-client");
            clientToTarget.setDaemon(true);
            targetToClient.setDaemon(true);
            clientToTarget.start();
            targetToClient.start();
            while (clientToTarget.isAlive() && targetToClient.isAlive()) {
                clientToTarget.join(100);
                targetToClient.join(100);
            }
        }
    }

    private static void watchFiles(final ServerSocket server, final AtomicBoolean stopped,
                                   final Set<Socket> sockets, final Path cutFile, final Path releaseFile,
                                   final Path stopFile, final Path cutAckFile) {
        boolean acknowledged = false;
        while (!stopped.get()) {
            try {
                if (Files.exists(stopFile)) {
                    stopped.set(true);
                    closeAll(sockets);
                    server.close();
                    return;
                }
                if (cutActive(cutFile, releaseFile)) {
                    closeAll(sockets);
                    if (!acknowledged) {
                        writeMarker(cutAckFile, "cut-active\n");
                        acknowledged = true;
                    }
                } else {
                    acknowledged = false;
                }
                Thread.sleep(25);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException failure) {
                if (!stopped.get()) {
                    System.err.println("Kafka raw TCP fault proxy watcher failed: " + failure.getMessage());
                }
                return;
            }
        }
    }

    private static boolean cutActive(final Path cutFile, final Path releaseFile) {
        return Files.exists(cutFile) && !Files.exists(releaseFile);
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
            final Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(path, content);
        } catch (IOException failure) {
            throw new IllegalStateException("Kafka raw TCP proxy could not write marker " + path, failure);
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

    private static String requireText(final String value, final String name) {
        if (value == null || value.isBlank() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException(name + " must be non-blank");
        }
        return value;
    }
}
