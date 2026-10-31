package com.nereusstream.delay.ownership;

import com.nereusstream.delay.adapter.BoundedDestinationPublishAdapter;
import com.nereusstream.delay.adapter.DestinationPhysicalAdmission;
import com.nereusstream.delay.adapter.TargetDestinationMux;
import com.nereusstream.delay.protocol.CapacityDimension;
import java.text.Normalizer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Original process-owned finite routing/physical window; no Target, credential or SEND authority. */
public final class TargetWorkerPublicationDispatch {
    public record Cluster(long requests, long bytes) {
        public Cluster {
            finite(requests);
            finite(bytes);
        }
    }

    public record Configuration(int maximumChannels, long requests, long bytes, Map<String, Cluster> clusters) {
        public Configuration {
            finite(requests);
            finite(bytes);
            Objects.requireNonNull(clusters);
            if (maximumChannels < 1 || maximumChannels == Integer.MAX_VALUE || clusters.isEmpty()) {
                throw new IllegalArgumentException(
                        "publication dispatch requires finite channels and explicit clusters");
            }
            for (var entry : clusters.entrySet()) {
                final var id = Objects.requireNonNull(entry.getKey());
                final var limit = Objects.requireNonNull(entry.getValue());
                if (id.isBlank()
                        || id.length() > 1024
                        || id.indexOf('\0') >= 0
                        || !Normalizer.isNormalized(id, Normalizer.Form.NFC)
                        || limit.requests() > requests
                        || limit.bytes() > bytes) {
                    throw new IllegalArgumentException(
                            "publication cluster exceeds its original identity/physical bound");
                }
            }
            clusters = Map.copyOf(clusters);
        }

        /** Closed literal schema, also used by the existing bounded UTF-8 deployment-file loader. */
        public static Configuration load(Map<String, String> supplied) {
            final var fields = new HashMap<>(supplied);
            final int channels = Math.toIntExact(number(fields, "maximumChannels"));
            final long requests = number(fields, "requests"), bytes = number(fields, "bytes");
            final int count = Math.toIntExact(number(fields, "clusterCount"));
            if (count < 1 || count > fields.size() / 3) {
                throw new IllegalArgumentException("publication cluster list exceeds its finite configuration");
            }
            final var clusters = new HashMap<String, Cluster>();
            for (int i = 0; i < count; i++) {
                final String prefix = "cluster." + i + '.';
                final String id = Objects.requireNonNull(fields.remove(prefix + "id"), "missing cluster id");
                final var limit = new Cluster(number(fields, prefix + "requests"), number(fields, prefix + "bytes"));
                if (clusters.putIfAbsent(id, limit) != null) {
                    throw new IllegalArgumentException("duplicate publication cluster");
                }
            }
            if (!fields.isEmpty()) {
                throw new IllegalArgumentException("unknown publication dispatch fields: " + fields.keySet());
            }
            return new Configuration(channels, requests, bytes, clusters);
        }

        private static long number(Map<String, String> fields, String name) {
            final String value = Objects.requireNonNull(fields.remove(name), "missing dispatch bound: " + name);
            if (!value.matches("[0-9]{1,19}")) {
                throw new IllegalArgumentException("dispatch bound must be a finite decimal");
            }
            final long result = Long.parseLong(value);
            finite(result);
            return result;
        }
    }

    private final Configuration configuration;
    private final DestinationPhysicalAdmission physical;
    private final TargetDestinationMux mux;
    private final BoundedDestinationPublishAdapter adapter;
    private final com.nereusstream.delay.scheduler.WorkClassExecutionRegistry classes;
    private final com.nereusstream.delay.store.SharedRocksDbResources resources;
    private boolean closed;

    TargetWorkerPublicationDispatch(
            Configuration configuration,
            TargetWorkerProcessRuntime.Assembly graph,
            com.nereusstream.delay.scheduler.BoundedAsyncMetricExporter metrics) {
        this.configuration = Objects.requireNonNull(configuration);
        classes = graph.classes();
        resources = graph.resources();
        final var worker = graph.config();
        worker.requireRuntime(graph.classes(), graph.resources());
        for (var dimension :
                java.util.List.of(CapacityDimension.PHYSICAL_REQUESTS, CapacityDimension.ZOMBIE_REQUESTS)) {
            if (Math.addExact(configuration.requests(), worker.systemWriter().records())
                    > worker.hardCaps().amount(dimension)) {
                throw new IllegalArgumentException(
                        "publication requests consume the independent Source writer reserve");
            }
        }
        for (var dimension : java.util.List.of(CapacityDimension.PHYSICAL_BYTES, CapacityDimension.ZOMBIE_BYTES)) {
            if (Math.addExact(configuration.bytes(), worker.systemWriter().bytes())
                    > worker.hardCaps().amount(dimension)) {
                throw new IllegalArgumentException("publication bytes consume the independent Source writer reserve");
            }
        }
        physical = new DestinationPhysicalAdmission(configuration.requests(), configuration.bytes());
        configuration
                .clusters()
                .forEach((id, limit) -> physical.registerTargetCluster(id, limit.requests(), limit.bytes()));
        mux = new TargetDestinationMux(configuration.maximumChannels());
        adapter = new BoundedDestinationPublishAdapter(mux, physical, graph.classes(), graph.io(), metrics);
    }

    void requireConfiguration(Configuration expected) {
        if (closed || !configuration.equals(expected)) {
            throw new IllegalStateException("original publication dispatch cannot be replaced");
        }
    }

    public DestinationPhysicalAdmission physical() {
        return physical;
    }

    public TargetDestinationMux mux() {
        return mux;
    }

    public BoundedDestinationPublishAdapter adapter() {
        return adapter;
    }

    public void requireWorker(TargetWorkerShardRuntime expected) {
        expected.requireFleetComposition(classes, resources);
    }

    /** Called only after this original process has proved its Host/SDK/Store dependencies exited. */
    synchronized boolean closeIfDrained() {
        if (closed) {
            return true;
        }
        final var usage = physical.workerSnapshot();
        if (!mux.empty()
                || usage.activeRequests() != 0
                || usage.activeBytes() != 0
                || usage.protectedReadyRequests() != 0
                || usage.protectedReadyBytes() != 0) {
            return false;
        }
        adapter.close();
        closed = true;
        return true;
    }

    private static void finite(long value) {
        if (value < 1 || value == Long.MAX_VALUE) {
            throw new IllegalArgumentException("publication bound must be finite and positive");
        }
    }
}
