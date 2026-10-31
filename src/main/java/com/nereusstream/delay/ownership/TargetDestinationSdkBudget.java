package com.nereusstream.delay.ownership;

import com.nereusstream.delay.protocol.CapacityDimension;
import com.nereusstream.delay.protocol.CapacityVector;
import com.nereusstream.delay.protocol.ShardCapacityEnvelope;
import com.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import com.nereusstream.delay.store.NativeResourceUsage;
import com.nereusstream.delay.store.SharedRocksDbResources;
import com.nereusstream.delay.store.WorkerNativeResourceLedger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** One Worker-owned window for destination SDK generations, including opening and failed native cleanup. */
public final class TargetDestinationSdkBudget {
    private static final Set<CapacityDimension> DIMENSIONS = Set.of(
            CapacityDimension.OPEN_FILES,
            CapacityDimension.ADAPTER_CONNECTIONS,
            CapacityDimension.ADAPTER_PRODUCERS,
            CapacityDimension.ADAPTER_THREADS,
            CapacityDimension.BUFFERED_MESSAGES,
            CapacityDimension.BUFFERED_BYTES,
            CapacityDimension.DIRECT_BUFFER_BYTES,
            CapacityDimension.OTHER_NATIVE_BYTES);
    private final CapacityVector limit;
    private final int maximumClients;
    private final SharedRocksDbResources resources;
    private final WorkClassExecutionRegistry classes;
    private final Map<String, Client> clients = new HashMap<>();
    private CapacityVector used = CapacityVector.empty();

    /** Pool is a fixed Worker cost; committed Shard grants and other fixed SDK costs are not subtracted by guesswork. */
    public TargetDestinationSdkBudget(
            TargetWorkerRuntimeConfig config,
            List<ShardCapacityEnvelope> shards,
            CapacityVector otherFixedCost,
            CapacityVector transitionDemand,
            CapacityVector limit,
            int maximumClients,
            WorkClassExecutionRegistry classes,
            SharedRocksDbResources resources) {
        this.limit = finite(limit);
        this.maximumClients = maximumClients;
        this.classes = Objects.requireNonNull(classes, "actual Worker classes");
        this.resources = Objects.requireNonNull(resources, "actual Worker resources");
        if (maximumClients <= 0
                || maximumClients == Integer.MAX_VALUE
                || limit.amount(CapacityDimension.ADAPTER_CONNECTIONS) == 0
                || limit.amount(CapacityDimension.ADAPTER_THREADS) == 0
                || otherFixedCost.amount(CapacityDimension.ADAPTER_PRODUCERS)
                        < config.systemWriter().writers()) {
            throw new IllegalArgumentException(
                    "destination SDK pool requires finite clients and separate system-writer generations");
        }
        config.requireRuntime(classes, resources);
        config.requireCapacity(shards, otherFixedCost.add(limit), transitionDemand);
        resources.requireWorkerActivationReady();
        classes.bindWorkerSingleton(WorkClassExecutionRegistry.WorkerSingleton.DESTINATION_SDK_RESOURCES, this);
    }

    public void requireWorker(WorkClassExecutionRegistry expected, SharedRocksDbResources shared) {
        if (classes != expected || resources != shared) {
            throw new IllegalArgumentException("SDK pool belongs to another Worker resource graph");
        }
    }

    /** Reserve before constructing a client or enqueueing its opening task. No new thread or wait queue. */
    public synchronized Client reserve(String id, CapacityVector cost) {
        Objects.requireNonNull(id, "SDK allocation ID");
        finite(cost);
        resources.requireRuntimeBusinessAdmission();
        if (id.isBlank()
                || id.length() > 512
                || clients.containsKey(id)
                || clients.size() >= maximumClients
                || cost.amount(CapacityDimension.ADAPTER_CONNECTIONS) == 0
                        && cost.amount(CapacityDimension.ADAPTER_THREADS) == 0
                        && cost.amount(CapacityDimension.ADAPTER_PRODUCERS) == 0) {
            throw new IllegalStateException("SDK generation identity/window is unavailable");
        }
        final var next = used.add(cost);
        if (!limit.covers(next)) {
            throw new IllegalStateException("destination SDK envelope is exhausted");
        }
        final var nativeCost = resources.reserveNativeResource(
                "destination-sdk/" + id, NativeResourceUsage.zero(), cost.amount(CapacityDimension.OTHER_NATIVE_BYTES));
        final var client = new Client(id, cost, nativeCost);
        clients.put(id, client);
        used = next;
        return client;
    }

    public synchronized CapacityVector usage() {
        return used;
    }

    public synchronized int clients() {
        return clients.size();
    }

    public int maximumClients() {
        return maximumClients;
    }

    private static CapacityVector finite(CapacityVector vector) {
        Objects.requireNonNull(vector, "explicit SDK envelope");
        for (var dimension : CapacityDimension.values()) {
            if (vector.amount(dimension) == Long.MAX_VALUE
                    || !DIMENSIONS.contains(dimension) && vector.amount(dimension) != 0) {
                throw new IllegalArgumentException("unsupported/unbounded SDK resource dimension: " + dimension);
            }
        }
        return vector;
    }

    /** Kept by the actual client owner; timeout, result cancellation and failed close do not release it. */
    public final class Client {
        private final String id;
        private final CapacityVector cost;
        private final WorkerNativeResourceLedger.Reservation nativeCost;
        private boolean started;
        private boolean released;

        private Client(String id, CapacityVector cost, WorkerNativeResourceLedger.Reservation nativeCost) {
            this.id = id;
            this.cost = cost;
            this.nativeCost = nativeCost;
        }

        public void beginConstruction() {
            synchronized (TargetDestinationSdkBudget.this) {
                if (released || started) {
                    throw new IllegalStateException("SDK allocation is not unused");
                }
                resources.requireRuntimeBusinessAdmission();
                started = true;
            }
        }

        public void requireCovers(CapacityVector minimum) {
            synchronized (TargetDestinationSdkBudget.this) {
                if (released || !cost.covers(finite(minimum))) {
                    throw new IllegalStateException("SDK allocation does not cover its actual configured resources");
                }
            }
        }

        public void releaseUnstarted() {
            synchronized (TargetDestinationSdkBudget.this) {
                if (started && !released) {
                    throw new IllegalStateException("started SDK requires actual close completion");
                }
                release();
            }
        }
        /** Invoke only after successful SDK close, or the dependency's failed-constructor cleanup returned. */
        public void closed() {
            synchronized (TargetDestinationSdkBudget.this) {
                release();
            }
        }

        public boolean released() {
            synchronized (TargetDestinationSdkBudget.this) {
                return released;
            }
        }

        TargetDestinationSdkBudget owningBudget() {
            return TargetDestinationSdkBudget.this;
        }

        void requireResources(SharedRocksDbResources expected) {
            if (resources != expected) {
                throw new IllegalArgumentException("SDK input charge belongs to another actual Worker resource graph");
            }
        }

        private void release() {
            if (released) {
                return;
            }
            final var next = used.subtract(cost);
            nativeCost.close();
            if (clients.remove(id) != this) {
                throw new IllegalStateException("SDK allocation ownership changed");
            }
            used = next;
            released = true;
        }
    }
}
