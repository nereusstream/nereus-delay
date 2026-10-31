package com.nereusstream.delay.ownership;

import com.nereusstream.delay.scheduler.WorkClass;
import com.nereusstream.delay.scheduler.WorkClassTask;
import java.util.Objects;

/** A single bounded flight on the original Process registry and I/O; no private thread or queue. */
final class TargetMigrationMaintenanceWorkClassExecutor {
    private final TargetWorkerProcessRuntime.Assembly graph;
    private final String identity;
    private final long bytes;
    private WorkClassTask task;
    private volatile OwnedCheckpointIo<Boolean> io;
    private long sequence;

    TargetMigrationMaintenanceWorkClassExecutor(TargetWorkerProcessRuntime.Assembly graph, String identity, long bytes) {
        this.graph = Objects.requireNonNull(graph); this.identity = Objects.requireNonNull(identity); this.bytes = bytes;
        graph.classes().requireTaskServiceable(new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL, identity, bytes),
                graph.config().checkpoint().settlementTurn());
    }

    synchronized boolean idle() {
        if (io != null) {
            io.retryCleanup();
            if (!io.operationExited() || !io.ioExited()) return false;
            io.receipt().join(); io = null;
        }
        if (task != null) {
            final var state = graph.classes().state(task);
            if (state.orElse(null) == com.nereusstream.delay.scheduler.WorkClassExecutionRegistry.ExecutionState.FAILED)
                throw new IllegalStateException("original live migration WorkClass I/O admission failed");
            if (state.isPresent()) return false;
            task = null;
        }
        return true;
    }
    synchronized void submit(Runnable step, Runnable current) {
        if (!idle()) return;
        final var next = new WorkClassTask(WorkClass.OUTCOME_AND_CONTROL,
                identity + '-' + Long.toUnsignedString(++sequence), bytes);
        graph.classes().submit(next, () -> {
            synchronized (TargetMigrationMaintenanceWorkClassExecutor.this) {
                io = OwnedCheckpointIo.submit(guard -> { guard.run(); step.run(); guard.run(); return true; }, current, graph.io());
            }
        });
        task = next;
    }
    synchronized void requestStop() {
        if (task != null) { graph.classes().withdrawQueued(task); graph.classes().discardFailed(task); }
        if (io != null) io.requestStop();
    }
    synchronized boolean ioExited() {
        if (task != null && graph.classes().state(task).isPresent()) return false;
        if (io != null) {
            io.retryCleanup();
            if (!io.operationExited() || !io.ioExited()) return false;
        }
        return true;
    }
    synchronized void clearExited() {
        if (!ioExited()) return;
        io = null; task = null;
    }
}
