package io.nereusstream.delay.scheduler;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Process-local action registry for the bounded V1 work-class dispatcher.
 *
 * <p>Each queue identity is registered with one exact synchronous action
 * before it is offered. Queue admission failure removes that registration.
 * Successful execution removes it; a started action that fails remains in the
 * explicit {@link ExecutionState#FAILED} state and can only be queued again by
 * {@link #retry(WorkClassTask)} with the same class, identity and byte charge.
 * Never-started trailing tasks remain {@code QUEUED} when the event loop
 * restores them after a fatal/hold stop.</p>
 *
 * <p>The action must perform one bounded local mutation or durable external
 * handoff and return; this registry is not the task's durable authority and
 * must be rebuilt from shard/source/checkpoint indexes after process loss.</p>
 */
public final class WorkClassExecutionRegistry {
    private final WorkClassDispatcher dispatcher;
    private final Map<TaskKey, RegisteredAction> actions = new HashMap<>();
    private final EnumMap<WorkerSingleton, Object> workerSingletons = new EnumMap<>(WorkerSingleton.class);

    public WorkClassExecutionRegistry(final WorkClassRuntimeConfig config,
                                      final LongSupplier monotonicClockNanos) {
        Objects.requireNonNull(config, "config");
        final EnumMap<WorkClass, Consumer<WorkClassTask>> handlers = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            handlers.put(workClass, this::execute);
        }
        dispatcher = config.newDispatcher(monotonicClockNanos, handlers);
    }

    /** Atomically registers an exact action and offers its bounded queue identity. */
    public void submit(final WorkClassTask task, final Runnable action) {
        final WorkClassTask submitted = Objects.requireNonNull(task, "task");
        final RegisteredAction registration = new RegisteredAction(submitted,
                Objects.requireNonNull(action, "action"), ExecutionState.QUEUED);
        final TaskKey key = TaskKey.from(submitted);
        synchronized (this) {
            if (actions.putIfAbsent(key, registration) != null) {
                throw new IllegalStateException("work-class task identity is already registered");
            }
            try {
                dispatcher.offer(submitted);
            } catch (RuntimeException | Error failure) {
                actions.remove(key, registration);
                throw failure;
            }
        }
    }

    /** Requeues the exact retained action after its previous invocation failed. */
    public void retry(final WorkClassTask task) {
        final WorkClassTask requested = Objects.requireNonNull(task, "task");
        synchronized (this) {
            final RegisteredAction registration = requireExact(requested);
            if (registration.state != ExecutionState.FAILED) {
                throw new IllegalStateException("only a failed work-class task can be retried");
            }
            registration.state = ExecutionState.QUEUED;
            try {
                dispatcher.offer(registration.task);
            } catch (RuntimeException | Error failure) {
                registration.state = ExecutionState.FAILED;
                throw failure;
            }
        }
    }

    /** Runs one configured bounded turn through the registered actions. */
    public List<WorkClassTask> runTurn(final SchedulerBudget budget) {
        return dispatcher.runTurn(Objects.requireNonNull(budget, "budget"));
    }

    /** Returns the state only when the complete task identity matches. */
    public synchronized Optional<ExecutionState> state(final WorkClassTask task) {
        final WorkClassTask requested = Objects.requireNonNull(task, "task");
        final RegisteredAction registration = actions.get(TaskKey.from(requested));
        if (registration == null) {
            return Optional.empty();
        }
        requireSameTask(requested, registration);
        return Optional.of(registration.state);
    }

    public synchronized int registeredActions() {
        return actions.size();
    }

    /** Binds all Claim and Publish Admission actions on this Worker registry to one exact permit pool. */
    public synchronized void bindClaimExecutionAdmission(final ClaimExecutionAdmission admission) {
        final ClaimExecutionAdmission requested = Objects.requireNonNull(admission, "admission");
        final Object existing = workerSingletons.get(WorkerSingleton.CLAIM_EXECUTION_ADMISSION);
        if (existing != null && existing != requested) {
            throw new IllegalArgumentException(
                    "work-class registry is already bound to another CLAIM_EXECUTION_ADMISSION");
        }
        requested.bindWorkClassExecutionRegistry(this);
        workerSingletons.put(WorkerSingleton.CLAIM_EXECUTION_ADMISSION, requested);
    }

    /** Binds one process-wide resource authority to this exact Worker registry. */
    public synchronized void bindWorkerSingleton(final WorkerSingleton resource, final Object instance) {
        final WorkerSingleton key = Objects.requireNonNull(resource, "resource");
        final Object requested = Objects.requireNonNull(instance, "instance");
        final Object existing = workerSingletons.putIfAbsent(key, requested);
        if (existing != null && existing != requested) {
            throw new IllegalArgumentException("work-class registry is already bound to another " + key);
        }
    }

    public int pending(final WorkClass workClass) {
        return dispatcher.pending(Objects.requireNonNull(workClass, "workClass"));
    }

    public long pendingBytes(final WorkClass workClass) {
        return dispatcher.pendingBytes(Objects.requireNonNull(workClass, "workClass"));
    }

    private void execute(final WorkClassTask task) {
        final RegisteredAction registration;
        synchronized (this) {
            registration = requireExact(task);
            if (registration.state != ExecutionState.QUEUED) {
                throw new IllegalStateException("selected work-class action is not queued");
            }
            registration.state = ExecutionState.RUNNING;
        }
        try {
            registration.action.run();
        } catch (RuntimeException | Error failure) {
            synchronized (this) {
                if (actions.get(TaskKey.from(task)) == registration) {
                    registration.state = ExecutionState.FAILED;
                }
            }
            throw failure;
        }
        synchronized (this) {
            if (registration.state != ExecutionState.RUNNING
                    || !actions.remove(TaskKey.from(task), registration)) {
                throw new IllegalStateException("work-class action registration changed while it was running");
            }
        }
    }

    private RegisteredAction requireExact(final WorkClassTask task) {
        final RegisteredAction registration = actions.get(TaskKey.from(task));
        if (registration == null) {
            throw new IllegalStateException("work-class task action is not registered");
        }
        requireSameTask(task, registration);
        return registration;
    }

    private static void requireSameTask(final WorkClassTask task, final RegisteredAction registration) {
        if (!registration.task.equals(task)) {
            throw new IllegalStateException("work-class task identity was reused with a different byte charge");
        }
    }

    /** Process-local execution state; durable retry authority remains external. */
    public enum ExecutionState {
        QUEUED,
        RUNNING,
        FAILED
    }

    /** Closed set of process-wide resource authorities bound to one Worker execution graph. */
    public enum WorkerSingleton {
        STORE_RESOURCE_ENVELOPE,
        CLAIM_EXECUTION_ADMISSION,
        DESTINATION_PHYSICAL_ADMISSION
    }

    private record TaskKey(WorkClass workClass, String taskId) {
        private static TaskKey from(final WorkClassTask task) {
            return new TaskKey(task.workClass(), task.taskId());
        }
    }

    private static final class RegisteredAction {
        private final WorkClassTask task;
        private final Runnable action;
        private ExecutionState state;

        private RegisteredAction(final WorkClassTask task, final Runnable action, final ExecutionState state) {
            this.task = task;
            this.action = action;
            this.state = state;
        }
    }
}
