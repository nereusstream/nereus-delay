package io.nereusstream.delay.scheduler;

import java.util.Objects;

/** A bounded unit of event-loop work; the scheduler does not execute it. */
public record WorkClassTask(WorkClass workClass, String taskId, long bytes) {
    public WorkClassTask {
        Objects.requireNonNull(workClass, "workClass");
        Objects.requireNonNull(taskId, "taskId");
        if (taskId.isBlank() || bytes <= 0) {
            throw new IllegalArgumentException("work-class task identity and bytes must be positive");
        }
    }
}
