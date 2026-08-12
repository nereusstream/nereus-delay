package io.nereusstream.delay.store;

import io.nereusstream.delay.scheduler.WorkClass;
import io.nereusstream.delay.scheduler.WorkClassExecutionRegistry;
import io.nereusstream.delay.scheduler.WorkClassPolicy;
import io.nereusstream.delay.scheduler.WorkClassRuntimeConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.EnumMap;

import static org.junit.jupiter.api.Assertions.assertThrows;

class SharedRocksDbResourcesTest {
    @TempDir
    Path tempDir;

    @Test
    void oneStoreResourceEnvelopeCannotMultiplyQueuesAcrossRegistries() {
        final ShardStoreConfig config = ShardStoreConfig.defaults(tempDir.resolve("one-envelope"));
        try (SharedRocksDbResources resources = new SharedRocksDbResources(config)) {
            final WorkClassExecutionRegistry first = workClasses();
            final WorkClassExecutionRegistry second = workClasses();

            resources.bindWorkClassExecutionRegistry(first);
            resources.bindWorkClassExecutionRegistry(first);
            assertThrows(IllegalArgumentException.class,
                    () -> resources.bindWorkClassExecutionRegistry(second));
        }
    }

    @Test
    void oneRegistryCannotSpanTwoStoreResourceEnvelopes() {
        final ShardStoreConfig firstConfig = ShardStoreConfig.defaults(tempDir.resolve("first-envelope"));
        final ShardStoreConfig secondConfig = ShardStoreConfig.defaults(tempDir.resolve("second-envelope"));
        try (SharedRocksDbResources first = new SharedRocksDbResources(firstConfig);
             SharedRocksDbResources second = new SharedRocksDbResources(secondConfig)) {
            final WorkClassExecutionRegistry registry = workClasses();

            first.bindWorkClassExecutionRegistry(registry);
            assertThrows(IllegalArgumentException.class,
                    () -> second.bindWorkClassExecutionRegistry(registry));

            // The rejected envelope can still be attached to its own complete graph.
            second.bindWorkClassExecutionRegistry(workClasses());
        }
    }

    private static WorkClassExecutionRegistry workClasses() {
        final EnumMap<WorkClass, WorkClassPolicy> policies = new EnumMap<>(WorkClass.class);
        for (WorkClass workClass : WorkClass.values()) {
            final boolean protectedClass = switch (workClass) {
                case LEASE_FENCE, SOURCE_APPLY, OUTCOME_AND_CONTROL, EXPIRY, DUE_SCHEDULER, GC -> true;
                case QUERY, CHECKPOINT -> false;
            };
            policies.put(workClass, new WorkClassPolicy(1, 1, 1_024,
                    1, 1_024, 1_000, protectedClass ? 1 : 0,
                    protectedClass ? 1 : 0, workClass == WorkClass.LEASE_FENCE));
        }
        return new WorkClassExecutionRegistry(
                new WorkClassRuntimeConfig(policies, 100, 100, 16, 8_192), () -> 0);
    }
}
