package com.nereusstream.delay.transport;

import java.time.Duration;
import org.apache.kafka.clients.producer.GuardedProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

/** Same original producer/component facts for Source, BASELINE and K2; no replacement producer or protocol action. */
final class KafkaClientArtifactProducerCleanup {
    private KafkaClientArtifactProducerCleanup() {}

    static void close(Producer<?, ?> producer, Duration timeout) {
        if (!(producer instanceof GuardedProducer<?, ?> guarded)) {
            throw new IllegalStateException("actual guarded producer cleanup is required");
        }
        guarded.closeGuardedResources(timeout);
        if (!guarded.guardedResourcesTerminated()) {
            throw new IllegalStateException("original producer native resources remain live");
        }
    }

    static KafkaClientArtifactTargetCleanupFailure partial(
            KafkaProducer.ConstructionCleanupFailure failure, Duration timeout, Runnable released) {
        return new KafkaClientArtifactTargetCleanupFailure(failure, () -> {
            failure.reconcile(timeout);
            if (!failure.resourcesTerminated()) {
                throw new IllegalStateException("original partial producer remains live");
            }
            released.run();
        });
    }
}
