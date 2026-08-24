package com.nereusstream.delay.transport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class InMemoryCommandTransportRegistryTest {
    @Test
    void closeRetriesOnlyTheTransportThatFailedTheFirstTeardown() {
        final CloseTrackingTransport stable = new CloseTrackingTransport(key(1), false);
        final CloseTrackingTransport flaky = new CloseTrackingTransport(key(2), true);
        final InMemoryCommandTransportRegistry registry = new InMemoryCommandTransportRegistry();
        registry.register(stable);
        registry.register(flaky);

        final IllegalStateException failure = assertThrows(IllegalStateException.class, registry::close);

        assertEquals("simulated transport close failure", failure.getMessage());
        assertEquals(1, stable.closeCalls.get());
        assertEquals(1, flaky.closeCalls.get());
        assertNull(registry.exact(stable.key));
        assertNull(registry.exact(flaky.key));

        registry.close();
        registry.close();

        assertEquals(1, stable.closeCalls.get());
        assertEquals(2, flaky.closeCalls.get());
    }

    private static KafkaCommandTransportKey key(final int seed) {
        return new KafkaCommandTransportKey(
                "cluster",
                "topic-" + seed,
                UUID.randomUUID(),
                seed,
                new CredentialBindingKey(seed, digest(seed), digest(seed + 1)));
    }

    private static Digest32 digest(final int seed) {
        final byte[] value = new byte[32];
        for (int index = 0; index < value.length; index++) {
            value[index] = (byte) (seed + index);
        }
        return new Digest32(value);
    }

    private static final class CloseTrackingTransport implements CommandTransport {
        private final CommandTransportKey key;
        private final boolean failFirstClose;
        private final AtomicInteger closeCalls = new AtomicInteger();

        private CloseTrackingTransport(final CommandTransportKey key, final boolean failFirstClose) {
            this.key = key;
            this.failFirstClose = failFirstClose;
        }

        @Override
        public CommandTransportKey key() {
            return key;
        }

        @Override
        public CompletionStage<? extends TransportResult> send(
                final TransportRequest request, final TransportOwnershipPermit ownershipPermit) {
            return null;
        }

        @Override
        public void close() {
            final int calls = closeCalls.incrementAndGet();
            if (failFirstClose && calls == 1) {
                throw new IllegalStateException("simulated transport close failure");
            }
        }
    }
}
