package io.nereusstream.delay.ownership;

import io.nereusstream.delay.protocol.Bytes;
import io.nereusstream.delay.protocol.ProtocolCapabilityDeclarationV1;

import java.util.Objects;
import java.util.Optional;

/** Durable authority for the protocol capabilities of one live Worker session. */
public interface ProtocolCapabilityAuthority {
    Publication publish(ProtocolCapabilityDeclarationV1 declaration, long expectedRevision);

    Optional<Publication> current(String workerId);

    boolean withdraw(Publication expected);

    record Publication(long revision, ProtocolCapabilityDeclarationV1 declaration) {
        public Publication {
            if (revision <= 0) {
                throw new IllegalArgumentException("protocol capability revision must be positive");
            }
            Objects.requireNonNull(declaration, "declaration");
        }

        public boolean sameIdentity(final Publication other) {
            return other != null && revision == other.revision()
                    && Bytes.constantTimeEquals(declaration.canonicalBytes(), other.declaration.canonicalBytes());
        }
    }
}
