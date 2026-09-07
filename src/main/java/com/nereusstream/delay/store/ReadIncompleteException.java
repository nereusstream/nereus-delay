package com.nereusstream.delay.store;

import java.util.Objects;

/** Local read-plan yield. It is neither a business rejection nor evidence of an empty prefix. */
public final class ReadIncompleteException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final BoundedReadBudget.Exhaustion reason;

    ReadIncompleteException(final BoundedReadBudget.Exhaustion reason) {
        super("local read plan incomplete: " + Objects.requireNonNull(reason, "reason"));
        this.reason = reason;
    }

    public BoundedReadBudget.Exhaustion reason() {
        return reason;
    }
}
