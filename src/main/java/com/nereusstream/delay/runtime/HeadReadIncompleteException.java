package com.nereusstream.delay.runtime;

import com.nereusstream.delay.store.BoundedReadBudget;
import com.nereusstream.delay.store.ReadIncompleteException;

/** Local retry only, issued after proving the entire mutation has changed no Store/source view. */
public final class HeadReadIncompleteException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final BoundedReadBudget.Exhaustion reason;

    HeadReadIncompleteException(final ReadIncompleteException cause) {
        super("head plan incomplete before mutation commit: " + cause.reason(), cause);
        this.reason = cause.reason();
    }

    public BoundedReadBudget.Exhaustion reason() {
        return reason;
    }
}
