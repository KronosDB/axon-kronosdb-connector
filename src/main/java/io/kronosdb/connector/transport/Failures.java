package io.kronosdb.connector.transport;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/** Helpers for the errors futures complete with. */
public final class Failures {

    private Failures() {
    }

    /**
     * The error behind the {@link CompletionException} or {@link ExecutionException}
     * that dependent stages and {@code get()} wrap it in.
     */
    public static Throwable unwrap(Throwable t) {
        while ((t instanceof CompletionException || t instanceof ExecutionException) && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }
}
