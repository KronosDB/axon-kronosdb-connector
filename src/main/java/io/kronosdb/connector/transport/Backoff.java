package io.kronosdb.connector.transport;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/** Exponential backoff with jitter for re-opening handler streams. */
final class Backoff {

    private static final long INITIAL_MS = 500;
    private static final long MAX_MS = 30_000;
    private static final double JITTER_FACTOR = 0.25;

    private Backoff() {
    }

    /**
     * Tells a stream that keeps ending soon after it opened from one that
     * ended after a long life. The server ends the older of two handler or
     * platform streams of one client id, so two instances sharing an id would
     * otherwise take each other's streams over, reconnecting at the first
     * delay forever; counting the short lives lets the delay grow instead.
     */
    static final class Flaps {

        private static final long STABLE_NANOS = TimeUnit.SECONDS.toNanos(30);

        private volatile long openedAt = System.nanoTime();
        private volatile int inARow;

        void opened() {
            openedAt = System.nanoTime();
        }

        /** The stream ended: how many in a row, this one included, ended soon after opening. */
        int ended() {
            inARow = System.nanoTime() - openedAt < STABLE_NANOS ? inARow + 1 : 0;
            return inARow;
        }

        /** The attempt number a reconnect loop starts counting from. */
        int startAttempt() {
            return Math.max(0, inARow - 1);
        }
    }

    /** The delay before attempt number {@code attempt}, counting from 1. */
    static long delay(int attempt) {
        long delay = Math.min(INITIAL_MS * (1L << Math.min(attempt - 1, 20)), MAX_MS);
        long jitter = (long) (delay * JITTER_FACTOR * ThreadLocalRandom.current().nextDouble());
        return delay + jitter;
    }
}
