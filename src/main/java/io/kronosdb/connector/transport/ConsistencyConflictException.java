package io.kronosdb.connector.transport;

/**
 * An append's consistency condition failed: an event matching it exists at or
 * after the condition's marker. Nothing was written.
 */
public class ConsistencyConflictException extends TransportException {

    private static final long serialVersionUID = 1L;

    private final long conflictingPosition;

    public ConsistencyConflictException(long conflictingPosition) {
        super("consistency condition violated: conflicting event at position "
                + Long.toUnsignedString(conflictingPosition));
        this.conflictingPosition = conflictingPosition;
    }

    /** The position of a matching event, unsigned. */
    public long conflictingPosition() {
        return conflictingPosition;
    }
}
