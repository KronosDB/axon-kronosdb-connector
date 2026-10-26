package io.kronosdb.connector.axon.event;

import io.kronosdb.protocol.ProcessorStatus;
import io.kronosdb.protocol.SegmentStatus;
import org.axonframework.messaging.eventhandling.processing.EventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.StreamingEventProcessor;
import org.axonframework.messaging.eventhandling.processing.streaming.segmenting.EventTrackerStatus;
import org.axonframework.messaging.eventhandling.processing.streaming.token.TrackingToken;
import org.axonframework.messaging.eventhandling.processing.subscribing.SubscribingEventProcessor;

import java.util.List;

/**
 * Utility class constructing {@link ProcessorStatus} instances for all known
 * {@link EventProcessor} types.
 * <p>
 * Normalizes each processor type into the status shape that
 * KronosDB's platform service expects.
 */
final class EventProcessorInfoUtils {

    private static final String POOLED_STREAMING = "Pooled Streaming";
    private static final String SUBSCRIBING = "Subscribing";
    private static final String UNKNOWN = "Unknown";

    static ProcessorStatus describeStreaming(StreamingEventProcessor processor) {
        List<SegmentStatus> segmentStatuses = processor.processingStatus()
                .values()
                .stream()
                .map(EventProcessorInfoUtils::buildSegmentStatus)
                .toList();

        int active = processor.processingStatus().size();
        return new ProcessorStatus(
                processor.name(),
                POOLED_STREAMING,
                active,
                Math.max(0, processor.maxCapacity() - active),
                processor.isRunning(),
                processor.isError(),
                true,
                processor.getTokenStoreIdentifier(),
                segmentStatuses);
    }

    static ProcessorStatus describeSubscribing(SubscribingEventProcessor processor) {
        return new ProcessorStatus(processor.name(), SUBSCRIBING, 0, 0, false, false, false, "", List.of());
    }

    static ProcessorStatus describeUnknown(EventProcessor processor) {
        return new ProcessorStatus(processor.name(), UNKNOWN, 0, 0, false, false, false, "", List.of());
    }

    private static SegmentStatus buildSegmentStatus(EventTrackerStatus status) {
        return new SegmentStatus(
                status.getSegment().getSegmentId(),
                status.isCaughtUp(),
                status.isReplaying(),
                status.getSegment().getMask() + 1,
                getPosition(status.getTrackingToken()),
                status.isErrorState() ? buildErrorMessage(status.getError()) : "");
    }

    private static long getPosition(TrackingToken trackingToken) {
        if (trackingToken != null) {
            return trackingToken.position().orElse(0);
        }
        return 0;
    }

    private static String buildErrorMessage(Throwable error) {
        return error.getClass().getName() + ": " + error.getMessage();
    }

    private EventProcessorInfoUtils() {
    }
}
