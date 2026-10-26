package io.kronosdb.connector.transport;

import io.kronosdb.protocol.Ack;
import io.kronosdb.protocol.ClientAck;
import io.kronosdb.protocol.ClientIdentity;
import io.kronosdb.protocol.ErrorDetail;
import io.kronosdb.protocol.Frame;
import io.kronosdb.protocol.GetPlatformInfo;
import io.kronosdb.protocol.InstructionResult;
import io.kronosdb.protocol.OpenPlatform;
import io.kronosdb.protocol.PlatformHeartbeat;
import io.kronosdb.protocol.PlatformInfo;
import io.kronosdb.protocol.ProcessorAction;
import io.kronosdb.protocol.ProcessorStatus;
import io.kronosdb.protocol.RequestReconnect;
import io.kronosdb.protocol.ServerHeartbeat;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Channel abstraction for the KronosDB platform service of the native protocol.
 * Manages the client lifecycle, heartbeat, event processor reporting,
 * and handling of server-initiated processor instructions.
 */
public class PlatformChannel {

    private static final Logger logger = LoggerFactory.getLogger(PlatformChannel.class);

    private final Transport transport;
    private final String context;

    private volatile @Nullable ClientStream platformStream;
    private ScheduledExecutorService heartbeatExecutor;
    private volatile ClientIdentity lastIdentification;
    private volatile Runnable lastReconnectCallback;
    private volatile boolean reconnecting = false;
    private final Backoff.Flaps flaps = new Backoff.Flaps();
    private volatile boolean disconnected = false;

    /**
     * Handler for server-initiated event processor instructions.
     * <p>
     * Set via {@link #setInstructionHandler(InstructionHandler)} before opening the stream.
     * When the server sends a processor instruction (pause, start, split, merge, release),
     * the handler is invoked. The handler should return a {@link CompletableFuture} that
     * completes when the instruction has been executed, with {@code true} for success.
     */
    private volatile InstructionHandler instructionHandler;

    PlatformChannel(Transport transport, String context) {
        this.transport = transport;
        this.context = context;
    }

    /**
     * Connects to the platform service by identifying this client.
     */
    public CompletableFuture<PlatformInfo> connect(ClientIdentity identification) {
        return transport.request(new GetPlatformInfo(identification), frame -> {
            if (frame instanceof PlatformInfo info) {
                return info;
            }
            throw new TransportException("the server answered with " + frame.getClass().getSimpleName()
                    + " instead of PlatformInfo");
        });
    }

    /**
     * Sets the handler for server-initiated event processor instructions.
     */
    public void setInstructionHandler(InstructionHandler handler) {
        this.instructionHandler = handler;
    }

    /**
     * Opens the persistent lifecycle stream for heartbeat, processor reporting,
     * and server instructions.
     */
    public void openStream(ClientIdentity identification, Runnable onReconnectRequested) {
        this.lastIdentification = identification;
        this.lastReconnectCallback = onReconnectRequested;
        try {
            openStreamInternal(identification, onReconnectRequested);
        } catch (RuntimeException e) {
            logger.warn("Could not open the platform stream for context [{}]: {}.", context, e.getMessage());
            schedulePlatformReconnect();
        }
    }

    private void openStreamInternal(ClientIdentity identification, Runnable onReconnectRequested) {
        NativeConnection connection = transport.connection();
        flaps.opened();
        platformStream = connection.open(new OpenPlatform(identification), false, new StreamListener() {
            @Override
            public void onFrame(Frame frame, boolean last) {
                switch (frame) {
                    case ServerHeartbeat ignored -> transport.handlers().execute(PlatformChannel.this::sendHeartbeat);
                    case RequestReconnect ignored -> {
                        logger.info("Server requested reconnect for context [{}].", context);
                        transport.handlers().execute(() -> {
                            if (onReconnectRequested != null) {
                                onReconnectRequested.run();
                            }
                        });
                    }
                    case io.kronosdb.protocol.ProcessorInstruction instruction ->
                            transport.handlers().execute(() -> handleProcessorInstruction(instruction));
                    default -> {
                        // Acks, topology changes: nothing to do.
                    }
                }
                if (last) {
                    logger.info("Platform stream completed for context [{}].", context);
                    platformStream = null;
                    streamEnded();
                    schedulePlatformReconnect();
                }
            }

            @Override
            public void onFailure(Throwable cause) {
                logger.warn("Platform stream error for context [{}]: {}.", context, cause.getMessage());
                platformStream = null;
                streamEnded();
                schedulePlatformReconnect();
            }
        });
    }

    private void streamEnded() {
        int shortLives = flaps.ended();
        if (shortLives >= 2) {
            logger.warn("The platform stream ended {} times in a row soon after opening. "
                    + "Another instance or connection may be using the same client id.", shortLives);
        }
    }

    /**
     * Thread-safe write to the platform stream.
     */
    private void sendToStream(Frame frame) {
        ClientStream stream = platformStream;
        if (stream != null) {
            stream.send(frame);
        }
    }

    /**
     * Sends an event processor status report to the server.
     */
    public void sendEventProcessorInfo(ProcessorStatus info) {
        sendToStream(info);
    }

    /**
     * Enables periodic heartbeat messages.
     */
    public synchronized void enableHeartbeat(long intervalMillis) {
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kronosdb-heartbeat-" + context);
            t.setDaemon(true);
            return t;
        });
        heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeat, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeat() {
        // Never let an exception escape — a throw from a scheduled task cancels
        // all future runs, silently killing the heartbeat.
        try {
            sendToStream(new PlatformHeartbeat());
        } catch (Exception e) {
            logger.warn("Failed to send heartbeat for context [{}]: {}.", context, e.getMessage());
        }
    }

    private void sendAck(String instructionId, boolean success, @Nullable String errorMessage) {
        ErrorDetail error = errorMessage == null ? null : new ErrorDetail(errorMessage, "", List.of(), "");
        try {
            sendToStream(new ClientAck(new Ack(instructionId, success, error)));
        } catch (RuntimeException e) {
            logger.debug("Could not acknowledge instruction [{}]: {}.", instructionId, e.getMessage());
        }
    }

    private void sendInstructionResult(String instructionId, boolean success, @Nullable String errorMessage) {
        ErrorDetail error = errorMessage == null ? null : new ErrorDetail(errorMessage, "", List.of(), "");
        try {
            sendToStream(new InstructionResult(instructionId, success, error));
        } catch (RuntimeException e) {
            logger.debug("Could not report the result of instruction [{}]: {}.", instructionId, e.getMessage());
        }
    }

    private void handleProcessorInstruction(io.kronosdb.protocol.ProcessorInstruction wire) {
        String instructionId = wire.instructionId();
        ProcessorInstruction instruction = ProcessorInstruction.of(wire);
        InstructionHandler handler = this.instructionHandler;
        if (instruction == null) {
            logger.debug("Ignoring processor instruction {} for processor [{}].", wire.action(), wire.processorName());
            if (!instructionId.isEmpty()) {
                sendAck(instructionId, false, "unsupported instruction " + wire.action());
            }
            return;
        }
        if (handler == null) {
            logger.debug("No instruction handler set, ignoring {} instruction for processor [{}].",
                    instruction.type(), instruction.processorName());
            if (!instructionId.isEmpty()) {
                sendAck(instructionId, true, null);
            }
            return;
        }

        logger.debug("Received {} instruction for processor [{}].", instruction.type(), instruction.processorName());

        // Acknowledge that we received it
        if (!instructionId.isEmpty()) {
            sendAck(instructionId, true, null);
        }

        // Execute the instruction asynchronously and send the result
        handler.handle(instruction).whenComplete((success, error) -> {
            if (!instructionId.isEmpty()) {
                if (error != null) {
                    sendInstructionResult(instructionId, false, Failures.unwrap(error).getMessage());
                } else {
                    sendInstructionResult(instructionId, success != null && success, null);
                }
            }
        });
    }

    private void schedulePlatformReconnect() {
        if (disconnected || transport.isClosed() || lastIdentification == null) {
            return;
        }
        synchronized (this) {
            if (reconnecting) {
                return;
            }
            reconnecting = true;
        }

        transport.background(() -> {
            int attempt = flaps.startAttempt();
            try {
                while (!disconnected && !transport.isClosed()) {
                    attempt++;
                    long delay = Backoff.delay(attempt);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    try {
                        logger.info("Reconnecting platform stream for context [{}] (attempt {}, delay {}ms).",
                                context, attempt, delay);
                        openStreamInternal(lastIdentification, lastReconnectCallback);

                        if (platformStream == null) {
                            logger.warn("Platform stream open returned but stream is null — retrying.");
                            continue;
                        }

                        logger.info("Platform stream reconnected for context [{}].", context);
                        return;
                    } catch (Exception e) {
                        logger.warn("Platform reconnect attempt {} failed for context [{}]: {}.",
                                attempt, context, e.getMessage());
                        platformStream = null;
                    }
                }
            } finally {
                reconnecting = false;
            }
        });
    }

    /**
     * Disconnects the platform stream.
     */
    public synchronized void disconnect() {
        disconnected = true;
        if (heartbeatExecutor != null) {
            heartbeatExecutor.shutdown();
        }
        ClientStream stream = platformStream;
        platformStream = null;
        if (stream != null) {
            stream.cancel();
        }
    }

    /**
     * Handler interface for server-initiated event processor instructions.
     */
    @FunctionalInterface
    public interface InstructionHandler {
        /**
         * Handles a processor instruction from the server.
         *
         * @param instruction the instruction to execute
         * @return a future that completes with {@code true} on success, {@code false} on failure
         */
        CompletableFuture<Boolean> handle(ProcessorInstruction instruction);
    }

    /**
     * Represents a server instruction for an event processor.
     */
    public record ProcessorInstruction(Type type, String processorName, int segmentId) {

        public enum Type {
            PAUSE, START, SPLIT, MERGE, RELEASE_SEGMENT, REQUEST_INFO
        }

        /** The instruction the wire frame asks for, or {@code null} for an action this client does not know. */
        static @Nullable ProcessorInstruction of(io.kronosdb.protocol.ProcessorInstruction wire) {
            String name = wire.processorName();
            int segment = (int) wire.segment();
            int action = wire.action().value();
            if (action == ProcessorAction.PAUSE.value()) {
                return new ProcessorInstruction(Type.PAUSE, name, -1);
            } else if (action == ProcessorAction.START.value()) {
                return new ProcessorInstruction(Type.START, name, -1);
            } else if (action == ProcessorAction.SPLIT_SEGMENT.value()) {
                return new ProcessorInstruction(Type.SPLIT, name, segment);
            } else if (action == ProcessorAction.MERGE_SEGMENT.value()) {
                return new ProcessorInstruction(Type.MERGE, name, segment);
            } else if (action == ProcessorAction.RELEASE_SEGMENT.value()) {
                return new ProcessorInstruction(Type.RELEASE_SEGMENT, name, segment);
            } else if (action == ProcessorAction.REPORT_STATUS.value()) {
                return new ProcessorInstruction(Type.REQUEST_INFO, name, -1);
            }
            return null;
        }
    }
}
