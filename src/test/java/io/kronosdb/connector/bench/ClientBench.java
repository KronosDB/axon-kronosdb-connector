package io.kronosdb.connector.bench;

import com.sun.management.OperatingSystemMXBean;
import io.kronosdb.connector.transport.CommandChannel;
import io.kronosdb.connector.transport.EventStoreChannel;
import io.kronosdb.connector.transport.KronosDbConnectionFactory;
import io.kronosdb.connector.transport.ResultStream;
import io.kronosdb.protocol.Command;
import io.kronosdb.protocol.CommandResult;
import io.kronosdb.protocol.Criterion;
import io.kronosdb.protocol.EventRecord;
import io.kronosdb.protocol.Events;
import io.kronosdb.protocol.Payload;
import io.kronosdb.protocol.StoredRecord;
import io.kronosdb.protocol.Tag;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Client benchmark for the native transport: event store appends and reads and a
 * command round trip straight through the channels, with no Axon in the way.
 * One scenario per JVM, so a driver can sample the server's CPU around it:
 *
 * <pre>
 * java -cp ... io.kronosdb.connector.bench.ClientBench SCENARIO HOST PORT [SECONDS [WARMUP]]
 * </pre>
 *
 * Scenarios: {@code append-1-c1}, {@code append-1-c16}, {@code append-1-c64},
 * {@code append-100-c1}, {@code read-tag-c1}, {@code read-tag-c16}
 * (a source of one tag's 100 events), {@code read-all-c1} (a source of 10,000 events),
 * {@code cmd-c1}, {@code cmd-c16}, {@code cmd-c64} (command dispatch round trip, handler
 * in this JVM on a connection of its own).
 *
 * <p>Workers run flat out for WARMUP seconds, then for SECONDS, the measured window.
 * The output has markers for the driver and one result line:
 * {@code RESULT scenario ops/s events/s p50us p99us clientCpuUsPerOp ops seconds}. The same scenarios
 * and output exist for the gRPC connector, so the two can be compared.
 */
public final class ClientBench {

    private static final int PAYLOAD_BYTES = 256;
    private static final int AGGREGATES = 100;
    private static final int PER_AGGREGATE = 100;

    /** One operation; returns how many events it moved. */
    private interface Op {
        int run() throws Exception;
    }

    private static final String RUN = UUID.randomUUID().toString().substring(0, 8);
    private static final byte[] PAYLOAD = new byte[PAYLOAD_BYTES];

    private static EventStoreChannel store;
    private static CommandChannel commands;

    public static void main(String[] args) throws Exception {
        String scenario = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        int seconds = args.length > 3 ? Integer.parseInt(args[3]) : 5;
        int warmup = args.length > 4 ? Integer.parseInt(args[4]) : 3;

        KronosDbConnectionFactory factory = KronosDbConnectionFactory
                .forClient("client-bench", UUID.randomUUID().toString()).host(host).port(port).build();
        store = factory.connect("default").eventStoreChannel();
        KronosDbConnectionFactory handlerFactory = null;
        if (scenario.startsWith("cmd")) {
            commands = factory.connect("default").commandChannel();
            handlerFactory = KronosDbConnectionFactory
                    .forClient("client-bench-handler", UUID.randomUUID().toString()).host(host).port(port).build();
            handlerFactory.connect("default").commandChannel().registerCommandHandler(
                    command -> CompletableFuture.completedFuture(new CommandResult(
                            UUID.randomUUID().toString(), command.messageId(), "", null,
                            new Payload("Result", "1", ByteBuffer.wrap(PAYLOAD)), List.of(), List.of())),
                    100, UUID.randomUUID().toString(), "bench-handler", "BenchCommand").ackFuture().get();
            awaitHandler();
        }
        if (scenario.startsWith("read")) {
            seed();
        }

        int concurrency = Integer.parseInt(scenario.substring(scenario.lastIndexOf('c') + 1));
        Op op = switch (scenario.substring(0, scenario.lastIndexOf("-c"))) {
            case "append-1" -> () -> append(1);
            case "append-100" -> () -> append(100);
            case "read-tag" -> ClientBench::readTag;
            case "read-all" -> ClientBench::readAll;
            case "cmd" -> ClientBench::command;
            default -> throw new IllegalArgumentException("unknown scenario " + scenario);
        };

        run(op, concurrency, warmup, false, scenario);
        run(op, concurrency, seconds, true, scenario);

        if (handlerFactory != null) {
            handlerFactory.shutdown();
        }
        factory.shutdown();
        System.exit(0);
    }

    // ------------------------------------------------------------ the driver

    private static void run(Op op, int concurrency, int seconds, boolean report, String scenario) throws Exception {
        AtomicBoolean measuring = new AtomicBoolean(true);
        AtomicBoolean stop = new AtomicBoolean();
        long[][] latencies = new long[concurrency][];
        int[] counts = new int[concurrency];
        long[] events = new long[concurrency];
        Thread[] workers = new Thread[concurrency];
        for (int w = 0; w < concurrency; w++) {
            int worker = w;
            latencies[w] = new long[1 << 20];
            workers[w] = new Thread(() -> {
                int n = 0;
                long moved = 0;
                try {
                    while (!stop.get()) {
                        long start = System.nanoTime();
                        moved += op.run();
                        long elapsed = System.nanoTime() - start;
                        if (n < latencies[worker].length) {
                            latencies[worker][n++] = elapsed;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                counts[worker] = n;
                events[worker] = moved;
            }, "bench-" + w);
        }
        OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        if (report) {
            System.out.println("BENCH_START");
            System.out.flush();
        }
        long cpuBefore = os.getProcessCpuTime();
        long begin = System.nanoTime();
        for (Thread t : workers) {
            t.start();
        }
        Thread.sleep(seconds * 1000L);
        stop.set(true);
        for (Thread t : workers) {
            t.join();
        }
        double elapsed = (System.nanoTime() - begin) / 1e9;
        long cpu = os.getProcessCpuTime() - cpuBefore;
        if (!report) {
            return;
        }
        System.out.println("BENCH_END");
        System.out.flush();
        int total = Arrays.stream(counts).sum();
        long moved = Arrays.stream(events).sum();
        long[] all = new long[total];
        int at = 0;
        for (int w = 0; w < concurrency; w++) {
            System.arraycopy(latencies[w], 0, all, at, counts[w]);
            at += counts[w];
        }
        Arrays.sort(all);
        System.out.printf("RESULT %s %.0f %.0f %.0f %.0f %.1f %d %.3f%n", scenario, total / elapsed, moved / elapsed,
                all[total / 2] / 1e3, all[(int) (total * 0.99)] / 1e3, cpu / 1e3 / total, total, elapsed);
    }

    // ------------------------------------------------------------ operations

    private static int append(int batch) throws Exception {
        List<StoredRecord> events = new ArrayList<>(batch);
        long now = System.currentTimeMillis();
        for (int i = 0; i < batch; i++) {
            events.add(record(UUID.randomUUID().toString(), "BenchEvent",
                    tag("orderId", "o-" + (now % 1000) + "-" + i)));
        }
        EventStoreChannel.AppendTransaction tx = store.startAppendTransaction();
        tx.append(null, events);
        tx.commit().get();
        return batch;
    }

    private static int readTag() throws Exception {
        return drain(store.source(0, List.of(new Criterion(List.of(),
                List.of(tag("agg", RUN + "-" + ThreadLocalRandom.current().nextInt(AGGREGATES)))))));
    }

    private static int readAll() throws Exception {
        return drain(store.source(0, List.of(new Criterion(List.of(), List.of(tag("run", RUN))))));
    }

    /** Takes every event off a source and counts them, waking on arrival rather than polling. */
    private static int drain(ResultStream<Events> stream) throws Exception {
        Semaphore arrived = new Semaphore(0);
        stream.onAvailable(arrived::release);
        int count = 0;
        while (!stream.isCompleted()) {
            Events batch = stream.nextIfAvailable();
            if (batch == null) {
                arrived.tryAcquire(1, TimeUnit.SECONDS);
                continue;
            }
            for (StoredRecord record : batch.events()) {
                if (record.event().payload().remaining() != PAYLOAD_BYTES) {
                    throw new IllegalStateException("short event");
                }
                count++;
            }
        }
        if (stream.getError().isPresent()) {
            throw new IllegalStateException(stream.getError().get());
        }
        return count;
    }

    private static int command() throws Exception {
        CommandResult result = commands.sendCommand(new Command(UUID.randomUUID().toString(), "BenchCommand",
                System.currentTimeMillis(), new Payload("BenchCommand", "1", ByteBuffer.wrap(PAYLOAD)),
                List.of(), List.of(), "client-bench", "bench")).get();
        if (!result.errorCode().isEmpty()) {
            throw new IllegalStateException(result.errorCode());
        }
        return 1;
    }

    /** Waits until a command reaches the handler: its registration has to get into the routing table. */
    private static void awaitHandler() throws Exception {
        for (int attempt = 0; attempt < 200; attempt++) {
            try {
                command();
                return;
            } catch (Exception e) {
                Thread.sleep(50);
            }
        }
        throw new IllegalStateException("the command handler never became reachable");
    }

    private static void seed() throws Exception {
        for (int k = 0; k < AGGREGATES; k++) {
            List<StoredRecord> events = new ArrayList<>(PER_AGGREGATE);
            for (int i = 0; i < PER_AGGREGATE; i++) {
                events.add(record(UUID.randomUUID().toString(), "BenchEvent",
                        tag("agg", RUN + "-" + k), tag("run", RUN)));
            }
            EventStoreChannel.AppendTransaction tx = store.startAppendTransaction();
            tx.append(null, events);
            tx.commit().get();
        }
    }

    private static Tag tag(String key, String value) {
        return new Tag(ByteBuffer.wrap(key.getBytes(StandardCharsets.UTF_8)),
                ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static StoredRecord record(String id, String name, Tag... tags) {
        return new StoredRecord(0, new EventRecord(0, id, name, "1", System.currentTimeMillis(), List.of(),
                List.of(tags), ByteBuffer.wrap(PAYLOAD)));
    }
}
