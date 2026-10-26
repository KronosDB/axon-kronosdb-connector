package io.kronosdb.connector.bench;

import io.kronosdb.connector.transport.EventStoreChannel;
import io.kronosdb.connector.transport.KronosDbConnectionFactory;
import io.kronosdb.protocol.EventRecord;
import io.kronosdb.protocol.StoredRecord;
import io.kronosdb.protocol.Tag;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Raw event store client benchmark: appends against a running server through
 * {@link EventStoreChannel}, with no Axon in the way.
 *
 * <pre>
 * java -cp ... io.kronosdb.connector.bench.AppendBench HOST PORT [SECONDS]
 * </pre>
 *
 * Each scenario runs its workers flat out for SECONDS after a warm-up of the
 * same length, and reports appends per second, events per second and the
 * p50/p99 latency of one append round trip (commit to response).
 */
public final class AppendBench {

    private static final int PAYLOAD_BYTES = 256;

    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int seconds = args.length > 2 ? Integer.parseInt(args[2]) : 5;

        KronosDbConnectionFactory factory = KronosDbConnectionFactory
                .forClient("append-bench", UUID.randomUUID().toString())
                .host(host)
                .port(port)
                .build();
        EventStoreChannel channel = factory.connect("default").eventStoreChannel();

        System.out.printf("%-22s %10s %12s %10s %10s%n", "scenario", "appends/s", "events/s", "p50", "p99");
        for (int[] scenario : new int[][]{{1, 1}, {16, 1}, {64, 1}, {1, 100}}) {
            int concurrency = scenario[0];
            int batch = scenario[1];
            run(channel, concurrency, batch, seconds, false);
            run(channel, concurrency, batch, seconds, true);
        }
        factory.shutdown();
    }

    private static void run(EventStoreChannel channel, int concurrency, int batch, int seconds, boolean report)
            throws Exception {
        AtomicBoolean stop = new AtomicBoolean();
        long[][] latencies = new long[concurrency][];
        int[] counts = new int[concurrency];
        Thread[] workers = new Thread[concurrency];
        for (int w = 0; w < concurrency; w++) {
            int worker = w;
            latencies[w] = new long[1 << 20];
            workers[w] = new Thread(() -> {
                byte[] payload = new byte[PAYLOAD_BYTES];
                int n = 0;
                try {
                    while (!stop.get()) {
                        List<StoredRecord> events = events(batch, payload);
                        long start = System.nanoTime();
                        EventStoreChannel.AppendTransaction tx = channel.startAppendTransaction();
                        tx.append(null, events);
                        tx.commit().get();
                        long elapsed = System.nanoTime() - start;
                        if (n < latencies[worker].length) {
                            latencies[worker][n++] = elapsed;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                counts[worker] = n;
            }, "bench-" + w);
        }
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
        if (!report) {
            return;
        }
        int total = Arrays.stream(counts).sum();
        long[] all = new long[total];
        int at = 0;
        for (int w = 0; w < concurrency; w++) {
            System.arraycopy(latencies[w], 0, all, at, counts[w]);
            at += counts[w];
        }
        Arrays.sort(all);
        String name = batch == 1 ? "append x1, c=" + concurrency : "append x" + batch + ", c=" + concurrency;
        System.out.printf("%-22s %10.0f %12.0f %8.0fus %8.0fus%n", name, total / elapsed,
                total * (double) batch / elapsed, all[total / 2] / 1e3, all[(int) (total * 0.99)] / 1e3);
    }

    private static List<StoredRecord> events(int batch, byte[] payload) {
        List<StoredRecord> events = new ArrayList<>(batch);
        long now = System.currentTimeMillis();
        for (int i = 0; i < batch; i++) {
            events.add(new StoredRecord(0, new EventRecord(
                    0,
                    UUID.randomUUID().toString(),
                    "BenchEvent",
                    "1",
                    now,
                    List.of(),
                    List.of(new Tag(
                            ByteBuffer.wrap("orderId".getBytes(StandardCharsets.UTF_8)),
                            ByteBuffer.wrap(("o-" + (now % 1000) + "-" + i).getBytes(StandardCharsets.UTF_8)))),
                    ByteBuffer.wrap(payload))));
        }
        return events;
    }
}
