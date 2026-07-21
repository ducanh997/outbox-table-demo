package com.example.outboxtabledemo;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LoadGenerator {

    private static final DateTimeFormatter MYSQL_DATETIME =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    public static void main(String[] args) throws Exception {
        Properties props = loadApplicationProperties();
        String host = props.getProperty("debezium.props[database.hostname]", "localhost");
        String port = props.getProperty("debezium.props[database.port]", "3306");
        String user = props.getProperty("debezium.props[database.user]");
        String pass = props.getProperty("debezium.props[database.password]");
        if (user == null || pass == null) {
            System.err.println("Missing database.user or database.password in application.properties");
            System.exit(1);
        }

        int threads = Integer.parseInt(System.getProperty("threads", "10"));
        int perThread = Integer.parseInt(System.getProperty("perThread", "1000"));
        String table = System.getProperty("table", "outbox_blackhole");
        boolean useSp = Boolean.parseBoolean(System.getProperty("useSp", "false"));

        if (threads <= 0 || perThread <= 0) {
            System.err.println("threads and perThread must be positive");
            System.exit(1);
        }

        if (!table.matches("^[a-zA-Z_][a-zA-Z0-9_]*$")) {
            System.err.println("Invalid table name: " + table);
            System.exit(1);
        }

        String url = "jdbc:mysql://" + host + ":" + port + "/obt";
        int total = threads * perThread;
        String mode = useSp ? "stored-procedure" : "insert";
        System.out.println(">>> Load test: " + threads + " threads x " + perThread + " = " + total
                + " total | table=" + table + " | mode=" + mode);
        System.out.println(">>> Start timestamp (ms): " + System.currentTimeMillis());

        ExecutorService exec = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicLong inserted = new AtomicLong();
        AtomicInteger failures = new AtomicInteger();
        long start = System.nanoTime();

        String sql;
        if (useSp && "outbox_innodb".equals(table)) {
            sql = "{CALL emit_outbox_innodb('Order', ?, 'OrderCreated', ?, ?)}";
        } else {
            sql = "INSERT INTO " + table
                    + " (id, aggregate_type, aggregate_id, event_type, payload, created_at) "
                    + "VALUES (UUID(), 'Order', ?, 'OrderCreated', ?, ?)";
        }

        try {
            for (int t = 0; t < threads; t++) {
                final int tid = t;
                exec.submit(() -> {
                    try (Connection conn = DriverManager.getConnection(url, user, pass);
                         PreparedStatement ps = conn.prepareStatement(sql)) {
                        for (int i = 0; i < perThread; i++) {
                            long seq = tid * 1_000_000L + i;
                            String payload = "{\"ts_ms\":" + System.currentTimeMillis()
                                    + ",\"seq\":" + seq
                                    + ",\"amount\":" + (i % 100) + "}";
                            String createdAt = MYSQL_DATETIME.format(Instant.now());
                            ps.setString(1, "order-" + seq);
                            ps.setString(2, payload);
                            ps.setString(3, createdAt);
                            ps.executeUpdate();
                            inserted.incrementAndGet();
                        }
                    } catch (Exception e) {
                        failures.incrementAndGet();
                        System.err.println("Thread " + tid + " error:");
                        e.printStackTrace();
                    } finally {
                        latch.countDown();
                    }
                });
            }

            boolean completed = latch.await(60, TimeUnit.SECONDS);
            double elapsedSec = (System.nanoTime() - start) / 1_000_000_000.0;
            long insertedCount = inserted.get();
            int failureCount = failures.get();
            if (!completed) {
                System.err.println(String.format(
                        ">>> Load test timed out after 60s: %d/%d inserted in %.2fs",
                        insertedCount, total, elapsedSec));
                System.exit(1);
            }
            double failureRate = total > 0 ? (double) failureCount / total : 0;
            System.out.printf(">>> Done: %d/%d inserted in %.2fs (%.0f ops/s) | failures: %d (%.1f%%)%n",
                    insertedCount, total, elapsedSec,
                    elapsedSec > 0 ? insertedCount / elapsedSec : 0,
                    failureCount, failureRate * 100);
            System.out.println(">>> End timestamp (ms): " + System.currentTimeMillis());
            System.out.println(">>> Now watch the Spring Boot app log for latency stats.");
        } finally {
            exec.shutdownNow();
        }
    }

    private static Properties loadApplicationProperties() throws Exception {
        Properties props = new Properties();
        try (InputStream in = LoadGenerator.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (in == null) {
                System.err.println("application.properties not found on classpath");
                System.exit(1);
            }
            props.load(in);
        }
        return props;
    }

}
