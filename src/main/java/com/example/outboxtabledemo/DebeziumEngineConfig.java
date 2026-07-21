package com.example.outboxtabledemo;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.DebeziumEngine.ConnectorCallback;
import io.debezium.engine.format.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Configuration
@EnableConfigurationProperties(DebeziumProperties.class)
public class DebeziumEngineConfig implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DebeziumEngineConfig.class);

    private static final int PHASE = Integer.MAX_VALUE - 100;
    private static final long STOP_TIMEOUT_SECONDS = 15;

    private final DebeziumEngine<ChangeEvent<String, String>> engine;
    private final ExecutorService executor;
    private final ExecutorService closeExecutor;
    private final ConcurrentHashMap<String, ExecutorService> tableWorkers = new ConcurrentHashMap<>();
    private volatile boolean running = false;
    private final AtomicBoolean stopping = new AtomicBoolean(false);

    public DebeziumEngineConfig(DebeziumProperties props,
                                Consumer<ChangeEvent<String, String>> consumer) {
        this.executor = Executors.newSingleThreadExecutor(daemonThreadFactory("debezium-engine"));
        this.closeExecutor = Executors.newSingleThreadExecutor(daemonThreadFactory("debezium-engine-close"));
        this.engine = DebeziumEngine.create(Json.class)
                .using(props.toProperties())
                .using(completionCallback())
                .using(connectorCallback())
                .notifying(new DebeziumEngine.ChangeConsumer<>() {
                    private int retryCount = 0;

                    @Override
                    public void handleBatch(List<ChangeEvent<String, String>> records,
                                           DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
                        List<CompletableFuture<Void>> futures = records.stream()
                                .map(record -> CompletableFuture.runAsync(
                                        () -> consumer.accept(record),
                                        tableWorkers.computeIfAbsent(record.destination(),
                                                k -> Executors.newSingleThreadExecutor(daemonThreadFactory("debezium-worker-" + k)))))
                                .toList();
                        try {
                            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
                            for (var record : records) {
                                committer.markProcessed(record);
                            }
                            retryCount = 0;
                        } catch (CompletionException e) {
                            long delay = Math.min(1000L * (1L << retryCount++), 30_000L);
                            log.warn("Failed to process batch, retry in {}ms", delay, e.getCause());
                            Thread.sleep(delay);
                        } finally {
                            committer.markBatchFinished();
                        }
                    }
                })
                .build();
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    @Override
    public void start() {
        log.info("Starting Debezium engine");
        running = true;
        executor.execute(engine);
    }

    @Override
    public void stop(Runnable callback) {
        try {
            stop();
        } finally {
            callback.run();
        }
    }

    @Override
    public void stop() {
        if (!stopping.compareAndSet(false, true)) {
            return;
        }
        running = false;
        log.info("Stopping Debezium engine");
        closeEngineWithTimeout();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Debezium engine did not terminate within {}s, forcing shutdown",
                        STOP_TIMEOUT_SECONDS);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        } finally {
            closeExecutor.shutdownNow();
            tableWorkers.values().forEach(ExecutorService::shutdownNow);
        }
    }

    private void closeEngineWithTimeout() {
        Future<?> closeFuture = closeExecutor.submit(() -> {
            try {
                engine.close();
            } catch (IOException e) {
                log.warn("Error while closing Debezium engine", e);
            }
        });
        try {
            closeFuture.get(STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.warn("engine.close() did not return within {}s, forcing shutdown",
                    STOP_TIMEOUT_SECONDS);
            closeExecutor.shutdownNow();
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for engine.close()", e);
            closeExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.warn("Error waiting for engine.close()", e);
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private CompletionCallback completionCallback() {
        return (success, message, error) -> {
            running = false;
            if (success) {
                log.info("Debezium engine completed successfully: {}", message);
            } else {
                log.error("Debezium engine completed with error: {}", message, error);
            }
        };
    }

    private ConnectorCallback connectorCallback() {
        return new ConnectorCallback() {
            @Override
            public void connectorStarted() {
                log.info("Debezium connector started");
            }

            @Override
            public void connectorStopped() {
                log.info("Debezium connector stopped");
            }
        };
    }

    private static ThreadFactory daemonThreadFactory(String namePrefix) {
        AtomicInteger counter = new AtomicInteger(0);
        return r -> {
            Thread t = new Thread(r, namePrefix + "-" + counter.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
    }
}
