package com.example.outboxtabledemo;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.CompletionCallback;
import io.debezium.engine.DebeziumEngine.ConnectorCallback;
import io.debezium.engine.format.Json;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

@Configuration
@EnableConfigurationProperties(DebeziumProperties.class)
public class DebeziumEngineConfig implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DebeziumEngineConfig.class);

    private static final int PHASE = Integer.MAX_VALUE - 100;
    private static final long STOP_TIMEOUT_SECONDS = 15;
    private static final int MAX_RETRY_EXPONENT = 5;

    private final DebeziumEngine<ChangeEvent<String, String>> engine;
    private final ExecutorService executor;
    private final ExecutorService closeExecutor;
    private volatile boolean running = false;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final ConfigurableApplicationContext applicationContext;
    private final boolean enabled;

    public DebeziumEngineConfig(DebeziumProperties props,
                                Consumer<ChangeEvent<String, String>> consumer,
                                ConfigurableApplicationContext applicationContext,
                                @Value("${debezium.enabled:true}") boolean enabled) {
        this.applicationContext = applicationContext;
        this.enabled = enabled;
        this.executor = Executors.newSingleThreadExecutor(daemonThreadFactory("debezium-engine"));
        this.closeExecutor = Executors.newSingleThreadExecutor(daemonThreadFactory("debezium-engine-close"));
        this.engine = DebeziumEngine.create(Json.class)
                .using(props.toProperties())
                .using(completionCallback())
                .using(connectorCallback())
                .notifying(new DebeziumEngine.ChangeConsumer<>() {
                    @Override
                    public void handleBatch(List<ChangeEvent<String, String>> records,
                                           DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
                        try {
                            for (var record : records) {
                                processRecordWithRetry(record, consumer, committer);
                            }
                        }
                        finally {
                            committer.markBatchFinished();
                        }
                    }
                })
                .build();
    }

    @Override
    public boolean isAutoStartup() {
        return enabled;
    }

    @Override
    public int getPhase() {
        return PHASE;
    }

    @Override
    public void start() {
        if (!enabled) {
            log.info("Debezium engine is disabled");
            return;
        }
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
                if (!stopping.get()) {
                    log.error("Debezium engine failed unexpectedly — shutting down application");
                    Thread shutdownThread = new Thread(
                            () -> System.exit(SpringApplication.exit(applicationContext, () -> 1)),
                            "debezium-shutdown");
                    shutdownThread.setDaemon(false);
                    shutdownThread.start();
                }
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

    private void processRecordWithRetry(ChangeEvent<String, String> record,
                                        Consumer<ChangeEvent<String, String>> consumer,
                                        DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> committer) throws InterruptedException {
        int attempts = 0;
        while (true) {
            try {
                consumer.accept(record);
                committer.markProcessed(record);
                return;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Interrupted during record processing");
                }
                long backoff = computeBackoff(attempts++);
                log.warn("Failed to process record (attempt={}); retrying after {}ms", attempts, backoff, e);
                Thread.sleep(backoff);
            }
        }
    }

    private long computeBackoff(int attempts) {
        long baseDelay = 1000L * (1L << Math.min(attempts, MAX_RETRY_EXPONENT));
        return (long) (baseDelay * ThreadLocalRandom.current().nextDouble(0.8, 1.2));
    }
}
