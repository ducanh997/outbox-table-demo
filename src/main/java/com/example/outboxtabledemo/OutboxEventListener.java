package com.example.outboxtabledemo;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.engine.ChangeEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Component
public class OutboxEventListener implements Consumer<ChangeEvent<String, String>> {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventListener.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MeterRegistry registry;
    private final String connectorName;
    private final ConcurrentHashMap<String, Timer> timers = new ConcurrentHashMap<>();

    public OutboxEventListener(MeterRegistry registry,
                                @Value("${outbox.consumer.name}") String connectorName) {
        this.registry = registry;
        this.connectorName = connectorName;
    }

    private Timer timerFor(String table) {
        return timers.computeIfAbsent(table, t -> Timer.builder("outbox.event.latency")
                .description("Latency from row created_at (app emit time) to consumer receive")
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .tags(Tags.of("connector", connectorName, "table", t))
                .register(registry));
    }

    private void counter(String name, String table, String reason) {
        registry.counter(name, Tags.of("connector", connectorName, "table", table, "reason", reason))
                .increment();
    }

    @Override
    public void accept(ChangeEvent<String, String> event) {
        String key = event.key();
        String table = "unknown";
        try {
            String value = event.value();
            if (value == null) {
                counter("outbox.event.skipped", table, "null_value");
                return;
            }

            JsonNode root = MAPPER.readTree(value);
            JsonNode payload = root.path("payload");
            JsonNode source = payload.path("source");
            if (source.isMissingNode()) {
                counter("outbox.event.skipped", table, "missing_source");
                return;
            }

            table = source.path("table").asText("unknown");
            String op = payload.path("op").asText("");
            JsonNode after = payload.path("after");
            if (after.isMissingNode() || after.isNull()) {
                counter("outbox.event.skipped", table, "missing_after");
                return;
            }

            if (!"c".equals(op)) {
                counter("outbox.event.skipped", table, "non_create_op");
                return;
            }

            JsonNode createdAtNode = after.path("created_at");
            if (createdAtNode.isMissingNode() || createdAtNode.isNull()) {
                counter("outbox.event.skipped", table, "missing_created_at");
                return;
            }

            Instant createdAt = parseCreatedAt(createdAtNode);
            if (createdAt == null) {
                counter("outbox.event.failed", table, "invalid_created_at");
                log.warn("Invalid created_at value: key={}, value={}", key, createdAtNode);
                return;
            }

            long delayMs = Duration.between(createdAt, Instant.now()).toMillis();
            if (delayMs < 0) {
                counter("outbox.event.skipped", table, "future_created_at");
                log.warn("created_at is in the future: key={}, delayMs={}", key, delayMs);
                return;
            }

            log.info("event received: table={}, key={}, delay={}ms", table, key, delayMs);
            timerFor(table).record(Duration.ofMillis(delayMs));
            registry.counter("outbox.event.received",
                    Tags.of("connector", connectorName, "table", table)).increment();
        } catch (Exception e) {
            counter("outbox.event.failed", table, "exception");
            log.warn("Failed to process event: key={}", key, e);
        }
    }

    private static Instant parseCreatedAt(JsonNode node) {
        String text = node.asText("");
        if (text.isEmpty()) {
            return null;
        }
        try {
            return OffsetDateTime.parse(text).toInstant();
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
