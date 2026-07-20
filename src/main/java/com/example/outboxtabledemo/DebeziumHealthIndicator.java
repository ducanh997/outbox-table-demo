package com.example.outboxtabledemo;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class DebeziumHealthIndicator implements HealthIndicator {

    private final DebeziumEngineConfig engineConfig;

    public DebeziumHealthIndicator(DebeziumEngineConfig engineConfig) {
        this.engineConfig = engineConfig;
    }

    @Override
    public Health health() {
        if (engineConfig.isRunning()) {
            return Health.up()
                    .withDetail("engine", "running")
                    .build();
        }
        return Health.down()
                .withDetail("engine", "stopped")
                .withDetail("reason", "Debezium engine completed unexpectedly; restart required")
                .build();
    }
}
