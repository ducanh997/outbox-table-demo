package com.example.outboxtabledemo;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@ConfigurationProperties(prefix = "debezium")
public class DebeziumProperties {

    private final Map<String, String> props = new HashMap<>();

    public Map<String, String> getProps() {
        return props;
    }

    public Properties toProperties() {
        Properties p = new Properties();
        p.putAll(props);
        return p;
    }
}
