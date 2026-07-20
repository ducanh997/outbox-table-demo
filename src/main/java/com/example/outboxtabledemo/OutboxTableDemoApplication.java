package com.example.outboxtabledemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Clock;
import java.time.ZoneOffset;

@SpringBootApplication
public class OutboxTableDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(OutboxTableDemoApplication.class, args);
    }

    @Bean
    public Clock clock() {
        return Clock.system(ZoneOffset.UTC);
    }
}
