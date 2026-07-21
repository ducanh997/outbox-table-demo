package com.example.outboxtabledemo;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = "debezium.enabled=false")
class OutboxTableDemoApplicationTests {

    @Test
    void contextLoads() {
    }

}
