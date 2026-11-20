package com.example.streaming;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JobConfigTest {

    private static Map<String, String> baseEnv() {
        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_BOOTSTRAP", "kafka:9092");
        env.put("KAFKA_TOPIC", "order-events");
        env.put("DORIS_FENODES", "fe:8030");
        env.put("DORIS_DATABASE", "demo");
        env.put("DORIS_TABLE", "order_events");
        env.put("DORIS_USERNAME", "root");
        env.put("DORIS_PASSWORD", "p'ass");
        return env;
    }

    @Test
    void buildsKafkaAndDorisSql() {
        JobConfig cfg = JobConfig.from(baseEnv());
        String kafka = cfg.kafkaSourceDdl();
        String doris = cfg.dorisSinkDdl();

        assertTrue(kafka.contains("'topic' = 'order-events'"));
        assertTrue(kafka.contains("'properties.bootstrap.servers' = 'kafka:9092'"));
        assertTrue(doris.contains("'table.identifier' = 'demo.order_events'"));
        assertTrue(doris.contains("'fenodes' = 'fe:8030'"));
        // 密码里的单引号必须被转义，否则 SQL 语法会坏
        assertTrue(doris.contains("'password' = 'p''ass'"));
        assertTrue(doris.contains("'sink.enable-2pc' = 'true'"));
    }

    @Test
    void rejectsUnsafeTableIdentifier() {
        Map<String, String> env = baseEnv();
        env.put("DORIS_TABLE", "order_events; DROP TABLE x");
        assertThrows(IllegalArgumentException.class, () -> JobConfig.from(env));
    }

    @Test
    void insertFiltersBadRowsAndDerivesDate() {
        JobConfig cfg = JobConfig.from(baseEnv());
        String insert = cfg.insertSql();
        assertTrue(insert.contains("CAST(event_ts AS DATE)"));
        assertTrue(insert.contains("event_ts IS NOT NULL"));
        assertTrue(insert.contains("event_id IS NOT NULL"));
    }
}
