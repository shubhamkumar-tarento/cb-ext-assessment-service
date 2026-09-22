package com.igot.cb.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ProducerConfigurationTest {

    @Test
    void testProducerConfigs() {
        ProducerConfiguration config = new ProducerConfiguration();
        ReflectionTestUtils.setField(config, "kafkabootstrapAddress", "localhost:9092");

        Map<String, Object> props = config.producerFactory().getConfigurationProperties();
        assertEquals("localhost:9092", props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class, props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals(StringSerializer.class, props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    }
    @Test
    void testKafkaTemplate() {
        ProducerConfiguration config = new ProducerConfiguration();
        ReflectionTestUtils.setField(config, "kafkabootstrapAddress", "localhost:9092");

        KafkaTemplate<String, String> template = config.kafkaTemplate();
        assertNotNull(template);
    }
}