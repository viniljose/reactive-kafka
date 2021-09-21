package com.vinil.kafka.explorer.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;

@Configuration
public class KafkaConsumerConfiguration {

    @Bean
    public ReceiverOptions<String, Object> kafkaReceiverOptions(@Value(value = "${TOPIC_NAME}") String topic, KafkaProperties kafkaProperties) {
        ReceiverOptions<String, Object> basicReceiverOptions = ReceiverOptions.create(kafkaProperties.buildConsumerProperties());
        return basicReceiverOptions.subscription(Collections.singletonList(topic));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, Object> reactiveKafkaConsumerTemplate(ReceiverOptions<String, Object> kafkaReceiverOptions) {
        return new ReactiveKafkaConsumerTemplate<String, Object>(kafkaReceiverOptions);
    }
}
