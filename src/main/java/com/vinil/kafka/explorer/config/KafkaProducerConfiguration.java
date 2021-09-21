package com.vinil.kafka.explorer.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

import java.util.Map;

@Configuration
public class KafkaProducerConfiguration {
	@Bean
	public ReactiveKafkaProducerTemplate<String, Object> reactiveKafkaProducerTemplate(
			KafkaProperties properties) {
		final Map<String, Object> props = properties.buildProducerProperties();
		return new ReactiveKafkaProducerTemplate<String, Object>(SenderOptions.create(props));
	}
}
