package com.vinil.kafka.explorer.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

@Slf4j
@Service
public class ReactiveProducer {

    private final ReactiveKafkaProducerTemplate<String, Object> reactiveKafkaProducerTemplate;

    public ReactiveProducer(final ReactiveKafkaProducerTemplate<String, Object> reactiveKafkaProducerTemplate) {
        this.reactiveKafkaProducerTemplate = reactiveKafkaProducerTemplate;
    }

    public <T> Flux<SenderResult<T>> publishData(final String topic, final String key, final Flux<T> valueFlux) {

        log.info("Start pushing the data for the key {} to the kafka topic {}", key, topic);
        return reactiveKafkaProducerTemplate.send(valueFlux.map(value -> SenderRecord.create(new ProducerRecord<>(topic, key, value), value)))
                .doOnError(e -> log.error("Failed to push the object to kafka topic: {}", e.getMessage()))
                .doOnNext(r ->
                        log.info("Successfully published to the kafka topic {} for the key {} and metadata class is {}",
                                r.recordMetadata().topic(),
                                key,
                                r.correlationMetadata().getClass()
                        )
                );
    }
}
