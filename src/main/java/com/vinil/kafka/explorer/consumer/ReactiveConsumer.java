package com.vinil.kafka.explorer.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Slf4j
@Service
public class ReactiveConsumer implements CommandLineRunner {
    private final ReactiveKafkaConsumerTemplate<String, Object> reactiveKafkaConsumerTemplate;

    public ReactiveConsumer(ReactiveKafkaConsumerTemplate<String, Object> reactiveKafkaConsumerTemplate) {
        this.reactiveKafkaConsumerTemplate = reactiveKafkaConsumerTemplate;
    }

    private Flux<Object> consumeData() {
        return reactiveKafkaConsumerTemplate
                .receiveAutoAck()
                // .delayElements(Duration.ofSeconds(2L)) // BACKPRESSURE
                .doOnNext(consumerRecord -> log.info("received key={}, value={} from topic={}, offset={}",
                        consumerRecord.key(),
                        consumerRecord.value(),
                        consumerRecord.topic(),
                        consumerRecord.offset())
                )
                .map(ConsumerRecord::value)
                .doOnNext(object -> log.info("successfully consumed {}={}", Object.class.getSimpleName(), object))
                .doOnError(throwable -> log.error("something bad happened while consuming : {}", throwable.getMessage()));
        //.subscribe();

    }

    @Override
    public void run(String... args) {
        // we have to trigger consumption
        consumeData().subscribe();
    }
}
