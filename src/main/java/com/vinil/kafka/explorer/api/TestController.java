package com.vinil.kafka.explorer.api;

import com.vinil.kafka.explorer.producer.ReactiveProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

@RestController
@Slf4j
public class TestController {
    @Autowired
    private ReactiveProducer reactiveProducer;

    @GetMapping("/test")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void test(){
        log.info("Inside Test Controller");
        reactiveProducer.publishData("test-details","12345", Flux.just("Test data 17sep2021 1"))
                .doOnError(error ->{
                    log.error("Failed to publish test details : {}: Message : {}", "12345", error.getMessage());
                })
                .subscribe();
    }

    @GetMapping("/test_max_size")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void testMaxSize(){
        log.info("Inside Test Controller");
        Path path = Paths.get("src/main/resources/test1.json");
        long bytes = 0;
        try {
            bytes = Files.size(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info(String.format("------------------------------>%,d bytes", bytes));
        Flux<String> stringFlux = Flux.using(
                () -> Files.lines(path),
                Flux::fromStream,
                Stream::close
        );
        reactiveProducer.publishData("test-details","100001", stringFlux)
                .doOnError(error ->{
                    log.error("Failed to publish test details : {}: Message : {}", "12345", error.getMessage());
                })
                .subscribe();
    }

}
