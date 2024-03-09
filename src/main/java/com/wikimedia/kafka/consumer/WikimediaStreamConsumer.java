package com.wikimedia.kafka.consumer;

import com.wikimedia.kafka.producer.WikimediaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

@Service
@Slf4j
public class WikimediaStreamConsumer {

    private final WebClient webClientConfig;
    private final WikimediaProducer producer;

    private WikimediaStreamConsumer(WebClient.Builder webClientBuilder, WikimediaProducer producer){
        this.webClientConfig = webClientBuilder
                .baseUrl("https://stream.wikimedia.org/v2")
                .build();
        this.producer = producer;
    }

    public void consumeStreamAndPublish(){
        webClientConfig.get()
                .uri("/stream/recentchange")
                .retrieve()
                .bodyToFlux(String.class)
                .subscribe(producer::sendMessage);
    }
}
