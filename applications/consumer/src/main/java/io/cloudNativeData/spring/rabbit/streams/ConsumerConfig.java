package io.cloudNativeData.spring.rabbit.streams;

import io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.function.Consumer;

@Configuration
@Slf4j
public class ConsumerConfig {

    @Bean
    Consumer<SpringIoEvent> logConsumer()
    {
      return event -> { log.info("Received SpringIoEvent {}", event); };
    }
}
