package io.cloudNativeData.spring.rabbit.streams;

import io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.function.context.config.JsonMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.converter.JacksonJsonMessageConverter;
import org.springframework.messaging.converter.MessageConverter;

import java.util.function.Consumer;

//@Configuration
@Slf4j
public class ConsumerConfig {


    @Bean
    Consumer<SpringIoEvent> logConsumer()
    {
      return event -> { log.info("Received SpringIoEvent {}", event); };
    }
}
