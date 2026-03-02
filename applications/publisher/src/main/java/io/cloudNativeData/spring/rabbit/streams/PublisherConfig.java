package io.cloudNativeData.spring.rabbit.streams;

import com.rabbitmq.stream.Environment;
import io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;

//@Configuration
@Slf4j
public class PublisherConfig {

    @Value("${stream:events.spring.io}")
    private String streamName;


    @Bean
    RabbitStreamTemplate template(Environment environment){
        var template = new RabbitStreamTemplate(environment, streamName);
        template.setMessageConverter(new JacksonJsonMessageConverter());
        return template;
    };

    @Bean
    ApplicationRunner applicationRunner(RabbitStreamTemplate template) {
      return args -> {
        var results = template.convertAndSend(new SpringIoEvent("STARTED Rabbit demo"));

        log.info("Sent with results: {}", results.get());
      };
    }
}
