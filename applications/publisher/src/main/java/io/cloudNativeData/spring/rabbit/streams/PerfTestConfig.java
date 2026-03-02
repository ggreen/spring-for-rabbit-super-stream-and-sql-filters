package io.cloudNativeData.spring.rabbit.streams;

import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.rabbit.stream.producer.RabbitStreamTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.nio.charset.StandardCharsets;

@Configuration
@Slf4j
public class PerfTestConfig {

    /**
     * 3 Million
     */
    private static final Long loopCount = 3000000L;

    @Value("${stream:events.spring.io}")
    private String streamName;
    @Value("${bathSize}")
    private int batchSize;
    @Value("${subBatchSize}")
    private int subBatchSize;


    @Bean
    RabbitStreamTemplate template(Environment environment){
        var template = new RabbitStreamTemplate(environment, streamName);
        template.setProducerCustomizer((stream, builder) -> {

            builder.batchSize(batchSize); //722000
            builder.subEntrySize(subBatchSize);
        });

        return template;
    };


    @Bean
    Message msg(RabbitStreamTemplate template, JsonMapper  mapper)
    {
        var event = new SpringIoEvent("STARTED perftest");
        var msgPayload = mapper.writeValueAsString(event);
        log.info("Sending event: {}", msgPayload);

        return template.messageBuilder().addData(msgPayload.getBytes(StandardCharsets.UTF_8))
                .build();
    }

    @Bean
    ApplicationRunner applicationRunner(RabbitStreamTemplate template, Message streamMsg)
    {
        return args -> {

            var start = System.currentTimeMillis();

            for (int i=0; i< loopCount; i++) {

                 template.send(streamMsg);
            }

            var end = System.currentTimeMillis();

            log.info("{} per second", (loopCount/(end - start))*1000);
        };
    }

}
