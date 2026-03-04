package io.cloudNativeData.spring.rabbit.streams;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent;
import io.cloudNativeData.spring.rabbit.streams.domain.ai.SpringIOEventSurvey;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.ChatOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;

import java.util.List;


@Configuration
@Slf4j
public class AiFilterConfig {

    private final String prompt = """
            Given the following events from the Spring IO Conference.
            
            Provides you opinion on Talk. Indicate
            whether you think it was good, bad or just OK.
            
            
            [events]
            ```json
            {events}
            ```
            """;

    private final static String sqlFilter = """
            session = 'rabbit' AND year = 2026
            """;

    @Value("${streamName:events-1}")
    private String streamName;

    @Bean
    Environment amqpEnvironment() {
        return new AmqpEnvironmentBuilder()
                .connectionSettings()
                .environmentBuilder()
                .build();
    }

    @Bean("alertConnection")
    Connection alertConnection(Environment environment) {
        return environment.connectionBuilder()
                .build();
    }

    @Bean
    Consumer filterAmqConsumer(Connection connection,
                               java.util.function.Consumer<SpringIoEvent> consumerService,
                               Converter<byte[], SpringIoEvent> converter) {

        log.info("input consumed with SQL '{}' from input stream {}", sqlFilter, streamName);

        var builder = connection.consumerBuilder()
                .queue(streamName)
                .stream()
                .offset(ConsumerBuilder.StreamOffsetSpecification.FIRST);

        return builder
                .filter()
                .sql(sqlFilter)
                .stream()
                .builder().messageHandler((ctx, inputMessage) -> {
                    //Processing input message
                    consumerService.accept(converter.convert(inputMessage.body()));

                })
                .build();
    }

    @Bean
    ChatClient chatClient(ChatModel chatModel) {

        return ChatClient
                .builder(chatModel)
                .defaultOptions(ChatOptions.builder()
                        .build())
                .build();
    }

    @Bean
    java.util.function.Consumer<SpringIoEvent> aiEventsConsumer(List<SpringIoEvent> events,
                                                                ChatClient chatClient) {
        return event -> {
            log.info("Received SpringIoEvent {}", event);
            events.add(event);
            if (events.size() >= 8) {

                log.info("********* HERE IS THE VERDICT ***********\n Wait for IT!");

                var verdict = chatClient.prompt()
                        .user(u -> u.text(prompt)
                                .param("events", events))
                        .call()
                        .entity(SpringIOEventSurvey.class);

                log.info("*********\nSurvey verdict: {}\n***************", verdict);
            }
        };
    }

}
