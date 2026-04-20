package io.cloudNativeData.spring.rabbit.streams;

import com.rabbitmq.client.amqp.Connection;
import com.rabbitmq.client.amqp.Consumer;
import com.rabbitmq.client.amqp.ConsumerBuilder;
import com.rabbitmq.client.amqp.Environment;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import com.rabbitmq.stream.OffsetSpecification;
import io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent;
import io.cloudNativeData.spring.rabbit.streams.domain.ai.SpringIOEventSurvey;
import lombok.extern.slf4j.Slf4j;
import nyla.solutions.core.patterns.integration.Subscriber;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.prompt.ChatOptions;
import org.springframework.amqp.rabbit.listener.MessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.config.ListenerContainerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.rabbit.stream.listener.StreamListenerContainer;

import java.util.List;

@Configuration
@Slf4j
public class ConsumerConfig {

    //TODO: Remove application properties settings

    //Zen's SQL = session = 'rabbit' AND year = 2026
    //TODO: Remove application properties settings

    private final String prompt = """
                Given the following events from the Spring IO Conference.
                Provides your opinion on the Talk. Indicate
                whether you think it was GOOD,OK,BAD.
            
                [events]
                ```json
                {events}
                ```
                Provide a summarized opinion of the talk based on these events.
                Include events details to explain your opinion.
                Use a minimum of 7 words to summarize your opinion.
            """;

    private final static String sqlFilter = """
             session = 'rabbit' AND year = 2026
            """;

    @Value("${stream.name:events.super.streams.filtering-0}")
    private String streamName0;
    @Value("${stream.name:events.super.streams.filtering-1}")
    private String streamName1;


    @Bean
    Environment amqpEnvironment() {
        return new AmqpEnvironmentBuilder()
                .connectionSettings()
                .environmentBuilder()
                .build();
    }

    @Bean
    Connection streamConnection(Environment environment) {
        return environment.connectionBuilder()
                .name("consumer-" + streamName0)
                .build();
    }


    @Bean
    Consumer consumer0(Connection connection,
                       Converter<byte[], SpringIoEvent> messageConverter,
                       Subscriber<SpringIoEvent> subscriber) {
        return constructConsumer(streamName0, connection, messageConverter, subscriber);

    }

    @Bean
    Consumer consumer1(Connection connection,
                       Converter<byte[], SpringIoEvent> messageConverter,
                       Subscriber<SpringIoEvent> subscriber) {
        return constructConsumer(streamName1, connection, messageConverter, subscriber);

    }

    @Bean
    ChatClient chatClient(ChatModel chatModel) {

        return ChatClient
                .builder(chatModel)
                .defaultOptions(ChatOptions.builder()
                        .build())
                .build();
    }

    Consumer constructConsumer(String stream,
                               Connection connection,
                               Converter<byte[], SpringIoEvent> messageConverter,
                               Subscriber<SpringIoEvent> subscriber) {

        log.info("input consumed with SQL '{}' from input stream {}", sqlFilter, stream);

        return connection.consumerBuilder()
                .queue(stream)
                .stream()
                .offset(ConsumerBuilder.StreamOffsetSpecification.FIRST)
                .filter()
                .sql(sqlFilter)
                .stream()
                .builder().messageHandler((ctx, inputMessage) -> {
                    try {
                        //Processing input message
                        subscriber.accept(messageConverter.convert(inputMessage.body()));

                        //Acknowledge Message acceptance
                        ctx.accept();
                    } catch (Exception e) {
                        log.error("Error:{}", String.valueOf(e));
                        throw e;
                    }
                })
                .build();
    }


    @Bean
    Subscriber<SpringIoEvent> aiEventsConsumer(List<SpringIoEvent> events,
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
                System.exit(0);
            }
        };
    }
}
