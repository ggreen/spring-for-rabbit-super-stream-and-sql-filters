package io.cloudNativeData.spring.rabbit.streams;

import com.rabbitmq.client.amqp.*;
import com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder;
import io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent;
import lombok.extern.slf4j.Slf4j;
import nyla.solutions.core.util.Debugger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.messaging.converter.MessageConverter;


@Configuration
@Slf4j
public class FilteredConsumerConfig {

    private final static String sqlFilter = """
            session = 'rabbit' AND year = 2026
            """;

    @Value("${stream.name:events-1}")
    private String streamName;


    @Bean
    Environment amqpEnvironment()
    {
        return new AmqpEnvironmentBuilder()
                .connectionSettings()
                .environmentBuilder()
                .build();
    }

    @Bean("alertConnection")
    Connection alertConnection(Environment environment)
    {
        return environment.connectionBuilder()
                .build();
    }


    @Bean
    Consumer alertRabbitConsumer(Connection connection,
                                 java.util.function.Consumer<SpringIoEvent> alertConsumer,
                                 Converter<byte[],SpringIoEvent> converter){

        log.info("input consumed with SQL '{}' from input stream {}",sqlFilter,streamName);

        var builder = connection.consumerBuilder()
                .queue(streamName)
                .stream()
                .offset(ConsumerBuilder.StreamOffsetSpecification.FIRST);

        return builder
                .filter()
                .sql(sqlFilter)
                .stream()
                .builder().messageHandler((ctx,inputMessage) -> {

                    try {
                        //Processing input message
                        alertConsumer.accept(converter.convert(inputMessage.body()));
                    }
                    catch (Exception e)
                    {
                        log.error(Debugger.stackTrace(e));
                        throw e;
                    }

                })
                .build();
    }


    @Bean
    java.util.function.Consumer<SpringIoEvent> logFilteredConsumer()
    {
        return event -> { log.info("Received SpringIoEvent {}", event); };
    }
}
