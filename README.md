# spring-for-rabbit-super-stream-and-esql-filters

Start RabbitMQ

```shell
deployment/local/containers/rabbit.sh
```



Stream Tracking

```shell
rabbitmq-streams list_stream_tracking events.spring.io -n rabbit
```


Start Consumer 

```java
@org.springframework.context.annotation.Bean
java.util.function.Consumer<io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> logConsumer()
{
    return event -> { log.info("Received: {}", event); };
}
```

Consumer Properties

```properties
spring.application.name=spring-consumer
server.port=0

# Definite Spring Cloud Stream Consumer bean name
spring.cloud.function.definition=logConsumer

#Define an alias for the consumer bean
spring.cloud.stream.function.bindings.logConsumer-in-0=input

#Group the group name
spring.cloud.stream.bindings.input.group=spring.io

#Group the destination
spring.cloud.stream.bindings.input.destination=events

# Connection names
spring.cloud.stream.rabbit.binder.connection-name-prefix==${spring.application.name}

#Default a RabbitMQ stream
spring.cloud.stream.rabbit.bindings.input.consumer.containerType=STREAM
```

Adding Publisher

```java
@org.springframework.context.annotation.Bean
org.springframework.rabbit.stream.producer.RabbitStreamTemplate rabbitStreamTemplate(com.rabbitmq.stream.Environment environment,
        tools.jackson.databind.json.JsonMapper jsonMapper)
{
    var template = new org.springframework.rabbit.stream.producer.RabbitStreamTemplate(environment,"events.spring.io");
    template.setMessageConverter(new org.springframework.amqp.support.converter.JacksonJsonMessageConverter(jsonMapper));

    return template;
}
```


Replay by with set Offset to First

```java
@org.springframework.context.annotation.Bean
org.springframework.cloud.stream.config.ListenerContainerCustomizer<org.springframework.amqp.rabbit.listener.MessageListenerContainer> customizer() {
    return (msgListenerContainer, dest, group) -> {
        if (msgListenerContainer instanceof org.springframework.rabbit.stream.listener.StreamListenerContainer streamContainer) {
            streamContainer.setConsumerCustomizer((name, builder) -> {
                builder.subscriptionListener(
                        subscriptionContext -> subscriptionContext
                                .offsetSpecification(OffsetSpecification.first()));
            });
        }
    };
}
```


Publisher Performance Test

```java
/**
 * Loop 3 Million times
 */
private static final Long loopCount = 3000000L;

@org.springframework.beans.factory.annotation.Value("${bathSize:10000}")
private int batchSize;

@org.springframework.beans.factory.annotation.Value("${subBatchSize:5000}")
private int subBatchSize;

@org.springframework.beans.factory.annotation.Value("${perfTestStreamName:perfTest}")
private String perfTestStreamName;

@org.springframework.beans.factory.annotation.Value("${eventMessage:That's alot}")
private String eventMessage;


@org.springframework.context.annotation.Bean
org.springframework.rabbit.stream.producer.RabbitStreamTemplate template(com.rabbitmq.stream.Environment environment) {

    //Create Stream
    environment.streamCreator().stream(perfTestStreamName).create();

    var template = new org.springframework.rabbit.stream.producer.RabbitStreamTemplate(environment, perfTestStreamName);
    template.setProducerCustomizer((stream, builder) -> {
        builder.batchSize(batchSize);
        builder.subEntrySize(subBatchSize);
    });

    return template;
}


@org.springframework.context.annotation.Bean
com.rabbitmq.stream.Message msg(org.springframework.rabbit.stream.producer.RabbitStreamTemplate template, tools.jackson.databind.json.JsonMapper mapper) {
    var event = io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent.builder().event(eventMessage).build();
    var msgPayload = mapper.writeValueAsString(event);
    log.info("Sending event: {}", msgPayload);

    return template.messageBuilder().addData(msgPayload.getBytes(java.nio.charset.StandardCharsets.UTF_8))
            .build();
}

@org.springframework.context.annotation.Bean
org.springframework.boot.ApplicationRunner applicationRunner(org.springframework.rabbit.stream.producer.RabbitStreamTemplate template, com.rabbitmq.stream.Message streamMsg) {
    return args -> {

        var start = System.currentTimeMillis();

        for (int i = 0; i < loopCount; i++) {

            template.send(streamMsg);
        }

        var end = System.currentTimeMillis();
        var thruPut = (loopCount / (end - start)) * 1000;

        log.info("{} per second", nyla.solutions.core.util.Text.format().formatNumber(thruPut));
    };
}
```

## Super Streams

Use the following properties

```properties
spring.application.name=spring-consumer
server.port=0

spring.cloud.function.definition=logConsumer
spring.cloud.stream.function.bindings.logConsumer-in-0=input

spring.cloud.stream.bindings.input.group=spring.io
spring.cloud.stream.bindings.input.destination=events.super.streams

spring.cloud.stream.rabbit.binder.connection-name-prefix==${spring.application.name}
spring.cloud.stream.rabbit.bindings.input.consumer.containerType=STREAM

# Super Streams
spring.cloud.stream.bindings.input.consumer.instance-count=2
spring.cloud.stream.bindings.input.consumer.concurrency=1
spring.cloud.stream.rabbit.bindings.input.consumer.container-type=STREAM
spring.cloud.stream.rabbit.bindings.input.consumer.super-stream=true
spring.cloud.stream.bindings.input.content-type=application/json
```

Publisher for Super Stream

```java
@org.springframework.beans.factory.annotation.Value("classpath:csv/spring-io-session-events.csv")
private org.springframework.core.io.Resource resource;

@org.springframework.context.annotation.Bean
java.util.Iterator<java.util.List<String>> csvLines() throws java.io.IOException {
    return new nyla.solutions.core.io.csv.CsvReader(resource.getFile()).stream().iterator();
}


@org.springframework.context.annotation.Bean
java.util.function.Supplier<io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> eventPublisher(java.util.Iterator<java.util.List<String>> csvLines) {

    return () -> {
        if(csvLines.hasNext()) {
            var line = csvLines.next();
            log.info("Events {}",line);
            return  io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent.builder()
                    .event(line.getFirst())
                    .session(line.get(1))
                    .year(Integer.parseInt(line.get(2)))
                    .build();
        }
        return null;
    };
}
```


Publisher Super Stream Properties

```properties
spring.application.name=spring-publisher
server.port=0

spring.cloud.function.definition=eventPublisher

spring.cloud.stream.bindings.output.group=spring.io
spring.cloud.stream.bindings.output.destination=events.super.streams
spring.cloud.stream.rabbit.binder.connection-name-prefix==${spring.application.name}
spring.cloud.stream.rabbit.bindings.output.consumer.containerType=STREAM
#spring.cloud.stream.rabbit.bindings.output.consumer.autoBindDlq=true

bathSize=10000
subBatchSize=5000
spring.cloud.stream.function.bindings.eventPublisher-out-0=output
spring.cloud.stream.bindings.output.content-type=application/json


# Super Streams
spring.cloud.stream.bindings.output.producer.partition-count=2
spring.cloud.stream.bindings.output.producer.partition-key-expression=payload['session']
spring.cloud.stream.rabbit.bindings.output.producer.producer-type=stream-async
spring.cloud.stream.rabbit.bindings.output.producer.super-stream=true
spring.cloud.stream.rabbit.bindings.output.producer.declare-exchange=false
```


## SQL Filtering

Consuming Properties

```properties
spring.application.name=spring-consumer
server.port=0

spring.cloud.function.definition=logConsumer
spring.cloud.stream.function.bindings.logConsumer-in-0=input

spring.cloud.stream.bindings.input.group=spring.io
spring.cloud.stream.bindings.input.destination=events.super.streams.filtering


# Super Streams
spring.cloud.stream.rabbit.bindings.input.consumer.container-type=STREAM
spring.cloud.stream.rabbit.bindings.input.consumer.super-stream=true
spring.cloud.stream.bindings.input.consumer.instance-count=2
spring.cloud.stream.bindings.input.consumer.concurrency=1
spring.cloud.stream.bindings.input.content-type=application/json
spring.cloud.stream.rabbit.binder.connection-name-prefix==${spring.application.name}
```

Publisher for SQL Filters

```java
@Bean
java.util.function.Supplier<org.springframework.messaging.Message<io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent>> eventPublisher(java.util.Iterator<java.util.List<String>> csvLines) {

    return () -> {
        if (csvLines.hasNext()) {
            var line = csvLines.next();
            var event = io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent.builder()
                    .event(line.get(0))
                    .session(line.get(1))
                    .year(Integer.parseInt(line.get(2)))
                    .build();

            log.info("sending: {}",event);

            return org.springframework.messaging.support.MessageBuilder.withPayload(event)
                    .setHeader("session", event.session())
                    .setHeader("year", event.year())
                    .build();
        }
        return null;
    };
}
```


Consumer with SQL Filter

```java
//Zen's SQL = session = 'rabbit' AND year = 2026
// Arul's SQL = session IN ('postgres','dataflow')
// Vlad's SQL = session IN ('gemfire','valkey')
private final static String sqlFilter = """
         session = 'rabbit' AND year = 2026
        """;

@org.springframework.beans.factory.annotation.Value("${stream.name:events.super.streams.filtering-0}")
private String streamName0;
@org.springframework.beans.factory.annotation.Value("${stream.name:events.super.streams.filtering-1}")
private String streamName1;


@org.springframework.context.annotation.Bean
com.rabbitmq.client.amqp.Environment amqpEnvironment() {
    return new com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder()
            .connectionSettings()
            .environmentBuilder()
            .build();
}

@org.springframework.context.annotation.Bean
com.rabbitmq.client.amqp.Connection streamConnection(com.rabbitmq.client.amqp.Environment environment) {
    return environment.connectionBuilder()
            .name("consumer-"+streamName1)
            .build();
}



@org.springframework.context.annotation.Bean com.rabbitmq.client.amqp.Consumer consumer0(com.rabbitmq.client.amqp.Connection connection,
                                                                                         org.springframework.core.convert.converter.Converter<byte[], io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> messageConverter)
{
    return constructConsumer(streamName0,connection,messageConverter);

}

@org.springframework.context.annotation.Bean com.rabbitmq.client.amqp.Consumer consumer1(com.rabbitmq.client.amqp.Connection connection,
                                                                                         org.springframework.core.convert.converter.Converter<byte[], io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> messageConverter)
{
    return constructConsumer(streamName1,connection,messageConverter);

}


com.rabbitmq.client.amqp.Consumer constructConsumer(String stream,
                                                    com.rabbitmq.client.amqp.Connection connection,
                                                    org.springframework.core.convert.converter.Converter<byte[], io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> messageConverter) {

    log.info("input consumed with SQL '{}' from input stream {}", sqlFilter, stream);

    return connection.consumerBuilder()
            .queue(stream)
            .stream()
            .offset(com.rabbitmq.client.amqp.ConsumerBuilder.StreamOffsetSpecification.FIRST)
            .filter()
            .sql(sqlFilter)
            .stream()
            .builder().messageHandler((ctx, inputMessage) -> {
                try {
                    //Processing input message
                    var event = messageConverter.convert(inputMessage.body());
                    log.info("Received: {}", event);
                } catch (Exception e) {
                    log.error("Error:{}", String.valueOf(e));
                    throw e;
                }
            })
            .build();
}
```


AI Intelligence

```java

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

@org.springframework.beans.factory.annotation.Value("${stream.name:events.super.streams.filtering-0}")
private String streamName0;
@org.springframework.beans.factory.annotation.Value("${stream.name:events.super.streams.filtering-1}")
private String streamName1;


@org.springframework.context.annotation.Bean
com.rabbitmq.client.amqp.Environment amqpEnvironment() {
    return new com.rabbitmq.client.amqp.impl.AmqpEnvironmentBuilder()
            .connectionSettings()
            .environmentBuilder()
            .build();
}

@org.springframework.context.annotation.Bean
com.rabbitmq.client.amqp.Connection streamConnection(com.rabbitmq.client.amqp.Environment environment) {
    return environment.connectionBuilder()
            .name("consumer-"+streamName0)
            .build();
}



@org.springframework.context.annotation.Bean com.rabbitmq.client.amqp.Consumer consumer0(com.rabbitmq.client.amqp.Connection connection,
                                                                                         org.springframework.core.convert.converter.Converter<byte[], io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> messageConverter,
                                                                                         nyla.solutions.core.patterns.integration.Subscriber<io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> subscriber)
{
    return constructConsumer(streamName0,connection,messageConverter,subscriber);

}

@org.springframework.context.annotation.Bean com.rabbitmq.client.amqp.Consumer consumer1(com.rabbitmq.client.amqp.Connection connection,
                                                                                         org.springframework.core.convert.converter.Converter<byte[], io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> messageConverter,
                                                                                         nyla.solutions.core.patterns.integration.Subscriber<io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> subscriber)
{
    return constructConsumer(streamName1,connection,messageConverter,subscriber);

}

@org.springframework.context.annotation.Bean
org.springframework.ai.chat.client.ChatClient chatClient(org.springframework.ai.chat.model.ChatModel chatModel) {

    return org.springframework.ai.chat.client.ChatClient
            .builder(chatModel)
            .defaultOptions(org.springframework.ai.chat.prompt.ChatOptions.builder()
                    .build())
            .build();
}

com.rabbitmq.client.amqp.Consumer constructConsumer(String stream,
                                                    com.rabbitmq.client.amqp.Connection connection,
                                                    org.springframework.core.convert.converter.Converter<byte[], io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> messageConverter,
                                                    nyla.solutions.core.patterns.integration.Subscriber<io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> subscriber) {

    log.info("input consumed with SQL '{}' from input stream {}", sqlFilter, stream);

    return connection.consumerBuilder()
            .queue(stream)
            .stream()
            .offset(com.rabbitmq.client.amqp.ConsumerBuilder.StreamOffsetSpecification.FIRST)
            .filter()
            .sql(sqlFilter)
            .stream()
            .builder().messageHandler((ctx, inputMessage) -> {
                try {
                    //Processing input message
                    subscriber.accept(messageConverter.convert(inputMessage.body()));
                } catch (Exception e) {
                    log.error("Error:{}", String.valueOf(e));
                    throw e;
                }
            })
            .build();
}


@org.springframework.context.annotation.Bean
nyla.solutions.core.patterns.integration.Subscriber<io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> aiEventsConsumer(java.util.List<io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent> events,
                                                                                                                                    org.springframework.ai.chat.client.ChatClient chatClient) {
    return event -> {
        log.info("Received SpringIoEvent {}", event);
        events.add(event);
        if (events.size() >= 8) {

            log.info("********* HERE IS THE VERDICT ***********\n Wait for IT!");

            var verdict = chatClient.prompt()
                    .user(u -> u.text(prompt)
                            .param("events", events))
                    .call()
                    .entity(io.cloudNativeData.spring.rabbit.streams.domain.ai.SpringIOEventSurvey.class);

            log.info("*********\nSurvey verdict: {}\n***************", verdict);
            System.exit(0);
        }
    };
}
```

-----------------------------

# Cleanup

```shell
rabbitmqadmin delete queue name=events.spring.io
rabbitmqadmin delete queue name=events.super.streams-0
rabbitmqadmin delete queue name=events.super.streams-1
rabbitmqadmin delete queue name=perfTest
rabbitmqadmin delete queue name=events.super.streams.filtering-0
rabbitmqadmin delete queue name=events.super.streams.filtering-1
rabbitmqadmin delete exchange name=aiEventsConsumer-in-0
rabbitmqadmin delete exchange name=events
rabbitmqadmin delete exchange name=events.super.streams
rabbitmqadmin delete exchange name=logFilteredConsumer-in-0
rabbitmqadmin delete exchange name=logFilteredEvent-in-0
rabbitmqadmin delete exchange name=events.super.streams.filtering
```

