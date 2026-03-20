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


Errrro


```java
2026-03-20T14:58:03.263-04:00 ERROR 21937 --- [spring-consumer] [am-consumer-0-0] o.s.integration.handler.LoggingHandler   : org.springframework.messaging.MessagingException: Retry policy for operation 'org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter$Listener$$Lambda/0x00000070019f19d0' exhausted; aborting execution
	at org.springframework.integration.core.ErrorMessagePublisher.determinePayload(ErrorMessagePublisher.java:186)
	at org.springframework.integration.core.ErrorMessagePublisher.publish(ErrorMessagePublisher.java:162)
	at org.springframework.integration.handler.advice.ErrorMessageSendingRecoverer.recover(ErrorMessageSendingRecoverer.java:79)
	at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter$Listener.onMessage(AmqpInboundChannelAdapter.java:398)
	at org.springframework.rabbit.stream.listener.StreamListenerContainer.lambda$setupMessageListener$3(StreamListenerContainer.java:371)
	at org.springframework.rabbit.stream.listener.StreamListenerContainer.lambda$setupMessageListener$0(StreamListenerContainer.java:394)
	at com.rabbitmq.stream.impl.StreamConsumer.lambda$new$0(StreamConsumer.java:115)
	at com.rabbitmq.stream.impl.StreamConsumer.lambda$new$2(StreamConsumer.java:140)
	at com.rabbitmq.stream.impl.StreamConsumer.lambda$new$7(StreamConsumer.java:223)
	at com.rabbitmq.stream.impl.ConsumersCoordinator$ClientSubscriptionsManager.lambda$new$2(ConsumersCoordinator.java:649)
	at com.rabbitmq.stream.impl.ServerFrameHandler$DeliverVersion1FrameHandler.handleMessage(ServerFrameHandler.java:337)
	at com.rabbitmq.stream.impl.ServerFrameHandler$DeliverVersion1FrameHandler.handleDeliver(ServerFrameHandler.java:471)
	at com.rabbitmq.stream.impl.ServerFrameHandler$DeliverVersion2FrameHandler.doHandle(ServerFrameHandler.java:598)
	at com.rabbitmq.stream.impl.ServerFrameHandler$BaseFrameHandler.handle(ServerFrameHandler.java:242)
	at com.rabbitmq.stream.impl.Client$FrameHandlerTask.run(Client.java:2958)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.FutureTask.run$$$capture(FutureTask.java:317)
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java)
	at --- Async.Stack.Trace --- (captured by IntelliJ IDEA debugger)
	at java.base/java.util.concurrent.FutureTask.<init>(FutureTask.java:151)
	at java.base/java.util.concurrent.AbstractExecutorService.newTaskFor(AbstractExecutorService.java:98)
	at java.base/java.util.concurrent.AbstractExecutorService.submit(AbstractExecutorService.java:122)
	at java.base/java.util.concurrent.Executors$DelegatedExecutorService.submit(Executors.java:785)
	at com.rabbitmq.stream.impl.Client$StreamHandler.channelRead(Client.java:2851)
	at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:357)
	at io.netty.handler.codec.ByteToMessageDecoder.fireChannelRead(ByteToMessageDecoder.java:361)
	at io.netty.handler.codec.ByteToMessageDecoder.fireChannelRead(ByteToMessageDecoder.java:348)
	at io.netty.handler.codec.ByteToMessageDecoder.callDecode(ByteToMessageDecoder.java:470)
	at io.netty.handler.codec.ByteToMessageDecoder.channelRead(ByteToMessageDecoder.java:296)
	at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:357)
	at io.netty.handler.timeout.IdleStateHandler.channelRead(IdleStateHandler.java:288)
	at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:355)
	at io.netty.handler.flush.FlushConsolidationHandler.channelRead(FlushConsolidationHandler.java:152)
	at io.netty.channel.AbstractChannelHandlerContext.fireChannelRead(AbstractChannelHandlerContext.java:355)
	at io.netty.channel.DefaultChannelPipeline$HeadContext.channelRead(DefaultChannelPipeline.java:1429)
	at io.netty.channel.DefaultChannelPipeline.fireChannelRead(DefaultChannelPipeline.java:918)
	at io.netty.channel.nio.AbstractNioByteChannel$NioByteUnsafe.read(AbstractNioByteChannel.java:176)
	at io.netty.channel.nio.AbstractNioChannel$AbstractNioUnsafe.handle(AbstractNioChannel.java:445)
	at io.netty.channel.nio.NioIoHandler$DefaultNioRegistration.handle(NioIoHandler.java:388)
	at io.netty.channel.nio.NioIoHandler.processSelectedKey(NioIoHandler.java:596)
	at io.netty.channel.nio.NioIoHandler.processSelectedKeysOptimized(NioIoHandler.java:571)
	at io.netty.channel.nio.NioIoHandler.processSelectedKeys(NioIoHandler.java:512)
	at io.netty.channel.nio.NioIoHandler.run(NioIoHandler.java:484)
	at io.netty.channel.SingleThreadIoEventLoop.runIo(SingleThreadIoEventLoop.java:225)
	at io.netty.channel.SingleThreadIoEventLoop.run(SingleThreadIoEventLoop.java:196)
	at io.netty.util.concurrent.SingleThreadEventExecutor$5.run(SingleThreadEventExecutor.java:1195)
	at io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)
	at io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
	at java.base/java.lang.Thread.run(Thread.java:1583)
Caused by: org.springframework.core.retry.RetryException: Retry policy for operation 'org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter$Listener$$Lambda/0x00000070019f19d0' exhausted; aborting execution
	at org.springframework.core.retry.RetryTemplate.execute(RetryTemplate.java:197)
	at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter$Listener.onMessage(AmqpInboundChannelAdapter.java:385)
	... 45 more
	Suppressed: org.springframework.messaging.MessageDeliveryException: Dispatcher has no subscribers for channel 'spring-consumer.input'., failedMessage=GenericMessage [payload=byte[65], headers={year=2026, rabbitmq_streamContext=com.rabbitmq.stream.impl.ConsumersCoordinator$MessageHandlerContext@4895fbe3, session=rabbit, deliveryAttempt=4, amqp_messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, amqp_retryCount=0, id=df252a20-7349-3058-a145-faa5974adfb1, sourceData=(Body:'[B@7722f4d7(byte[65])' MessageProperties [headers={year=2026, session=rabbit}, messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, contentType=application/octet-stream, contentLength=0, deliveryMode=PERSISTENT, priority=0, deliveryTag=0]), contentType=application/octet-stream, timestamp=1774033076232}]
		at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:76)
		at org.springframework.integration.channel.AbstractMessageChannel.sendInternal(AbstractMessageChannel.java:438)
		at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:341)
		at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:310)
		at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:187)
		at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:166)
		at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:47)
		at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:108)
		at org.springframework.integration.endpoint.MessageProducerSupport.sendMessageWithTracking(MessageProducerSupport.java:278)
		at org.springframework.integration.endpoint.MessageProducerSupport.sendMessage(MessageProducerSupport.java:267)
		at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter.access$000(AmqpInboundChannelAdapter.java:71)
		at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter$Listener.lambda$onMessage$0(AmqpInboundChannelAdapter.java:392)
		at org.springframework.core.retry.RetryTemplate.execute(RetryTemplate.java:135)
		... 46 more
	Caused by: org.springframework.integration.MessageDispatchingException: Dispatcher has no subscribers, failedMessage=GenericMessage [payload=byte[65], headers={year=2026, rabbitmq_streamContext=com.rabbitmq.stream.impl.ConsumersCoordinator$MessageHandlerContext@4895fbe3, session=rabbit, deliveryAttempt=4, amqp_messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, amqp_retryCount=0, id=df252a20-7349-3058-a145-faa5974adfb1, sourceData=(Body:'[B@7722f4d7(byte[65])' MessageProperties [headers={year=2026, session=rabbit}, messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, contentType=application/octet-stream, contentLength=0, deliveryMode=PERSISTENT, priority=0, deliveryTag=0]), contentType=application/octet-stream, timestamp=1774033076232}]
		at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:156)
		at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:123)
		at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:72)
		... 58 more
	Suppressed: org.springframework.messaging.MessageDeliveryException: Dispatcher has no subscribers for channel 'spring-consumer.input'., failedMessage=GenericMessage [payload=byte[65], headers={year=2026, rabbitmq_streamContext=com.rabbitmq.stream.impl.ConsumersCoordinator$MessageHandlerContext@4895fbe3, session=rabbit, deliveryAttempt=4, amqp_messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, amqp_retryCount=0, id=df252a20-7349-3058-a145-faa5974adfb1, sourceData=(Body:'[B@7722f4d7(byte[65])' MessageProperties [headers={year=2026, session=rabbit}, messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, contentType=application/octet-stream, contentLength=0, deliveryMode=PERSISTENT, priority=0, deliveryTag=0]), contentType=application/octet-stream, timestamp=1774033076232}]
		at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:76)
		at org.springframework.integration.channel.AbstractMessageChannel.sendInternal(AbstractMessageChannel.java:438)
		at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:341)
		at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:310)
		at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:187)
		at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:166)
		at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:47)
		at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:108)
		at org.springframework.integration.endpoint.MessageProducerSupport.sendMessageWithTracking(MessageProducerSupport.java:278)
		at org.springframework.integration.endpoint.MessageProducerSupport.sendMessage(MessageProducerSupport.java:267)
		at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter.access$000(AmqpInboundChannelAdapter.java:71)
		at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter$Listener.lambda$onMessage$0(AmqpInboundChannelAdapter.java:392)
		at org.springframework.core.retry.RetryTemplate.execute(RetryTemplate.java:174)
		... 46 more
	Caused by: org.springframework.integration.MessageDispatchingException: Dispatcher has no subscribers, failedMessage=GenericMessage [payload=byte[65], headers={year=2026, rabbitmq_streamContext=com.rabbitmq.stream.impl.ConsumersCoordinator$MessageHandlerContext@4895fbe3, session=rabbit, deliveryAttempt=4, amqp_messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, amqp_retryCount=0, id=df252a20-7349-3058-a145-faa5974adfb1, sourceData=(Body:'[B@7722f4d7(byte[65])' MessageProperties [headers={year=2026, session=rabbit}, messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, contentType=application/octet-stream, contentLength=0, deliveryMode=PERSISTENT, priority=0, deliveryTag=0]), contentType=application/octet-stream, timestamp=1774033076232}]
		at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:156)
		at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:123)
		at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:72)
		... 58 more
	Suppressed: org.springframework.messaging.MessageDeliveryException: Dispatcher has no subscribers for channel 'spring-consumer.input'., failedMessage=GenericMessage [payload=byte[65], headers={year=2026, rabbitmq_streamContext=com.rabbitmq.stream.impl.ConsumersCoordinator$MessageHandlerContext@4895fbe3, session=rabbit, deliveryAttempt=4, amqp_messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, amqp_retryCount=0, id=df252a20-7349-3058-a145-faa5974adfb1, sourceData=(Body:'[B@7722f4d7(byte[65])' MessageProperties [headers={year=2026, session=rabbit}, messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, contentType=application/octet-stream, contentLength=0, deliveryMode=PERSISTENT, priority=0, deliveryTag=0]), contentType=application/octet-stream, timestamp=1774033076232}]
		at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:76)
		at org.springframework.integration.channel.AbstractMessageChannel.sendInternal(AbstractMessageChannel.java:438)
		at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:341)
		at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:310)
		at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:187)
		at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:166)
		at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:47)
		at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:108)
		at org.springframework.integration.endpoint.MessageProducerSupport.sendMessageWithTracking(MessageProducerSupport.java:278)
		at org.springframework.integration.endpoint.MessageProducerSupport.sendMessage(MessageProducerSupport.java:267)
		at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter.access$000(AmqpInboundChannelAdapter.java:71)
		at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter$Listener.lambda$onMessage$0(AmqpInboundChannelAdapter.java:392)
		at org.springframework.core.retry.RetryTemplate.execute(RetryTemplate.java:174)
		... 46 more
	Caused by: org.springframework.integration.MessageDispatchingException: Dispatcher has no subscribers, failedMessage=GenericMessage [payload=byte[65], headers={year=2026, rabbitmq_streamContext=com.rabbitmq.stream.impl.ConsumersCoordinator$MessageHandlerContext@4895fbe3, session=rabbit, deliveryAttempt=4, amqp_messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, amqp_retryCount=0, id=df252a20-7349-3058-a145-faa5974adfb1, sourceData=(Body:'[B@7722f4d7(byte[65])' MessageProperties [headers={year=2026, session=rabbit}, messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, contentType=application/octet-stream, contentLength=0, deliveryMode=PERSISTENT, priority=0, deliveryTag=0]), contentType=application/octet-stream, timestamp=1774033076232}]
		at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:156)
		at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:123)
		at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:72)
		... 58 more
Caused by: org.springframework.messaging.MessageDeliveryException: Dispatcher has no subscribers for channel 'spring-consumer.input'., failedMessage=GenericMessage [payload=byte[65], headers={year=2026, rabbitmq_streamContext=com.rabbitmq.stream.impl.ConsumersCoordinator$MessageHandlerContext@4895fbe3, session=rabbit, deliveryAttempt=4, amqp_messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, amqp_retryCount=0, id=df252a20-7349-3058-a145-faa5974adfb1, sourceData=(Body:'[B@7722f4d7(byte[65])' MessageProperties [headers={year=2026, session=rabbit}, messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, contentType=application/octet-stream, contentLength=0, deliveryMode=PERSISTENT, priority=0, deliveryTag=0]), contentType=application/octet-stream, timestamp=1774033076232}]
	at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:76)
	at org.springframework.integration.channel.AbstractMessageChannel.sendInternal(AbstractMessageChannel.java:438)
	at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:341)
	at org.springframework.integration.channel.AbstractMessageChannel.send(AbstractMessageChannel.java:310)
	at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:187)
	at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:166)
	at org.springframework.messaging.core.GenericMessagingTemplate.doSend(GenericMessagingTemplate.java:47)
	at org.springframework.messaging.core.AbstractMessageSendingTemplate.send(AbstractMessageSendingTemplate.java:108)
	at org.springframework.integration.endpoint.MessageProducerSupport.sendMessageWithTracking(MessageProducerSupport.java:278)
	at org.springframework.integration.endpoint.MessageProducerSupport.sendMessage(MessageProducerSupport.java:267)
	at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter.access$000(AmqpInboundChannelAdapter.java:71)
	at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter$Listener.lambda$onMessage$0(AmqpInboundChannelAdapter.java:392)
	at org.springframework.core.retry.RetryTemplate.execute(RetryTemplate.java:174)
	... 46 more
Caused by: org.springframework.integration.MessageDispatchingException: Dispatcher has no subscribers, failedMessage=GenericMessage [payload=byte[65], headers={year=2026, rabbitmq_streamContext=com.rabbitmq.stream.impl.ConsumersCoordinator$MessageHandlerContext@4895fbe3, session=rabbit, deliveryAttempt=4, amqp_messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, amqp_retryCount=0, id=df252a20-7349-3058-a145-faa5974adfb1, sourceData=(Body:'[B@7722f4d7(byte[65])' MessageProperties [headers={year=2026, session=rabbit}, messageId=2a02c42b-f7e8-3795-5788-04ea12d471ea, contentType=application/octet-stream, contentLength=0, deliveryMode=PERSISTENT, priority=0, deliveryTag=0]), contentType=application/octet-stream, timestamp=1774033076232}]
	at org.springframework.integration.dispatcher.UnicastingDispatcher.doDispatch(UnicastingDispatcher.java:156)
	at org.springframework.integration.dispatcher.UnicastingDispatcher.dispatch(UnicastingDispatcher.java:123)
	at org.springframework.integration.channel.AbstractSubscribableChannel.doSend(AbstractSubscribableChannel.java:72)
	... 58 more

2026-03-20T14:58:03.263-04:00  INFO 21937 --- [spring-consumer] [am-consumer-0-0] o.s.a.r.c.CachingConnectionFactory       : Attempting to connect to: [localhost:5672]
2026-03-20T14:58:03.270-04:00  INFO 21937 --- [spring-consumer] [am-consumer-0-0] o.s.a.r.c.CachingConnectionFactory       : Created new connection: =spring-consumer#1.publisher/SimpleConnection@2040ab18 [delegate=amqp://guest@127.0.0.1:5672/, localPort=52460]
2026-03-20T14:58:07.463-04:00  WARN 21937 --- [spring-consumer] [ionShutdownHook] c.rabbitmq.stream.impl.StreamConsumer    : Error while checking offset has been stored

java.util.concurrent.TimeoutException
	at java.base/java.util.concurrent.CompletableFuture.timedGet(CompletableFuture.java:1960) ~[na:na]
	at java.base/java.util.concurrent.CompletableFuture.get(CompletableFuture.java:2095) ~[na:na]
	at com.rabbitmq.stream.impl.StreamConsumer.waitForOffsetToBeStored(StreamConsumer.java:371) ~[stream-client-0.23.0.jar:0.23.0]
	at com.rabbitmq.stream.impl.StreamConsumer.lambda$new$4(StreamConsumer.java:177) ~[stream-client-0.23.0.jar:0.23.0]
	at com.rabbitmq.stream.impl.StreamConsumer.consumerUpdate(StreamConsumer.java:431) ~[stream-client-0.23.0.jar:0.23.0]
	at com.rabbitmq.stream.impl.StreamConsumer.maybeNotifyActiveToInactiveSac(StreamConsumer.java:538) ~[stream-client-0.23.0.jar:0.23.0]
	at com.rabbitmq.stream.impl.StreamConsumer.closeFromEnvironment(StreamConsumer.java:498) ~[stream-client-0.23.0.jar:0.23.0]
	at com.rabbitmq.stream.impl.StreamConsumer.close(StreamConsumer.java:493) ~[stream-client-0.23.0.jar:0.23.0]
	at com.rabbitmq.stream.impl.SuperStreamConsumer.close(SuperStreamConsumer.java:208) ~[stream-client-0.23.0.jar:0.23.0]
	at org.springframework.rabbit.stream.listener.StreamListenerContainer.lambda$stop$0(StreamListenerContainer.java:303) ~[spring-rabbit-stream-4.0.2.jar:4.0.2]
	at java.base/java.util.ArrayList.forEach(ArrayList.java:1596) ~[na:na]
	at org.springframework.rabbit.stream.listener.StreamListenerContainer.stop(StreamListenerContainer.java:301) ~[spring-rabbit-stream-4.0.2.jar:4.0.2]
	at org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter.doStop(AmqpInboundChannelAdapter.java:309) ~[spring-integration-amqp-7.0.3.jar:7.0.3]
	at org.springframework.integration.endpoint.AbstractEndpoint.stop(AbstractEndpoint.java:175) ~[spring-integration-core-7.0.3.jar:7.0.3]
	at org.springframework.cloud.stream.binder.DefaultBinding.stop(DefaultBinding.java:154) ~[spring-cloud-stream-5.0.0.jar:5.0.0]
	at org.springframework.cloud.stream.binding.BindingService.unbindConsumers(BindingService.java:387) ~[spring-cloud-stream-5.0.0.jar:5.0.0]
	at org.springframework.cloud.stream.binding.AbstractBindableProxyFactory.unbindInputs(AbstractBindableProxyFactory.java:128) ~[spring-cloud-stream-5.0.0.jar:5.0.0]
	at org.springframework.cloud.stream.binding.InputBindingLifecycle.doStopWithBindable(InputBindingLifecycle.java:67) ~[spring-cloud-stream-5.0.0.jar:5.0.0]
	at java.base/java.util.LinkedHashMap$LinkedValues.forEach(LinkedHashMap.java:833) ~[na:na]
	at org.springframework.cloud.stream.binding.AbstractBindingLifecycle.stop(AbstractBindingLifecycle.java:71) ~[spring-cloud-stream-5.0.0.jar:5.0.0]
	at org.springframework.cloud.stream.binding.AbstractBindingLifecycle.stop(AbstractBindingLifecycle.java:88) ~[spring-cloud-stream-5.0.0.jar:5.0.0]
	at org.springframework.context.support.DefaultLifecycleProcessor.doStop(DefaultLifecycleProcessor.java:480) ~[spring-context-7.0.5.jar:7.0.5]
	at org.springframework.context.support.DefaultLifecycleProcessor$LifecycleGroup.stop(DefaultLifecycleProcessor.java:645) ~[spring-context-7.0.5.jar:7.0.5]
	at java.base/java.lang.Iterable.forEach(Iterable.java:75) ~[na:na]
	at org.springframework.context.support.DefaultLifecycleProcessor.stopBeans(DefaultLifecycleProcessor.java:448) ~[spring-context-7.0.5.jar:7.0.5]
	at org.springframework.context.support.DefaultLifecycleProcessor.onClose(DefaultLifecycleProcessor.java:337) ~[spring-context-7.0.5.jar:7.0.5]
	at org.springframework.context.support.AbstractApplicationContext.doClose(AbstractApplicationContext.java:1199) ~[spring-context-7.0.5.jar:7.0.5]
	at org.springframework.boot.web.server.reactive.context.ReactiveWebServerApplicationContext.doClose(ReactiveWebServerApplicationContext.java:158) ~[spring-boot-web-server-4.0.3.jar:4.0.3]
	at org.springframework.context.support.AbstractApplicationContext.close(AbstractApplicationContext.java:1153) ~[spring-context-7.0.5.jar:7.0.5]
	at org.springframework.boot.SpringApplicationShutdownHook.closeAndWait(SpringApplicationShutdownHook.java:147) ~[spring-boot-4.0.3.jar:4.0.3]
	at java.base/java.lang.Iterable.forEach(Iterable.java:75) ~[na:na]
	at org.springframework.boot.SpringApplicationShutdownHook.run(SpringApplicationShutdownHook.java:116) ~[spring-boot-4.0.3.jar:4.0.3]
	at java.base/java.lang.Thread.run(Thread.java:1583) ~[na:na]

```