package io.cloudNativeData.spring.rabbit.streams;

import io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import tools.jackson.databind.json.JsonMapper;

@Configuration
@Slf4j
public class SerializationConfig {

    @Bean
    Converter<byte[], SpringIoEvent> converter(JsonMapper jsonMapper) {
        return bytes -> jsonMapper.readValue(bytes, SpringIoEvent.class);
    }
    @Bean
    MessageConverter messageConverter(Converter<byte[], SpringIoEvent> converter) {
        return new MessageConverter() {
            @Override
            public @Nullable Object fromMessage(Message<?> message, Class<?> targetClass) {

                var payload = message.getPayload();
                if(payload instanceof byte[] bytes) {
                    return converter.convert(bytes);
                }
                return payload;
            }

            @Override
            public @Nullable Message<?> toMessage(Object payload, @Nullable MessageHeaders headers) {
                return MessageBuilder.withPayload(payload).build();
            }
        };
    }
}
