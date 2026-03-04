package io.cloudNativeData.spring.rabbit.streams;

import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import tools.jackson.databind.json.JsonMapper;

@Configuration
@Slf4j
public class SerializationConfig {

    @Bean
    MessageConverter messageConverter(JsonMapper jsonMapper) {
        return new MessageConverter() {
            @Override
            public @Nullable Object fromMessage(Message<?> message, Class<?> targetClass) {

                var payload = message.getPayload();
                if(payload instanceof byte[] bytes) {
                    return jsonMapper.readValue(bytes, targetClass);
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
