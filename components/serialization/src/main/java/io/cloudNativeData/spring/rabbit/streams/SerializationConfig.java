package io.cloudNativeData.spring.rabbit.streams;

import io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent;
import jakarta.annotation.Nullable;
import org.jspecify.annotations.NonNull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.http.MediaType;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.support.MessageBuilder;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class SerializationConfig {

    @Bean
    Converter<byte[], SpringIoEvent> converter(JsonMapper jsonMapper) {
        return bytes -> jsonMapper.readValue(bytes, SpringIoEvent.class);
    }

    @Bean
    MessageConverter messageConverter(Converter<byte[], SpringIoEvent> converter) {
        return new MessageConverter() {
            @Override
            public @Nullable Object fromMessage(@NonNull Message<?> message, @NonNull Class<?> targetClass) {

                var payload = message.getPayload();
                if (payload instanceof byte[] bytes) {
                    return converter.convert(bytes);
                }
                return payload;
            }

            @Override
            public @NonNull Message<?> toMessage(@NonNull Object payload, @Nullable MessageHeaders headers) {

                if (payload instanceof byte[] bytes) {
                    payload = converter.convert(bytes);
                }
                var builder = MessageBuilder.withPayload(payload);

                if (headers != null) {}
                        builder.setHeader(MessageHeaders.CONTENT_TYPE,
                                MediaType.APPLICATION_JSON.toString()).build();

                return builder.build();
            }
        };
    }
}
