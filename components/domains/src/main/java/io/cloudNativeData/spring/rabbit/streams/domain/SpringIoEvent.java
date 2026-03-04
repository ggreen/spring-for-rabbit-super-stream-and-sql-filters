package io.cloudNativeData.spring.rabbit.streams.domain;

import lombok.Builder;

@Builder
public record SpringIoEvent(String event, String session) {
}
