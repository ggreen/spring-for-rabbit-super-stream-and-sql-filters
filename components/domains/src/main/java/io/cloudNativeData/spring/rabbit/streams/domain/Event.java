package io.cloudNativeData.spring.rabbit.streams.domain;

import lombok.Builder;

@Builder
public record Event(String id, long timestamp, String message, short priority) {
}
