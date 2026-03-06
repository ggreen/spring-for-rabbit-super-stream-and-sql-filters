package io.cloudNativeData.spring.rabbit.streams.domain.ai;

import lombok.Builder;

@Builder
public record SpringIOEventSurvey(String opinion,EventRating rating) {
}