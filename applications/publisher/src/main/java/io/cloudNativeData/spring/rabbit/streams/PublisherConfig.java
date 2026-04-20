package io.cloudNativeData.spring.rabbit.streams;

import io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent;
import lombok.extern.slf4j.Slf4j;
import nyla.solutions.core.io.csv.CsvReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

@Configuration
@Slf4j
public class PublisherConfig {

 @org.springframework.beans.factory.annotation.Value("classpath:csv/additional/events.csv")
// @Value("classpath:csv/spring-io-session-events.csv")
 private Resource resource;

 @Bean
 Iterator<List<String>> csvLines() throws IOException {
     return new CsvReader(resource.getFile()).stream().iterator();
 }


 @Bean
 Supplier<Message<SpringIoEvent>> eventPublisher(Iterator<List<String>> csvLines) {

     return () -> {
         if (csvLines.hasNext()) {
             var line = csvLines.next();
             var event = SpringIoEvent.builder()
                     .event(line.get(0))
                     .session(line.get(1))
                     .year(Integer.parseInt(line.get(2)))
                     .build();

             log.info("sending: {}",event);

             return MessageBuilder.withPayload(event)
                     .setHeader("session", event.session())
                     .setHeader("year", event.year())
                     .build();
         }
         return null;
     };
 }
}
