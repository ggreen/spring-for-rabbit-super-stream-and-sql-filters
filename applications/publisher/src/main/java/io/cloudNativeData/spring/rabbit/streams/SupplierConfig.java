package io.cloudNativeData.spring.rabbit.streams;

import io.cloudNativeData.spring.rabbit.streams.domain.SpringIoEvent;
import lombok.extern.slf4j.Slf4j;
import nyla.solutions.core.io.csv.CsvReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

@Configuration
@Slf4j
public class SupplierConfig {


    @Value("classpath:csv/spring-io-events.csv")
    private Resource resource;

    @Bean
    Iterator<List<String>> csvLines() throws IOException {
        return new CsvReader(resource.getFile()).stream().iterator();
    }
    @Bean
    Supplier<SpringIoEvent> eventPublisher(Iterator<List<String>> csvLines) {

        return () -> {
            if(csvLines.hasNext()) {
                var event = csvLines.next().getFirst();
                log.info("Events {}",event);
                return new SpringIoEvent(event);
            }

            return null;
        };
    }
}
