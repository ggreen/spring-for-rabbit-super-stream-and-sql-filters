package io.cloudNativeData.spring.rabbit.streams;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringRabbitStreamsApp {

	public static void main(String[] args) {
		SpringApplication.run(SpringRabbitStreamsApp.class, args);
	}


}
