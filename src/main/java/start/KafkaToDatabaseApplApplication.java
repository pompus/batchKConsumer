package start;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(basePackages = { "com.kafka.configuration" ,"com.config","com.consumer" })
public class KafkaToDatabaseApplApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaToDatabaseApplApplication.class, args);
	}
}
