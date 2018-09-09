package ca.mingz.dev.demo.spring.kafka.demo;

import ca.mingz.dev.demo.spring.kafka.demo.model.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication
public class ApplicationProducer implements CommandLineRunner {
    public static Logger logger = LoggerFactory.getLogger(ApplicationProducer.class);

	public static void main(String[] args) {
		SpringApplication.run(ApplicationProducer.class, args);
	}

    @Autowired
    private KafkaTemplate<String, Person> template;

	@Value("${kafka.topic}")
	private String topic;

    private final CountDownLatch latch = new CountDownLatch(3);

    @Override
    public void run(String... args) throws Exception {
        Person person;
        Message<Person> message;
        for(int n=0; n< 2; n++){
            person = Person.newBuilder().setFullName("James_Bond_" + n).build();
            message = MessageBuilder
                    .withPayload(person)
                    .setHeader(KafkaHeaders.TOPIC, topic)
                    .setHeader("Role", "007_" + n)
                    .setHeader("Time", new Date().toString())
                    .build();
            this.template.send(message);
        }
        template.flush();
    }
}
