package ca.mingz.dev.demo.spring.kafka.demo;

import ca.mingz.dev.demo.spring.kafka.demo.model.Person;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class Application implements CommandLineRunner {
    public static Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

    @Autowired
    private KafkaTemplate<String, Person> template;

	@Value("${kafka.topic}")
	private String topic;

    private final CountDownLatch latch = new CountDownLatch(3);

    @Override
    public void run(String... args) throws Exception {
        Person person0 = Person.newBuilder().setFullName("Anakin Skywalker").build();
        // See example https://memorynotfound.com/spring-kafka-adding-custom-header-kafka-message-example/
        Message<Person> message0 = MessageBuilder
                .withPayload(person0)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader("Role", "Father")
                .build();

        Person person1 = Person.newBuilder().setFullName("Leia Skywalker").build();
        Message<Person> message1 = MessageBuilder
                .withPayload(person1)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader("Role", "Daughter")
                .build();

        Person person2 = Person.newBuilder().setFullName("Luke Skywalker").build();
        Message<Person> message2 = MessageBuilder
                .withPayload(person2)
                .setHeader(KafkaHeaders.TOPIC, topic)
                .setHeader("Role", "Son")
                .build();

//        this.template.send(topic, person0);
//        this.template.send(topic, person1);
//        this.template.send(topic, person2);

        this.template.send(message0);
        this.template.send(message1);
        this.template.send(message2);

        latch.await(60, TimeUnit.SECONDS);
        logger.info("All received");
    }

    @KafkaListener(topics = "${kafka.topic}")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        logger.info(cr.toString());
        latch.countDown();
    }
}
