package com.example.demo;

import com.example.Person;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class DemoApplication implements CommandLineRunner {

	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}

    @Autowired
    private KafkaTemplate<String, Person> template;

	@Value("${kafka.topic}")
	private String topic;

    private final CountDownLatch latch = new CountDownLatch(3);

    @Override
    public void run(String... args) throws Exception {
        Person person0 = Person.newBuilder().setFullName("Anakin Skywalker").build();
        Person person1 = Person.newBuilder().setFullName("Leia Skywalker").build();
        Person person2 = Person.newBuilder().setFullName("Luke Skywalker").build();
        this.template.send(topic, person0);
        this.template.send(topic, person1);
        this.template.send(topic, person2);
        latch.await(60, TimeUnit.SECONDS);
        System.out.println("All received");
    }

    @KafkaListener(topics = "${kafka.topic}")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        System.out.println(cr.toString());
        latch.countDown();
    }
}