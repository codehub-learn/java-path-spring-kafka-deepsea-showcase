package gr.codelearn.spring.kafka.produce.bootstrap;

import gr.codelearn.spring.kafka.base.BaseComponent;
import gr.codelearn.spring.kafka.domain.Donation;
import gr.codelearn.spring.kafka.domain.Person;
import gr.codelearn.spring.kafka.produce.producer.SampleProducer;
import gr.codelearn.spring.kafka.util.SampleDataGenerator;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Profile("!skip-runners")
@Component
@RequiredArgsConstructor
public class ProducerRunner extends BaseComponent implements CommandLineRunner {
	private final SampleProducer sampleProducer;

	@Value("${app.kafka.topics.generic}")
	private String genericTopic;
	@Value("${app.kafka.topics.person}")
	private String personTopic;

	@Override
	public void run(final String... args) throws Exception {
		logger.info("----- Going to send messages without keys.");
		Thread.sleep(1000);
		SampleDataGenerator.generatePersons(10).forEach(p -> sampleProducer.sendMessageWithoutKey(genericTopic, p));

		Thread.sleep(2000);
		logger.info("----- Going to send messages with keys asynchronously.");
		Thread.sleep(1000);
		SampleDataGenerator.generatePersons(200)
						   .forEach(p -> sampleProducer.sendMessageWithKeyAsync(genericTopic,
																				ThreadLocalRandom.current()
																								 .nextLong(1, 101),
																				p));
		Thread.sleep(2000);
		logger.info("----- Going to send messages with keys synchronously.");
		Thread.sleep(1000);
		for (Person p : SampleDataGenerator.generatePersons(40)) {
			sampleProducer.sendMessageWithKeySync(genericTopic, ThreadLocalRandom.current().nextLong(1, 101), p);
		}

		Thread.sleep(2000);
		logger.info("----- Going to send messages using producer record.");
		Thread.sleep(1000);
		for (Person p : SampleDataGenerator.generatePersons(10)) {
			sampleProducer.sendMessageWithKeyRecord(genericTopic, ThreadLocalRandom.current().nextLong(1, 101), p);
		}

		Thread.sleep(2000);
		logger.info("----- Going to send messages using transactions.");
		Thread.sleep(1000);
		sampleProducer.sendMessageWithKeyTransactionally(genericTopic, SampleDataGenerator.generatePersons(10));

		Thread.sleep(2000);
		logger.info("----- Going to send messages to fail filter keys.");
		Thread.sleep(1000);
		SampleDataGenerator.generateFilteredOutPersons(2)
						   .forEach(p -> sampleProducer.sendMessageWithKeyAsync(personTopic,
																				ThreadLocalRandom.current()
																								 .nextLong(1,
																										   101),
																				p));

		Thread.sleep(2000);
		logger.info("----- Going to send messages to multitype topic.");
		Thread.sleep(1000);
		for (Person person : SampleDataGenerator.generatePersons(2)) {
			sampleProducer.sendMessageWithKeyMulti(genericTopic, ThreadLocalRandom.current().nextLong(1, 101), person);
		}
		for (Donation donation : SampleDataGenerator.generateDonations(2)) {
			sampleProducer.sendMessageWithKeyMulti(genericTopic, ThreadLocalRandom.current().nextLong(1, 101),
												   donation);
		}
		for (String content : SampleDataGenerator.generateContent(2)) {
			sampleProducer.sendMessageWithKeyMulti(genericTopic, ThreadLocalRandom.current().nextLong(1, 101),
												   content);
		}
	}
}
