package gr.codelearn.spring.kafka.produce.bootstrap;

import gr.codelearn.spring.kafka.base.BaseComponent;
import gr.codelearn.spring.kafka.produce.producer.SampleProducer;
import gr.codelearn.spring.kafka.util.SampleDataGenerator;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Profile("!skip-runner")
@Component
@RequiredArgsConstructor
public class ProducerRunner extends BaseComponent implements CommandLineRunner {
	private final SampleProducer sampleProducer;
	@Value("${app.kafka.topic.generic}")
	private String genericTopic;

	@Override
	public void run(final String... args) throws Exception {
		//		logger.info("-----------Sending messages without keys-----------");
		//		Thread.sleep(1000);
		//		SampleDataGenerator.generatePersons(10).forEach(
		//				p -> sampleProducer.sendMessageWithoutKey(genericTopic, p));
		//
		//		Thread.sleep(2000);
		//		logger.info("-----------Sending messages asynchronously-----------");
		//		Thread.sleep(1000);
		//		SampleDataGenerator.generatePersons(100).forEach(
		//				p -> sampleProducer.sendMessageWithKeyAsync(genericTopic, ThreadLocalRandom.current().nextLong
		//				(1, 101),
		//															p));
		//
		//		Thread.sleep(2000);
		//		logger.info("-----------Sending messages synchronously-----------");
		//		Thread.sleep(1000);
		//		SampleDataGenerator.generatePersons(50).forEach(
		//				p -> sampleProducer.sendMessageWithKeyAsync(genericTopic, ThreadLocalRandom.current().nextLong
		//				(1, 101),
		//															p));
		//
		//		Thread.sleep(2000);
		//		logger.info("-----------Sending messages using producer record -----------");
		//		Thread.sleep(1000);
		//		SampleDataGenerator.generatePersons(50).forEach(
		//				p -> sampleProducer.sendMessageWithKeyRecord(genericTopic, ThreadLocalRandom.current()
		//				.nextLong(1,
		//																												101),
		//															 p));
		//		Thread.sleep(2000);
		logger.info("-----------Sending messages using transactions -----------");
		Thread.sleep(1000);

		sampleProducer.sendMessageWithKeyTransactionally(genericTopic, SampleDataGenerator.generatePersons(10));
	}
}
