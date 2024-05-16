package gr.codelearn.spring.kafka.produce.service;

import gr.codelearn.spring.kafka.base.BaseComponent;
import gr.codelearn.spring.kafka.produce.producer.SampleProducer;
import gr.codelearn.spring.kafka.util.SampleDataGenerator;
import lombok.RequiredArgsConstructor;
import net.datafaker.Faker;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.LongStream;

@Service
@RequiredArgsConstructor
public class ProducerServiceImpl extends BaseComponent implements ProducerService {
	private final SampleProducer sampleProducer;

	@Value("${app.kafka.topics.generic}")
	private String genericTopic;

	@Value("${app.kafka.topics.person}")
	private String personsTopic;

	@Value("${app.kafka.topics.donation}")
	private String donationsTopic;

	private final Faker faker = new Faker();
	private static final Integer NUM_OF_MESSAGES = 1;

	@Override
	//	@Scheduled(cron = "0/10 * * * * ?")
	public void produceSampleMessages() {
		LongStream.range(0, NUM_OF_MESSAGES).forEach(i -> {
			sampleProducer.sendMessageWithoutKey(genericTopic, faker.restaurant().description());
		});
	}

	@Override
	@Scheduled(cron = "0/5 * * * * ?")
	public void producePersons() {
		SampleDataGenerator.generatePersons(1).forEach(person -> {
			sampleProducer.sendMessageWithKeyAsync(personsTopic, ThreadLocalRandom.current().nextLong(1, 101),
												   person);
		});
	}

	@Override
	@Scheduled(cron = "0/5 * * * * ?")
	public void produceDonations() {
		SampleDataGenerator.generateDonations(5).forEach(donation -> {
			sampleProducer.sendMessageWithKeyAsync(donationsTopic, ThreadLocalRandom.current().nextLong(1, 101),
												   donation);
		});
	}
}
