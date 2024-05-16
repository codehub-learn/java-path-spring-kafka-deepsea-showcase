package gr.codelearn.spring.kafka.produce.bootstrap;

import gr.codelearn.spring.kafka.base.BaseComponent;
import gr.codelearn.spring.kafka.produce.producer.SampleProducer;
import gr.codelearn.spring.kafka.util.SampleDataGenerator;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Profile("!skip-error-runners")
@Component
@RequiredArgsConstructor
public class ProducerErrorRunner extends BaseComponent implements CommandLineRunner {
	private final SampleProducer sampleProducer;

	@Value("${app.kafka.topics.donation}")
	private String donationTopic;

	@Override
	public void run(final String... args) throws Exception {

		logger.info("----- Going to send erroneous messages.");
		Thread.sleep(1000);
		SampleDataGenerator.generateErroneousDonations(2)
						   .forEach(p -> sampleProducer.sendMessageWithKeyAsync(donationTopic,
																				ThreadLocalRandom.current()
																								 .nextLong(1, 101),
																				p));
	}
}
