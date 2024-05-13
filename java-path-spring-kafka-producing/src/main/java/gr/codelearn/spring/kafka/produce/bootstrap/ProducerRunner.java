package gr.codelearn.spring.kafka.produce.bootstrap;

import gr.codelearn.spring.kafka.base.BaseComponent;
import gr.codelearn.spring.kafka.produce.producer.SampleProducer;
import gr.codelearn.spring.kafka.util.SampleDataGenerator;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class ProducerRunner extends BaseComponent implements CommandLineRunner {
	private final SampleProducer sampleProducer;
	@Value("${app.kafka.topic.generic}")
	private String genericTopic;

	@Override
	public void run(final String... args) throws Exception {
		SampleDataGenerator.generatePersons(10).forEach(p -> sampleProducer.sendMessageWithoutKeys(genericTopic,
																								   p));
	}
}
