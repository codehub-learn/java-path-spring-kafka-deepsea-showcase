package gr.codelearn.spring.kafka.produce.producer;

import gr.codelearn.spring.kafka.base.BaseComponent;
import gr.codelearn.spring.kafka.domain.Person;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

@Component
@RequiredArgsConstructor
public class SampleProducer extends BaseComponent {
	private final KafkaTemplate<Long, Object> kafkaTemplate;

	@Autowired
	@Qualifier("transactionalKafkaTemplate")
	private final KafkaTemplate<Long, Object> transactionalKafkaTemplate;

	@Autowired
	@Qualifier("multiTypeKafkaTemplate")
	private final KafkaTemplate<Long, Object> multiTypeKafkaTemplate;

	public void sendMessageWithoutKey(final String topic, final Object value) {
		kafkaTemplate.send(topic, value);
		logger.debug("Topic {}, {}.", topic, value);
	}

	public void sendMessageWithKeyAsync(final String topic, final Long key, final Object value) {
		var future = kafkaTemplate.send(topic, key, value);

		future.whenComplete((result, ex) -> {
			if (ex == null) {
				logger.debug("{}:{}, delivered to {}@{}.", result.getProducerRecord().key(),
							 result.getProducerRecord().value(), result.getRecordMetadata().partition(),
							 result.getRecordMetadata().offset());
			} else {
				logger.warn("Unable to deliver message {}:{}.", key, value, ex);
			}
		});
	}

	public void sendMessageWithKeySync(final String topic, final Long key, final Object value)
			throws ExecutionException, InterruptedException {
		var result = kafkaTemplate.send(topic, key, value).get();

		logger.debug("{}:{}, delivered to {}@{}.", result.getProducerRecord().key(),
					 result.getProducerRecord().value(), result.getRecordMetadata().partition(),
					 result.getRecordMetadata().offset());
	}

	public void sendMessageWithKeyRecord(final String topic, final Long key, final Object value) {
		var future = kafkaTemplate.send(generateProducerRecord(topic, key, value));

		future.whenComplete((result, ex) -> {
			if (ex == null) {
				logger.debug("{}:{}, delivered to {}@{}.", result.getProducerRecord().key(),
							 result.getProducerRecord().value(), result.getRecordMetadata().partition(),
							 result.getRecordMetadata().offset());
			} else {
				logger.warn("Unable to deliver message {}:{}.", key, value, ex);
			}
		});
	}

	public void sendMessageWithKeyTransactionally(final String topic, final List<Person> persons) {
		transactionalKafkaTemplate.executeInTransaction(kafkaTemplate -> {
			persons.forEach(p -> {
				var key = ThreadLocalRandom.current().nextLong(1, 7);
				kafkaTemplate.send(topic, key, p);
				logger.debug("Topic {}, {}:{}.", topic, key, p);
			});
			//throw new RuntimeException("Break transaction");
			return true;
		});
	}

	public void sendMessageWithKeyMulti(final String topic, final Long key, final Object value)
			throws ExecutionException, InterruptedException {
		var result = multiTypeKafkaTemplate.send(topic, key, value).get();

		logger.debug("{}:{}, delivered to {}@{}.", result.getProducerRecord().key(),
					 result.getProducerRecord().value(), result.getRecordMetadata().partition(),
					 result.getRecordMetadata().offset());
	}

	private ProducerRecord<Long, Object> generateProducerRecord(final String topic, final Long key,
																final Object value) {
		var producerRecord = new ProducerRecord<>(topic, key, value);
		producerRecord.headers().add(new RecordHeader("Sample header", UUID.randomUUID().toString().getBytes()));

		return producerRecord;
	}
}
