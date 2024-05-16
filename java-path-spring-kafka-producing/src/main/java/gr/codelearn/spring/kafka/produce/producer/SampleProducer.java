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

	public void sendMessageWithoutKey(final String topic, final Object message) {
		var future = kafkaTemplate.send(topic, message);

		future.whenComplete((result, ex) -> {
			if (ex != null) {
				logger.error("Error sending message '{}' to topic {}.", message, topic, ex);
			} else {
				logger.debug("{}:{} delivered to {}@{}.", result.getProducerRecord().key(),
							 result.getProducerRecord().value(), result.getRecordMetadata().partition(),
							 result.getRecordMetadata().offset());
			}
		});
	}

	public void sendMessageWithKeyAsync(final String topic, final Long key, final Object message) {
		var future = kafkaTemplate.send(topic, key, message);

		future.whenComplete((result, ex) -> {
			if (ex != null) {
				logger.error("Error sending message '{}' to topic {}.", message, topic, ex);
			} else {
				logger.debug("{}:{} delivered to {}@{}.", result.getProducerRecord().key(),
							 result.getProducerRecord().value(), result.getRecordMetadata().partition(),
							 result.getRecordMetadata().offset());
			}
		});
	}

	public void sendMessageWithKeySync(final String topic, final Long key, final Object message)
			throws ExecutionException, InterruptedException {
		var result = kafkaTemplate.send(topic, key, message).get();
		logger.debug("{}:{} delivered to {}@{}.", result.getProducerRecord().key(),
					 result.getProducerRecord().value(), result.getRecordMetadata().partition(),
					 result.getRecordMetadata().offset());
	}

	public void sendMessageWithKeyRecord(final String topic, final Long key, final Object message) {
		var future = kafkaTemplate.send(getProducerRecord(topic, key, message));

		future.whenComplete((result, ex) -> {
			if (ex != null) {
				logger.error("Error sending message '{}' to topic {}.", message, topic, ex);
			} else {
				logger.debug("{}:{} delivered to {}@{}.", result.getProducerRecord().key(),
							 result.getProducerRecord().value(), result.getRecordMetadata().partition(),
							 result.getRecordMetadata().offset());
			}
		});
	}

	private ProducerRecord<Long, Object> getProducerRecord(final String topic, final Long key, final Object message) {
		var producerRecord = new ProducerRecord<>(topic, key, message);
		producerRecord.headers().add(new RecordHeader("header-key", UUID.randomUUID().toString().getBytes()));
		return producerRecord;
	}

	public void sendMessageWithKeyTransactionally(final String topic, final List<Person> persons) {
		transactionalKafkaTemplate.executeInTransaction(kafkaTemplate -> {
			persons.forEach(p -> {
				var key = ThreadLocalRandom.current().nextLong(1, 101);
				kafkaTemplate.send(topic, key, p);
				logger.debug("Topic {}, {}:{}.", topic, key, p);
			});
			//throw new RuntimeException("Break transaction");
			return true;
		});
	}
}
