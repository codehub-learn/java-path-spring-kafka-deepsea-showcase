package gr.codelearn.spring.kafka.consume.consumer;

import gr.codelearn.spring.kafka.base.BaseComponent;
import gr.codelearn.spring.kafka.domain.Person;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

@Component
public class PersonConsumer extends BaseComponent {
	@KafkaListener(topics = "${app.kafka.topics.person}", groupId = "person-group", concurrency = "1")
	@SendTo("${app.kafka.topics.person-forward}")
	public Message<?> listen(ConsumerRecord<Long, Person> consumerRecord) {
		logger.trace("Received {}:'{}' from {}@{}@{}.",
					 consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(), consumerRecord
							 .partition(),
					 consumerRecord.offset());
		if (consumerRecord.value().email() == null) {
			throw new IllegalArgumentException("Email is null.");
		}

		return MessageBuilder.withPayload(consumerRecord.value())
							 .setHeader(KafkaHeaders.RECEIVED_PARTITION, consumerRecord.partition())
							 .setHeader(KafkaHeaders.OFFSET, consumerRecord.offset())
							 .setHeader(KafkaHeaders.TIMESTAMP, consumerRecord.timestamp())
							 .setHeader(KafkaHeaders.RECEIVED_TOPIC, consumerRecord.topic())
							 .setHeader(KafkaHeaders.RECEIVED_KEY, consumerRecord.key())
							 .setHeader(KafkaHeaders.CORRELATION_ID, String.format("%s-%s-%s-%s",
																				   consumerRecord.topic(),
																				   consumerRecord.partition(),
																				   consumerRecord.offset(),
																				   consumerRecord.key()))
							 .build();
	}
}
