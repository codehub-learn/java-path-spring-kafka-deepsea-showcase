package gr.codelearn.spring.kafka.consume.consumer;

import gr.codelearn.spring.kafka.base.BaseComponent;
import gr.codelearn.spring.kafka.domain.Donation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;

import java.util.Objects;

//@Component
public class DonationConsumer extends BaseComponent {
	@KafkaListener(topics = "${app.kafka.topics.donation}", groupId = "donation-group", concurrency = "1")
	public void listen(ConsumerRecord<Long, Donation> consumerRecord) {
		logger.trace("Received {}:'{}' from {}@{}@{}.",
					 consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(), consumerRecord
							 .partition(),
					 consumerRecord.offset());
		if (consumerRecord.value().donor() == null) {
			throw new IllegalArgumentException("Donor is null.");
		}
	}

	//	@KafkaListener(topics = "${app.kafka.topics.donation}", groupId = "donation-group")
	public void onMessage(final ConsumerRecord<Long, Object> consumerRecord, final Acknowledgment acknowledgment) {
		// Assumes factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
		logger.trace("Received {}:'{}' from {}@{}@{}.",
					 consumerRecord.key(), consumerRecord.value(), consumerRecord.topic(), consumerRecord.partition(),
					 consumerRecord.offset());
		Objects.requireNonNull(acknowledgment).acknowledge();
	}
}
