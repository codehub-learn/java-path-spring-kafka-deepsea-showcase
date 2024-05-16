package gr.codelearn.spring.kafka.consume.consumer;

import gr.codelearn.spring.kafka.base.BaseComponent;
import gr.codelearn.spring.kafka.domain.Donation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DonationConsumer extends BaseComponent {
	@KafkaListener(topics = "${app.kafka.topics.donation}", groupId = "donation-group", concurrency = "2")
	public void listen(ConsumerRecord<Long, Donation> consumerRecord) {
		logger.trace("Received {}:{} from {}@{}@{}.", consumerRecord.key(), consumerRecord.value(),
					 consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
	}
}
