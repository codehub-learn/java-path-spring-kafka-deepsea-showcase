package gr.codelearn.spring.kafka.consume.consumer;

import gr.codelearn.spring.kafka.base.BaseComponent;
import gr.codelearn.spring.kafka.domain.Donation;
import gr.codelearn.spring.kafka.domain.Person;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@KafkaListener(id = "generic-multi-1", topics = "${app.kafka.topics.generic}",
			   containerFactory = "multiTypeKafkaListenerContainerFactory")
public class MultiTypeConsumer extends BaseComponent {
	@KafkaHandler
	public void handle(Donation donation, ConsumerRecordMetadata metadata,
					   @Header(KafkaHeaders.RECEIVED_KEY) String key) {
		logger.trace("Received {}:'{}' from {}@{}@{}.", key, donation, metadata.topic(), metadata.partition(),
					 metadata.offset());
	}

	@KafkaHandler
	public void handle(Person person, ConsumerRecordMetadata metadata,
					   @Header(KafkaHeaders.RECEIVED_KEY) String key) {
		logger.trace("Received {}:'{}' from {}@{}@{}.", key, person, metadata.topic(), metadata.partition(),
					 metadata.offset());
	}

	@KafkaHandler(isDefault = true)
	public void listenByDefault(String payload, ConsumerRecordMetadata metadata) {
		logger.trace("Received by default {}:'{}' from {}@{}@{}.", "N/A", payload, metadata.topic(),
					 metadata.partition(), metadata.offset());
	}
}
