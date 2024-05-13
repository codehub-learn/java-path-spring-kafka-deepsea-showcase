package gr.codelearn.spring.kafka.produce.producer;

import gr.codelearn.spring.kafka.base.BaseComponent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class SampleProducer extends BaseComponent {
	private final KafkaTemplate<Long, Object> kafkaTemplate;

	public void sendMessageWithoutKeys(final String topic, final Object message) {
		var future = kafkaTemplate.send(topic, message);
		logger.debug("Sent message: {} to topic: {}", message, topic);

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
}
