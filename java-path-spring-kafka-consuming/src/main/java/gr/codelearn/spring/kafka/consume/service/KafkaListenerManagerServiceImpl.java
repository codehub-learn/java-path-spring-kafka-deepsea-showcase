package gr.codelearn.spring.kafka.consume.service;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaListenerManagerServiceImpl {
	private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

	public void pauseListener(final String listenerId) {
		var listener = kafkaListenerEndpointRegistry.getListenerContainer(listenerId);
		if (listener != null) {
			listener.pause();
		}
	}

	public void resumeListener(final String listenerId) {
		var listener = kafkaListenerEndpointRegistry.getListenerContainer(listenerId);
		if (listener != null) {
			listener.resume();
		}
	}
}
