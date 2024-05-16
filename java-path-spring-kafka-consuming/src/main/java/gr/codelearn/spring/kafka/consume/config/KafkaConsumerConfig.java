package gr.codelearn.spring.kafka.consume.config;

import gr.codelearn.spring.kafka.base.BaseComponent;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig extends BaseComponent {
	private final KafkaTemplate<Long, Object> kafkaTemplate;

	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${app.kafka.topics.generic}")
	private String genericTopic;
	@Value("${app.kafka.topics.person}")
	private String personTopic;
	@Value("${app.kafka.topics.donation}")
	private String donationTopic;

	@Value("${app.kafka.topics.person-forward}")
	private String personsForwardTopic;
	@Value("${app.kafka.topics.donation-dlt}")
	private String donationsDltTopic;
	@Value("${app.kafka.topics.donation-retry}")
	private String donationRetryTopic;

	@Value("${spring.kafka.consumer.group-id}")
	private String consumerGroupId;

	@Bean
	public ConcurrentKafkaListenerContainerFactory<Long, Object> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<Long, Object> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setConcurrency(3);
		factory.getContainerProperties().setIdleBetweenPolls(500);
		factory.getContainerProperties().setPollTimeout(5000);
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.COUNT_TIME);
		factory.getContainerProperties().setAckCount(10);
		factory.getContainerProperties().setAckTime(10000);

		return factory;
	}

	private ConsumerFactory<Long, Object> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(getDefaultConfigurationProperties());
	}

	private Map<String, Object> getDefaultConfigurationProperties() {
		Map<String, Object> configProperties = new HashMap<>();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		configProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		configProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		configProperties.put(ConsumerConfig.DEFAULT_ISOLATION_LEVEL,
							 IsolationLevel.READ_COMMITTED.toString().toLowerCase());
		configProperties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
							 "org.apache.kafka.clients.consumer.RoundRobinAssignor");
		configProperties.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, true);
		configProperties.put(JsonDeserializer.TRUSTED_PACKAGES, "gr.codelearn.spring.kafka.domain");

		return configProperties;
	}
}
