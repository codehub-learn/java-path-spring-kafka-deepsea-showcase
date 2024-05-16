package gr.codelearn.spring.kafka.consume.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${app.kafka.topics.person-forward}")
	private String personsTopicForward;

	@Value("${app.kafka.topics.donation-dlt}")
	private String donationsTopicDeadLetterTopic;

	@Value("${app.kafka.topics.donation-retry}")
	private String donationsTopicRetryTopic;

	@Bean
	public KafkaTemplate<Long, Object> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	private ProducerFactory<Long, Object> producerFactory() {
		return new DefaultKafkaProducerFactory<>(getDefaultConfigurationProperties());
	}

	private Map<String, Object> getDefaultConfigurationProperties() {
		Map<String, Object> configProperties = new HashMap<>();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		configProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
		configProperties.put(ProducerConfig.LINGER_MS_CONFIG, "0");
		configProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
		configProperties.put(ProducerConfig.ACKS_CONFIG, "all");
		configProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "demo-producer");
		configProperties.put(ProducerConfig.RETRIES_CONFIG, "3");
		configProperties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "250");
		configProperties.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, "true");

		return configProperties;
	}

	@Bean
	public KafkaAdmin.NewTopics newTopics() {
		return new KafkaAdmin.NewTopics(createKeylessTopic(personsTopicForward),
										createKeyfulTopic(donationsTopicDeadLetterTopic),
										createKeyfulTopic(donationsTopicRetryTopic));
	}

	private NewTopic createKeyfulTopic(final String topicName) {
		return TopicBuilder.name(topicName)
						   .partitions(6)
						   .replicas(3)
						   .compact()
						   .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
						   .build();
	}

	private NewTopic createKeylessTopic(final String topicName) {
		return TopicBuilder.name(topicName)
						   .partitions(3)
						   .replicas(2)
						   .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2")
						   .build();
	}
}
