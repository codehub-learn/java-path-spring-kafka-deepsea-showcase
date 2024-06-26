package gr.codelearn.spring.kafka.produce.config;

import gr.codelearn.spring.kafka.base.BaseComponent;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig extends BaseComponent {
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Value("${app.kafka.topics.generic}")
	private String genericTopic;
	@Value("${app.kafka.topics.person}")
	private String personsTopic;
	@Value("${app.kafka.topics.donation}")
	private String donationTopic;

	@Bean
	public KafkaAdmin generateKafkaAdmin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configs.put(AdminClientConfig.CLIENT_ID_CONFIG, "local-admin-1");
		return new KafkaAdmin(configs);
	}

	@Primary
	@Bean
	public KafkaTemplate<Long, Object> kafkaTemplate() {
		var kafkaTemplate = new KafkaTemplate<>(producerFactory(false));
		//		kafkaTemplate.setProducerListener(new ProducerListener<>() {
		//			@Override
		//			public void onSuccess(ProducerRecord<Long, Object> producerRecord, RecordMetadata recordMetadata) {
		//				logger.trace("ACK received from broker for record with key {} and value {} at offset {}",
		//							 producerRecord.key(), producerRecord.value(), recordMetadata.offset());
		//			}
		//
		//			@Override
		//			public void onError(ProducerRecord<Long, Object> producerRecord, RecordMetadata recordMetadata,
		//								Exception exception) {
		//				logger.warn("Unable to produce message for record with key {} and value {}.",
		//							producerRecord.key(), producerRecord.value(), exception);
		//			}
		//		});

		return kafkaTemplate;
	}

	@Bean("transactionalKafkaTemplate")
	public KafkaTemplate<Long, Object> transactionalkafkaTemplate() {
		return new KafkaTemplate<>(producerFactory(true));
	}

	private ProducerFactory<Long, Object> producerFactory(final boolean transactional) {
		var configProperties = getDefaultConfigurationProperties();
		if (transactional) {
			configProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "X1-");
			configProperties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000);
		}
		return new DefaultKafkaProducerFactory<>(configProperties);
	}

	@Bean("multiTypeKafkaTemplate")
	public KafkaTemplate<Long, Object> multiTypeKafkaTemplate() {
		return new KafkaTemplate<>(multiTypeFactory());
	}

	private ProducerFactory<Long, Object> multiTypeFactory() {
		var configProperties = getDefaultConfigurationProperties();
		return new DefaultKafkaProducerFactory<>(configProperties);
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
		configProperties.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, "true");

		return configProperties;
	}

	@Bean
	public KafkaAdmin.NewTopics generateTopics() {
		return new KafkaAdmin.NewTopics(createKeylessTopic(genericTopic),
										createKeyfulTopic(personsTopic),
										createKeyfulTopic(donationTopic));
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
