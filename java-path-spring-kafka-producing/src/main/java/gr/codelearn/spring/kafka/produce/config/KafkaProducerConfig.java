package gr.codelearn.spring.kafka.produce.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
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

	@Value("${app.kafka.topic.generic}")
	private String genericTopic;
	@Value("${app.kafka.topic.person}")
	private String personsTopic;
	@Value("${app.kafka.topic.donation}")
	private String donationTopic;

	@Bean
	public KafkaAdmin generateKafkaAdmin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configs.put(AdminClientConfig.CLIENT_ID_CONFIG, "local-admin-1");
		return new KafkaAdmin(configs);
	}

	@Bean
	public NewTopic generateNewTopic() {
		return TopicBuilder.name(genericTopic)
						   .partitions(6).replicas(2).build();
	}

	@Bean
	public KafkaTemplate<Long, Object> generateKafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}

	private ProducerFactory<Long, Object> producerFactory() {
		return new DefaultKafkaProducerFactory<>(getDefaultConfigurationProperties());
	}

	private Map<String, Object> getDefaultConfigurationProperties() {
		return Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
					  ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class,
					  ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
					  ProducerConfig.BATCH_SIZE_CONFIG, "16384",
					  ProducerConfig.LINGER_MS_CONFIG, "0",
					  ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000",
					  ProducerConfig.ACKS_CONFIG, "all",
					  ProducerConfig.CLIENT_ID_CONFIG, "demo-producer",
					  ProducerConfig.RETRIES_CONFIG, "3",
					  JsonSerializer.ADD_TYPE_INFO_HEADERS, "true");
	}
}
