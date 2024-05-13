package gr.codelearn.spring.kafka.topic.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {
	@Value("${spring.kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean
	public KafkaAdmin generateKafkaAdmin() {
		Map<String, Object> configs = new HashMap<>();
		configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configs.put(AdminClientConfig.CLIENT_ID_CONFIG, "local-admin-1");
		return new KafkaAdmin(configs);
	}

	@Bean
	public NewTopic newTopic() {
		return TopicBuilder.name("test-topic")
						   .config(TopicConfig.RETENTION_MS_CONFIG, "100000")
						   .config(TopicConfig.RETENTION_BYTES_CONFIG, "256000")
						   .partitions(7).replicas(3).build();
	}

	@Bean
	public KafkaAdmin.NewTopics newTopics() {
		return new KafkaAdmin.NewTopics(TopicBuilder.name("test-batch-topic-1")
													.config(TopicConfig.RETENTION_MS_CONFIG, "100000")
													.config(TopicConfig.RETENTION_BYTES_CONFIG, "256000")
													.partitions(6).replicas(3).build(),
										TopicBuilder.name("test-batch-topic-2")
													.config(TopicConfig.RETENTION_MS_CONFIG, "100000")
													.config(TopicConfig.RETENTION_BYTES_CONFIG, "256000")
													.partitions(6).replicas(3).build());
	}
}
