package gr.codelearn.spring.kafka.consume.config;

import gr.codelearn.spring.kafka.base.BaseComponent;
import gr.codelearn.spring.kafka.domain.Donation;
import gr.codelearn.spring.kafka.domain.Person;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import org.springframework.kafka.support.mapping.DefaultJackson2JavaTypeMapper;
import org.springframework.kafka.support.mapping.Jackson2JavaTypeMapper;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.List;
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
	private String personsTopic;

	@Value("${app.kafka.topics.person-forward}")
	private String personsTopicForward;

	@Value("${app.kafka.topics.donation}")
	private String donationsTopic;

	@Value("${app.kafka.topics.donation-dlt}")
	private String donationsTopicDeadLetterTopic;

	@Value("${app.kafka.topics.donation-retry}")
	private String donationsTopicRetryTopic;

	@Value("${spring.kafka.consumer.group-id}")
	private String consumerGroupId;

	@Bean
	public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Long, Object>> kafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<Long, Object> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(genericConsumerFactory());
		factory.setConcurrency(2);
		factory.getContainerProperties().setIdleBetweenPolls(500);
		factory.getContainerProperties().setPollTimeout(5000);
		factory.getContainerProperties().setAckCount(10);
		factory.getContainerProperties().setAckTime(10000);
		factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.COUNT_TIME);
		factory.setCommonErrorHandler(generateErrorHandler());
		factory.setReplyTemplate(kafkaTemplate);
		factory.setRecordFilterStrategy(rec -> {
			if (rec.value() instanceof Person person) {
				return List.of("ignore", "invalid", "skip").contains(person.lastname().toLowerCase());
			} else if (rec.value() instanceof Donation donation) {
				return List.of("ignore", "invalid", "skip").contains(donation.donor().lastname().toLowerCase());
			} else {
				return false;
			}
		});

		return factory;
	}

	private ConsumerFactory<Long, Object> genericConsumerFactory() {
		return new DefaultKafkaConsumerFactory<>(new HashMap<>(getDefaultConfigurationProperties()));
	}

	@Bean("multiTypeKafkaListenerContainerFactory")
	public ConcurrentKafkaListenerContainerFactory<Long, Object> multiTypeKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<Long, Object> factory =
				new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(multiTypeConsumerFactory());
		factory.setRecordMessageConverter(multiTypeConverter());
		factory.setConcurrency(1);
		factory.getContainerProperties().setPollTimeout(10000);
		factory.getContainerProperties().setIdleBetweenPolls(5000);
		return factory;
	}

	private ConsumerFactory<Long, Object> multiTypeConsumerFactory() {
		var configProperties = getDefaultConfigurationProperties();
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

		return new DefaultKafkaConsumerFactory<>(configProperties);
	}

	@Bean
	public RecordMessageConverter multiTypeConverter() {
		StringJsonMessageConverter converter = new StringJsonMessageConverter();

		DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
		typeMapper.setTypePrecedence(Jackson2JavaTypeMapper.TypePrecedence.TYPE_ID);
		typeMapper.addTrustedPackages("gr.codelearn.spring.kafka.domain");

		Map<String, Class<?>> mappings = new HashMap<>();
		mappings.put("donation", Donation.class);
		mappings.put("person", Person.class);

		typeMapper.setIdClassMapping(mappings);
		converter.setTypeMapper(typeMapper);

		return converter;
	}

	private Map<String, Object> getDefaultConfigurationProperties() {
		Map<String, Object> configProperties = new HashMap<>();
		configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
		configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
		configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
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

	private CommonErrorHandler generateErrorHandler() {
		var fixedBackOff = new FixedBackOff(5000L, 3L);

		var expBackOff = new ExponentialBackOff(1000L, 2);
		expBackOff.setMaxInterval(4000L);
		expBackOff.setMaxElapsedTime(15000L);
		expBackOff.setMaxAttempts(5);

		var errorHandler = new DefaultErrorHandler(generatePublishingRecoverer(), fixedBackOff);

		var exceptionsToBeIgnored = List.of(InvalidPropertiesFormatException.class);
		exceptionsToBeIgnored.forEach(errorHandler::addNotRetryableExceptions);

		var exceptionsToBeRetried = List.of(IllegalArgumentException.class);
		exceptionsToBeRetried.forEach(errorHandler::addRetryableExceptions);

		errorHandler.setRetryListeners((erroneousRecord, exception, deliveryAttempt) -> {
			logger.warn("Retrying message {} due to exception {}. Attempt {}.",
						erroneousRecord, exception, deliveryAttempt);
		});
		return errorHandler;
	}

	private DeadLetterPublishingRecoverer generatePublishingRecoverer() {
		return new DeadLetterPublishingRecoverer(kafkaTemplate, (rec, exception) -> {
			logger.error("Exception in publishingRecoverer : {} ", exception.getMessage(), exception);

			if (exception.getCause() instanceof RecoverableDataAccessException) {
				return new TopicPartition(donationsTopicRetryTopic, rec.partition());
			} else {
				return new TopicPartition(donationsTopicDeadLetterTopic, rec.partition());
			}
		});
	}
}
