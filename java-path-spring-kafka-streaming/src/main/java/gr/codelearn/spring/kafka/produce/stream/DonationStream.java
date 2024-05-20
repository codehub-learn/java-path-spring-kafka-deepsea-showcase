package gr.codelearn.spring.kafka.produce.stream;

import gr.codelearn.spring.kafka.domain.Donation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;

@Configuration
public class DonationStream {
	@Value("${app.kafka.topics.donation}")
	private String donationsTopic;
	@Value("${app.kafka.topics.donation-organization}")
	private String donationsPerOrganizationTopic;
	@Value("${app.kafka.topics.donation-count}")
	private String donationsCountTopic;
	@Value("${app.kafka.topics.donation-high}")
	private String donationsHighTopic;
	@Value("${app.kafka.topics.donation-low}")
	private String donationsLowTopic;

	//	@Bean
	public KStream<Long, Donation> streamFilterLargeDonations(StreamsBuilder builder) {
		var donationSerde = new JsonSerde<>(Donation.class);

		var largeDonationsStream = builder.stream(donationsTopic, Consumed.with(Serdes.Long(), donationSerde))
										  .filter((k, v) -> v.amount().compareTo(BigDecimal.valueOf(100L)) >= 0);

		largeDonationsStream.to(donationsHighTopic);
		//largeDonationsStream.print(Printed.toSysOut());

		return largeDonationsStream;
	}

	//	@Bean
	public KStream<Long, Donation> streamDynamicRouteDonations(StreamsBuilder builder) {
		var donationSerde = new JsonSerde<>(Donation.class);

		var donationsStream = builder.stream(donationsTopic, Consumed.with(Serdes.Long(), donationSerde));
		TopicNameExtractor<Long, Donation> sensorTopicExtractor =
				(k, v, recordContext) -> "%s_%s".formatted(v.donor().age(), v.donor().firstname());
		donationsStream.to(sensorTopicExtractor);

		return donationsStream;
	}

	//	@Bean
	public KStream<Long, Donation> streamSplitDonations(StreamsBuilder builder) {
		var donationSerde = new JsonSerde<>(Donation.class);

		var donationsStream = builder.stream(donationsTopic, Consumed.with(Serdes.Long(), donationSerde));

		donationsStream.split()
					   .branch((k, v) -> v.amount().compareTo(BigDecimal.valueOf(100L)) >= 0,
							   Branched.withConsumer(kstream -> kstream.to(donationsHighTopic)))
					   .branch((k, v) -> v.amount().compareTo(BigDecimal.valueOf(100L)) < 0,
							   // Allows a more dynamic approach
							   Branched.withConsumer(kstream -> kstream.to((k, v, recordContext) -> donationsLowTopic)))
					   .noDefaultBranch();

		return donationsStream;
	}

	@Bean
	public KStream<Long, Donation> streamSplitDonationsWithBrancher(StreamsBuilder builder) {
		var donationSerde = new JsonSerde<>(Donation.class);

		var donationsStream = builder.stream(donationsTopic, Consumed.with(Serdes.Long(), donationSerde));
		new KafkaStreamBrancher<Long, Donation>()
				.branch((k, v) -> v.amount().compareTo(BigDecimal.valueOf(100L)) >= 0,
						(kstream -> kstream.to(donationsHighTopic)))
				.branch((k, v) -> v.amount().compareTo(BigDecimal.valueOf(100L)) < 0,
						(kstream -> kstream.to((k, v, recordContext) -> donationsLowTopic)))
				//.defaultBranch()
				.onTopOf(donationsStream);

		return donationsStream;
	}
}
