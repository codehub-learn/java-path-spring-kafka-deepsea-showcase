package gr.codelearn.spring.kafka.produce.stream;

import gr.codelearn.spring.kafka.domain.Donation;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaStreamBrancher;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;

@Configuration
public class DonationStream {
	@Value("${app.kafka.topics.donation}")
	private String donationsTopic;

	/* We will proceed with topic defaults regarding the topics we are going to utilize in stream examples. We are
	 * still injecting source main topics.
	 * NOTE:
	 * This is not proposed for production environments
	 */

	//	@Bean
	public KStream<Long, Donation> streamFilterLargeDonations(StreamsBuilder builder) {
		var donationSerde = new JsonSerde<>(Donation.class);

		var largeDonationsStream = builder.stream(donationsTopic, Consumed.with(Serdes.Long(), donationSerde))
										  .filter((k, v) -> v.amount().compareTo(100) >= 0);

		largeDonationsStream.to("demo-kafka-donations-high");
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
					   .branch((k, v) -> v.amount().compareTo(100) >= 0,
							   Branched.withConsumer(kstream -> kstream.to("demo-kafka-donations-high")))
					   .branch((k, v) -> v.amount().compareTo(100) < 0,
							   // Allows a more dynamic approach
							   Branched.withConsumer(
									   kstream -> kstream.to((k, v, recordContext) -> "demo-kafka-donations-low")))
					   .noDefaultBranch();

		return donationsStream;
	}

	//	@Bean
	public KStream<Long, Donation> streamSplitDonationsWithBrancher(StreamsBuilder builder) {
		var donationSerde = new JsonSerde<>(Donation.class);

		var donationsStream = builder.stream(donationsTopic, Consumed.with(Serdes.Long(), donationSerde));
		new KafkaStreamBrancher<Long, Donation>()
				.branch((k, v) -> v.amount().compareTo(100) >= 0,
						(kstream -> kstream.to("demo-kafka-donations-high")))
				.branch((k, v) -> v.amount().compareTo(100) < 0,
						(kstream -> kstream.to((k, v, recordContext) -> "demo-kafka-donations-low")))
				//.defaultBranch()
				.onTopOf(donationsStream);

		return donationsStream;
	}

	//	@Bean
	public KStream<String, Donation> streamDonationsGroupByOrganization(StreamsBuilder builder) {
		var donationSerde = new JsonSerde<>(Donation.class);

		var donationsStream = builder.stream(donationsTopic, Consumed.with(Serdes.Long(), donationSerde))
									 .map((k, v) -> KeyValue.pair(v.organization().toString(), v));

		donationsStream.groupBy((k, v) -> k, Grouped.with(Serdes.String(), donationSerde))
					   .count()
					   .toStream()
					   .to("demo-kafka-donations-count");

		return donationsStream;
	}

	//	@Bean
	public KStream<Long, Donation> streamSumDonationAmountPerOrganization(StreamsBuilder builder) {
		var donationSerde = new JsonSerde<>(Donation.class);

		var donationsStream = builder.stream(donationsTopic, Consumed.with(Serdes.Long(), donationSerde));
		donationsStream.map((k, v) -> KeyValue.pair(v.organization().toString(), v.amount()))
					   .groupByKey()
					   .reduce(Integer::sum)
					   .toStream()
					   .to("demo-kafka-donations-sum-per-org", Produced.with(Serdes.String(), Serdes.Integer()));

		return donationsStream;

	}

	//	@Bean
	public KStream<Long, Donation> streamSumDonationAmountPerWindow(StreamsBuilder builder) {
		var donationSerde = new JsonSerde<>(Donation.class);

		var windowLength = Duration.ofMinutes(3L);
		var advanceLength = Duration.ofMinutes(1L);
		var hopLength = Duration.ofSeconds(10L);
		var windowSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, windowLength.toMillis());

		var donationsStream = builder.stream(donationsTopic, Consumed.with(Serdes.Long(), donationSerde));
		donationsStream.map((k, v) -> KeyValue.pair(v.organization().toString(), v.amount()))
					   .groupByKey()
					   .windowedBy(TimeWindows.ofSizeWithNoGrace(windowLength))
					   //					   .windowedBy(TimeWindows.ofSizeWithNoGrace(windowLength).advanceBy
					   //					   (advanceLength))
					   //					   .windowedBy(TimeWindows.ofSizeAndGrace(windowLength, hopLength))
					   .reduce(Integer::sum, Materialized.with(Serdes.String(), Serdes.Integer()))
					   .toStream()
					   .through("demo-kafka-donations-sum-per-window",
								Produced.with(windowSerde, Serdes.Integer()))
					   .print(Printed.toSysOut());

		return donationsStream;
	}
}
