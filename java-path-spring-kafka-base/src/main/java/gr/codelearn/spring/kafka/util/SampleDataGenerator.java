package gr.codelearn.spring.kafka.util;

import gr.codelearn.spring.kafka.domain.Donation;
import gr.codelearn.spring.kafka.domain.Person;
import net.datafaker.Faker;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public final class SampleDataGenerator {
	private static final Faker faker = new Faker();

	public static List<Person> generatePersons(final int howMany) {
		return IntStream.range(0, howMany)
						.mapToObj(i -> generatePerson())
						.toList();
	}

	private static Person generatePerson() {
		var firstName = faker.name().firstName();
		var lastName = faker.name().lastName();

		return Person.builder()
					 .firstName(firstName)
					 .lastName(lastName)
					 .email(String.format("%s.%s@gmailx.com", firstName.toLowerCase(), lastName.toLowerCase()))
					 .phoneNumber(faker.phoneNumber().phoneNumberInternational())
					 .age(faker.number().numberBetween(18, 100))
					 .build();
	}

	public static List<Donation> generateDomations(final int howMany) {
		return IntStream.range(0, howMany)
						.mapToObj(i -> generateDonation())
						.toList();
	}

	private static Donation generateDonation() {
		return Donation.builder().madeAt(faker.date().past(100, TimeUnit.DAYS))
					   .donor(generatePerson())
					   .amount(BigDecimal.valueOf(faker.number().randomDouble(2, 1, 100)))
					   .build();
	}
}
