package gr.codelearn.spring.kafka.util;

import gr.codelearn.spring.kafka.domain.Donation;
import gr.codelearn.spring.kafka.domain.Organization;
import gr.codelearn.spring.kafka.domain.Person;
import net.datafaker.Faker;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public final class SampleDataGenerator {
	private static final Faker faker = new Faker();

	public static List<Person> generatePersons(final int howMany) {
		return IntStream.range(1, howMany + 1)
						.mapToObj(i -> generatePerson()).toList();
	}

	private static Person generatePerson() {
		var firstName = faker.name().firstName().replaceAll("'", "");
		var lastName = faker.name().lastName().replaceAll("'", "");
		return new Person(firstName, lastName,
						  String.format("%s.%s@gmailx.com", firstName.toLowerCase(), lastName.toLowerCase()),
						  faker.phoneNumber().phoneNumberInternational(),
						  faker.number().numberBetween(18, 100));
	}

	public static List<Person> generateFilteredOutPersons(final int howMany) {
		return IntStream.range(1, howMany + 1)
						.mapToObj(i -> generateFilteredOutPerson()).toList();
	}

	private static Person generateFilteredOutPerson() {
		var firstName = faker.name().firstName().replaceAll("'", "");
		var lastName = "Invalid";
		return new Person(firstName, lastName,
						  String.format("%s.%s@gmailx.com", firstName.toLowerCase(), lastName.toLowerCase()),
						  faker.phoneNumber().phoneNumberInternational(),
						  faker.number().numberBetween(18, 100));
	}

	public static List<Donation> generateDonations(final int howMany) {
		return IntStream.range(1, howMany + 1)
						.mapToObj(i -> generateDonation()).toList();
	}

	private static Donation generateDonation() {
		return new Donation(faker.date().past(100, TimeUnit.DAYS),
							generatePerson(),
							faker.number().numberBetween(1, 200),
							Organization.values()[ThreadLocalRandom.current()
																   .nextInt(1, (Organization.values().length))]);
	}

	public static List<Donation> generateErroneousDonations(final int howMany) {
		return IntStream.range(1, howMany + 1)
						.mapToObj(i -> generateErroneousDonation()).toList();
	}

	private static Donation generateErroneousDonation() {
		return new Donation(faker.date().past(100, TimeUnit.DAYS),
							null,
							faker.number().numberBetween(1, 200),
							Organization.values()[ThreadLocalRandom.current()
																   .nextInt(1, (Organization.values().length))]);
	}

	public static List<String> generateContent(final int howMany) {
		return IntStream.range(1, howMany + 1)
						.mapToObj(i -> faker.famousLastWords().lastWords()).toList();
	}
}
