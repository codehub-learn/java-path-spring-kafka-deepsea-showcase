package gr.codelearn.spring.kafka.domain;

import java.util.Date;

public record Donation(Date madeAt, Person donor, Integer amount, Organization organization) {
}
