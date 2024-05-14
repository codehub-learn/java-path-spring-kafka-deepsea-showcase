package gr.codelearn.spring.kafka.domain;

import java.math.BigDecimal;
import java.util.Date;

public record Donation(Date madeAt, Person donor, BigDecimal amount) {
}
