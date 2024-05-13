package gr.codelearn.spring.kafka.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.math.BigDecimal;
import java.util.Date;

@Getter
@Setter
@ToString
@Builder
public class Donation {
	private Date madeAt;
	private Person donor;
	private BigDecimal amount;
}
