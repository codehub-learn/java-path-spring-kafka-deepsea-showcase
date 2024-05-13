package gr.codelearn.spring.kafka.domain;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Builder
public class Person {
	private String firstName;
	private String lastName;
	private String email;
	private String phoneNumber;
	private Integer age;
}
