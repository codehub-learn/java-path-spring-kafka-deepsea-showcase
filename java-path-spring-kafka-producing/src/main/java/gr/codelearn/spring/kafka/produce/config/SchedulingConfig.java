package gr.codelearn.spring.kafka.produce.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Profile("enable-scheduling")
@Configuration
@EnableScheduling
public class SchedulingConfig {
	@Bean
	public TaskScheduler taskScheduler() {
		var taskScheduler = new ThreadPoolTaskScheduler();
		taskScheduler.setPoolSize(4);
		taskScheduler.setThreadNamePrefix("task-scheduler-");

		taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
		taskScheduler.setAwaitTerminationSeconds(60);

		taskScheduler.initialize();

		return taskScheduler;
	}
}
