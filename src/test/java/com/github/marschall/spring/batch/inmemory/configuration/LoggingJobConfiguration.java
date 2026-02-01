package com.github.marschall.spring.batch.inmemory.configuration;

import java.lang.invoke.MethodHandles;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.CallableTaskletAdapter;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.infrastructure.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Configures a single job with three steps that just log.
 */
@Configuration
public class LoggingJobConfiguration {

  private static final Log LOGGER = LogFactory.getLog(MethodHandles.lookup().lookupClass());
  
  @Autowired
  public JobRepository jobRepository;
  
  @Autowired
  public PlatformTransactionManager transactionManager;

  @Bean
  public Job loggingJob() {
    return new JobBuilder("loggingJob", this.jobRepository)
      .start(this.step1())
      .next(this.step2())
      .next(this.step3())
      .build();
  }

  @Bean
  public Step step1() {
    return new StepBuilder("step1", this.jobRepository)
      .tasklet(this.loggingTasklet("step1"), this.transactionManager)
      .build();
  }

  @Bean
  public Step step2() {
    return new StepBuilder("step2", this.jobRepository)
            .tasklet(this.loggingTasklet("step2"), this.transactionManager)
            .build();
  }

  @Bean
  public Step step3() {
    return new StepBuilder("step3", this.jobRepository)
            .tasklet(this.loggingTasklet("step3"), this.transactionManager)
            .build();
  }

  private Tasklet loggingTasklet(String stepName) {
    return new CallableTaskletAdapter(() -> {
      LOGGER.info("executing step: " + stepName);
      return RepeatStatus.FINISHED;
    });
  }

}
