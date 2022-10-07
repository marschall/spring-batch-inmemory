package com.github.marschall.spring.batch.inmemory.configuration;

import java.lang.invoke.MethodHandles;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.CallableTaskletAdapter;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
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
  public PlatformTransactionManager txManager;

  @Bean
  public Job loggingJob() {
    return new JobBuilder("loggingJob", this.jobRepository)
      .incrementer(new RunIdIncrementer())
      .start(this.step1())
      .next(this.step2())
      .next(this.step3())
      .build();
  }

  @Bean
  public Step step1() {
    return new StepBuilder("step1", this.jobRepository)
      .tasklet(this.loggingTasklet("step1"), this.txManager)
      .build();
  }

  @Bean
  public Step step2() {
    return new StepBuilder("step2", this.jobRepository)
            .tasklet(this.loggingTasklet("step2"), this.txManager)
            .build();
  }

  @Bean
  public Step step3() {
    return new StepBuilder("step3", this.jobRepository)
            .tasklet(this.loggingTasklet("step3"), this.txManager)
            .build();
  }

  private Tasklet loggingTasklet(String stepName) {
    CallableTaskletAdapter taskletAdapter = new CallableTaskletAdapter();
    taskletAdapter.setCallable(() -> {
      LOGGER.info("executing step: " + stepName);
      return RepeatStatus.FINISHED;
    });
    return taskletAdapter;
  }

}
