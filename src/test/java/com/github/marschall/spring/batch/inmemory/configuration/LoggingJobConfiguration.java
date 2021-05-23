package com.github.marschall.spring.batch.inmemory.configuration;

import java.lang.invoke.MethodHandles;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.CallableTaskletAdapter;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures a single job with three steps that just log.
 */
@Configuration
public class LoggingJobConfiguration {

  private static final Log LOGGER = LogFactory.getLog(MethodHandles.lookup().lookupClass());

  @Autowired
  public JobBuilderFactory jobBuilderFactory;

  @Autowired
  public StepBuilderFactory stepBuilderFactory;

  @Bean
  public Job loggingJob() {
    return this.jobBuilderFactory.get("loggingJob")
      .incrementer(new RunIdIncrementer())
      .start(this.step1())
      .next(this.step2())
      .next(this.step3())
      .build();
  }

  @Bean
  public Step step1() {
    return this.stepBuilderFactory.get("step1")
      .tasklet(this.loggingTasklet("step1"))
      .build();
  }

  @Bean
  public Step step2() {
    return this.stepBuilderFactory.get("step2")
            .tasklet(this.loggingTasklet("step2"))
            .build();
  }

  @Bean
  public Step step3() {
    return this.stepBuilderFactory.get("step3")
            .tasklet(this.loggingTasklet("step3"))
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
