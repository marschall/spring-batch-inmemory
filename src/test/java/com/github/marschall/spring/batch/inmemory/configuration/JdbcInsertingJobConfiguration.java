package com.github.marschall.spring.batch.inmemory.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.transaction.PlatformTransactionManager;

import com.github.marschall.spring.batch.inmemory.InsertTasklet;

/**
 * Configures a single job with a single step that just inserts a single row into a table.
 */
@Configuration
public class JdbcInsertingJobConfiguration {
  
  @Autowired
  public PlatformTransactionManager transactionManager;
  
  @Autowired
  public JobRepository jobRepository;

  @Autowired
  private JdbcOperations jdbcOperations;

  @Bean
  public Job insertingJob() {
    return new JobBuilder("insertingJob", this.jobRepository)
      .incrementer(new RunIdIncrementer())
      .start(this.insertingStep())
      .build();
  }

  @Bean
  public Step insertingStep() {
    return new StepBuilder("insertingStep", this.jobRepository)
      .tasklet(new InsertTasklet(this.jdbcOperations), this.transactionManager)
      .build();
  }

}
