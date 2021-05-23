package com.github.marschall.spring.batch.inmemory.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcOperations;

import com.github.marschall.spring.batch.inmemory.InsertTasklet;

/**
 * Configures a single job with a single step that just inserts a single row into a table.
 */
@Configuration
public class JdbcInsertingJobConfiguration {

  @Autowired
  public JobBuilderFactory jobBuilderFactory;

  @Autowired
  public StepBuilderFactory stepBuilderFactory;

  @Autowired
  private JdbcOperations jdbcOperations;

  @Bean
  public Job insertingJob() {
    return this.jobBuilderFactory.get("insertingJob")
      .incrementer(new RunIdIncrementer())
      .start(this.insertingStep())
      .build();
  }

  @Bean
  public Step insertingStep() {
    return this.stepBuilderFactory.get("insertingStep")
      .tasklet(new InsertTasklet(this.jdbcOperations))
      .build();
  }


}
