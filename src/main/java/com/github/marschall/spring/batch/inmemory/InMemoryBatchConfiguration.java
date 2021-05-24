package com.github.marschall.spring.batch.inmemory;

import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.AbstractBatchConfiguration;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.JobScope;
import org.springframework.batch.core.scope.StepScope;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * An alternative to {@link AbstractBatchConfiguration} that sets up Spring Batch
 * with a in-memory {@link JobRepository} and {@link JobExplorer} without the need
 * for a {@link BatchConfigurer}.
 */
@Configuration
public class InMemoryBatchConfiguration {

  private final InMemoryJobStorage storge;

  public InMemoryBatchConfiguration() {
    this.storge = new InMemoryJobStorage();
  }

  @Bean
  public JobBuilderFactory jobBuilders() {
    return new JobBuilderFactory(this.jobRepository());
  }

  @Bean
  public StepBuilderFactory stepBuilders() {
    // the in-memory job repository and job explorer do not support transactions
    // avoid the additional transaction by Spring Batch
    return new StepBuilderFactory(this.jobRepository(), new ResourcelessTransactionManager());
  }

  @Bean
  public JobRepository jobRepository() {
    return new InMemoryJobRepository(this.storge);
  }

  @Bean
  public JobLauncher jobLauncher() {
    SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
    jobLauncher.setJobRepository(this.jobRepository());
    return jobLauncher;
  }

  @Bean
  public JobExplorer jobExplorer() {
    return new InMemoryJobExplorer(this.storge);
  }

  @Bean
  public JobRegistry jobRegistry() {
    return new MapJobRegistry();
  }

  @Bean
  public static StepScope stepScope() {
    StepScope stepScope = new StepScope();
    stepScope.setAutoProxy(false);
    return stepScope;
  }

  @Bean
  public static JobScope jobScope() {
    JobScope jobScope = new JobScope();
    jobScope.setAutoProxy(false);
    return jobScope ;
  }

}
