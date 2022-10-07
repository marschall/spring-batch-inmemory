package com.github.marschall.spring.batch.inmemory;

import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.JobScope;
import org.springframework.batch.core.scope.StepScope;
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

  /**
   * Constructs a new {@link InMemoryBatchConfiguration}.
   */
  public InMemoryBatchConfiguration() {
    this.storge = new InMemoryJobStorage();
  }

  /**
   * Defines the {@link InMemoryJobStorage} bean.
   *
   * @return the {@link InMemoryJobStorage} bean.
   */
  @Bean
  public InMemoryJobStorage inMemoryJobStorage() {
    return this.storge;
  }

  /**
   * Defines the {@link JobBuilderFactory} bean.
   *
   * @return the {@link JobBuilderFactory} bean.
   */
  @Bean
  public JobBuilderFactory jobBuilders() {
    return new JobBuilderFactory(this.jobRepository());
  }

  /**
   * Defines the {@link StepBuilderFactory} bean.
   *
   * @return the {@link StepBuilderFactory} bean.
   */
  @Bean
  public StepBuilderFactory stepBuilders() {
    return new StepBuilderFactory(this.jobRepository());
  }

  /**
   * Defines the {@link JobRepository} bean which will be a {@link InMemoryJobRepository}.
   *
   * @return the {@link JobRepository} bean.
   */
  @Bean
  public JobRepository jobRepository() {
    return new InMemoryJobRepository(this.storge);
  }

  /**
   * Defines the {@link JobLauncher} bean.
   *
   * @return the {@link JobLauncher} bean.
   */
  @Bean
  public JobLauncher jobLauncher() {
    TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
    jobLauncher.setJobRepository(this.jobRepository());
    return jobLauncher;
  }

  /**
   * Defines the {@link JobRepository} bean which will be a {@link InMemoryJobExplorer}.
   *
   * @return the {@link JobRepository} bean.
   */
  @Bean
  public JobExplorer jobExplorer() {
    return new InMemoryJobExplorer(this.storge);
  }

  /**
   * Defines the {@link JobRegistry} bean.
   *
   * @return the {@link JobRegistry} bean.
   */
  @Bean
  public JobRegistry jobRegistry() {
    return new MapJobRegistry();
  }

  /**
   * Defines the {@link StepScope} bean.
   *
   * @return the {@link StepScope} bean.
   */
  @Bean
  public static StepScope stepScope() {
    StepScope stepScope = new StepScope();
    stepScope.setAutoProxy(false);
    return stepScope;
  }

  /**
   * Defines the {@link JobScope} bean.
   *
   * @return the {@link JobScope} bean.
   */
  @Bean
  public static JobScope jobScope() {
    JobScope jobScope = new JobScope();
    jobScope.setAutoProxy(false);
    return jobScope ;
  }

}
