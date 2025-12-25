package com.github.marschall.spring.batch.inmemory;

import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.DefaultBatchConfiguration;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.configuration.support.ScopeConfiguration;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * An alternative to {@link DefaultBatchConfiguration} that sets up Spring Batch
 * with a null {@link JobRepository} and {@link JobExplorer} and a default
 * {@link JobLauncher} and {@link JobRegistry}.
 */
@Configuration
@Import(ScopeConfiguration.class)
public class NullBatchConfiguration {

  /**
   * Defines the {@link NullJobRepository} bean which will be a {@link InMemoryJobRepository}.
   *
   * @return the {@link JobRepository} bean.
   */
  @Bean
  public JobRepository jobRepository() {
    return new NullJobRepository();
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
    return new NullJobExplorer();
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

}
