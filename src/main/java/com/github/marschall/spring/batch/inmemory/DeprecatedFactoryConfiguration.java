package com.github.marschall.spring.batch.inmemory;

import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Registers the deprecated {@link JobBuilderFactory} and {@link StepBuilderFactory}.
 * <p>
 * Requires a {@link JobRepository}.
 */
@Configuration
@Deprecated(forRemoval = true, since = "2.0.0")
public class DeprecatedFactoryConfiguration {
  
  @Autowired
  private JobRepository jobRepository;

  /**
   * Defines the {@link JobBuilderFactory} bean.
   *
   * @return the {@link JobBuilderFactory} bean.
   */
  @Bean
  public JobBuilderFactory jobBuilders() {
    return new JobBuilderFactory(this.jobRepository);
  }

  /**
   * Defines the {@link StepBuilderFactory} bean.
   *
   * @return the {@link StepBuilderFactory} bean.
   */
  @Bean
  public StepBuilderFactory stepBuilders() {
    return new StepBuilderFactory(this.jobRepository);
  }

}
