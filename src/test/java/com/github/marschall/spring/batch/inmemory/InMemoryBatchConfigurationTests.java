package com.github.marschall.spring.batch.inmemory;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.github.marschall.spring.batch.inmemory.configuration.LoggingJobConfiguration;

class InMemoryBatchConfigurationTests extends AbstractLoggingTests {

  @Configuration
  @Import({
    LoggingJobConfiguration.class,
    InMemoryBatchConfiguration.class
  })
  static class ContextConfiguration {

    @Bean
    public DataSource dataSource() {
      return new NullDataSource();
    }

  }

}
