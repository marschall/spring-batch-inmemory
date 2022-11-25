package com.github.marschall.spring.batch.inmemory;

import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;

import com.github.marschall.spring.batch.inmemory.configuration.LoggingJobConfiguration;

class InMemoryBatchConfigurationTests extends AbstractLoggingTests {

  @Configuration
  @Import({
    LoggingJobConfiguration.class,
    InMemoryBatchConfiguration.class
  })
  static class ContextConfiguration {

    @Bean
    public PlatformTransactionManager transactionManager() {
      return new ResourcelessTransactionManager();
    }

  }

}
