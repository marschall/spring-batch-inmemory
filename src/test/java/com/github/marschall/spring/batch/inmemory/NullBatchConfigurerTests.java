package com.github.marschall.spring.batch.inmemory;


import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.SimpleBatchConfiguration;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;

import com.github.marschall.spring.batch.inmemory.configuration.LoggingJobConfiguration;
import com.github.marschall.spring.batch.nulldatasource.NullDataSource;

class NullBatchConfigurerTests extends AbstractLoggingTests {

  @Configuration
  @EnableBatchProcessing
  @Import({
    LoggingJobConfiguration.class,
    SimpleBatchConfiguration.class
  })
  static class ContextConfiguration {

    @Bean
    BatchConfigurer batchConfigurer() {
      return new NullBatchConfigurer(this.txManager());
    }

    @Bean
    public DataSource dataSource() {
      return new NullDataSource();
    }

    @Bean
    public PlatformTransactionManager txManager() {
      return new ResourcelessTransactionManager();
    }

  }

}
