package com.github.marschall.spring.batch.inmemory;


import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.SimpleBatchConfiguration;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.github.marschall.spring.batch.inmemory.configuration.LoggingJobConfiguration;

@SpringBatchTest
class InMemoryBatchConfigurerTests extends AbstractLoggingTests {

  @Configuration
  @EnableBatchProcessing
  @Import({LoggingJobConfiguration.class, SimpleBatchConfiguration.class})
  static class ContextConfiguration {

    @Bean
    BatchConfigurer batchConfigurer() {
      return new InMemoryBatchConfigurer();
    }

    @Bean
    public DataSource dataSource() {
      return new NullDataSource();
    }

  }

}
