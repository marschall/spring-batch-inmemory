package com.github.marschall.spring.batch.inmemory;


import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.SimpleBatchConfiguration;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@SpringBatchTest
//@ContextConfiguration(classes = TestConfiguration.class)
class InMemoryBatchConfigurerTests {

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Autowired
  private JobRepositoryTestUtils jobRepositoryTestUtils;

  @Test
  void test() {
    fail("Not yet implemented");
  }

  @Configuration
  @EnableBatchProcessing
  @Import({TestJobConfiguration.class, SimpleBatchConfiguration.class})
  static class ContextConfiguration {

    @Bean
    BatchConfigurer batchConfigurer() {
      return new InMemoryBatchConfigurer();
    }

  }

}
