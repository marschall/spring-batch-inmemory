package com.github.marschall.spring.batch.inmemory;

import static org.junit.Assert.fail;

import org.junit.Test;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.SimpleBatchConfiguration;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

//@SpringBatchTest
class InMemoryBatchConfigurerTests {

  @Test
  void test() {
    fail("Not yet implemented");
  }

  @Configuration
  @Import(SimpleBatchConfiguration.class)
  static class TestConfiguration {

    @Bean
    BatchConfigurer batchConfigurer() {
      return new InMemoryBatchConfigurer();
    }

  }

}
