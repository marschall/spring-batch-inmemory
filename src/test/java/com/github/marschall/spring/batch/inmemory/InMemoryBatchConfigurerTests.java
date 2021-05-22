package com.github.marschall.spring.batch.inmemory;


import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
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

import com.github.marschall.spring.batch.inmemory.configuration.LoggingJobConfiguration;

@SpringBatchTest
class InMemoryBatchConfigurerTests {

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Autowired
  private JobRepositoryTestUtils jobRepositoryTestUtils;

  @Test
  void launchJob() throws Exception {
    this.jobLauncherTestUtils.launchJob();
  }

  @Test
  void createJobExecutions() throws Exception {
    List<JobExecution> jobExecutions = this.jobRepositoryTestUtils.createJobExecutions(4);
  }

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
