package com.github.marschall.spring.batch.inmemory;

import static com.github.marschall.spring.batch.inmemory.ClearPoint.AFTER_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.github.marschall.spring.batch.inmemory.configuration.LoggingJobConfiguration;

@SpringBatchTest
@ClearJobRepository(AFTER_TEST)
@TestMethodOrder(OrderAnnotation.class)
class ClearJobRepositoryTests {

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  private JobParameters jobParameters;

  @BeforeEach
  void setUp() {
    this.jobParameters = new JobParameters();
  }

  private String getJobName() {
    return this.jobLauncherTestUtils.getJob().getName();
  }

  private boolean isJobInstanceExists() {
    return this.jobLauncherTestUtils.getJobRepository().isJobInstanceExists(this.getJobName(), this.jobParameters);
  }

  private Object launchJob() throws Exception {
    // return type is Object because of https://github.com/spring-projects/spring-batch/issues/3976
    return this.jobLauncherTestUtils.launchJob(this.jobParameters);
  }

  @Test
  @Order(1)
  void firstMethod() throws Exception {
    JobExecution jobExecution = (JobExecution) this.launchJob();
    assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
    assertTrue(this.isJobInstanceExists());
  }

  @Test
  @Order(2)
  void secondMethod() throws Exception {
    assertFalse(this.isJobInstanceExists());
  }

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
