package com.github.marschall.spring.batch.inmemory;

import static com.github.marschall.spring.batch.inmemory.ClearPoint.AFTER_TEST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
import org.springframework.batch.test.JobOperatorTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;

import com.github.marschall.spring.batch.inmemory.configuration.LoggingJobConfiguration;

@SpringBatchTest
@ClearJobRepository(AFTER_TEST)
@TestMethodOrder(OrderAnnotation.class)
class ClearJobRepositoryTests {

  @Autowired
  private JobOperatorTestUtils jobOperatorTestUtils;

  @Autowired
  private ApplicationContext applicationContext;

  private JobParameters jobParameters;

  @BeforeEach
  void setUp() {
    this.jobParameters = new JobParameters();
    Job job = this.applicationContext.getBean("loggingJob", Job.class);
    this.jobOperatorTestUtils.setJob(job);
  }

  private String getJobName() {
    return this.jobOperatorTestUtils.getJob().getName();
  }

  private boolean isJobInstanceExists() {
    return this.jobOperatorTestUtils.getJobRepository().getJobInstance(this.getJobName(), this.jobParameters) != null;
  }

  private Object launchJob() throws Exception {
    // return type is Object because of https://github.com/spring-projects/spring-batch/issues/3976
    return this.jobOperatorTestUtils.startJob(this.jobParameters);
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
    public PlatformTransactionManager transactionManager() {
      return new ResourcelessTransactionManager();
    }

  }

}
