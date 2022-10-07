package com.github.marschall.spring.batch.inmemory;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@Rollback
@SpringBatchTest
@TestMethodOrder(OrderAnnotation.class)
abstract class AbstractJdbcTests {

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Autowired
  private JobRepositoryTestUtils jobRepositoryTestUtils;

  @Autowired
  private JdbcOperations jdbcOperations;
  
  @Autowired
  private ApplicationContext applicationContext;

  @Test
  @Order(1)
  void launchJob() throws Exception {
    Job job = this.applicationContext.getBean("insertingJob", Job.class);
    this.jobLauncherTestUtils.setJob(job);
    JobExecution jobExecution = this.jobLauncherTestUtils.launchJob();
    assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
  }

  @Test
  @Order(2)
  void createJobExecutions() throws Exception {
    List<JobExecution> jobExecutions = this.jobRepositoryTestUtils.createJobExecutions(4);
    assertThat(jobExecutions, hasSize(4));
  }

  @Test
  @Order(3) // run after the jobs
  void insertsAreRolledBack() {
    assertEquals(0, this.countRows(), "rows present at start of test");
  }

  private int countRows() {
    return this.jdbcOperations.queryForObject("SELECT COUNT(*) FROM BATCH_TEST", Integer.class);
  }

}
