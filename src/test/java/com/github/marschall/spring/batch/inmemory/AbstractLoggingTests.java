package com.github.marschall.spring.batch.inmemory;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

@SpringBatchTest
abstract class AbstractLoggingTests {

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Autowired
  private JobRepositoryTestUtils jobRepositoryTestUtils;

  @Autowired
  private ApplicationContext applicationContext;

  @Test
  void launchJob() throws Exception {
    LocalDateTime start = LocalDateTime.now();
    Job job = this.applicationContext.getBean("loggingJob", Job.class);
    this.jobLauncherTestUtils.setJob(job);
    JobExecution jobExecution = this.jobLauncherTestUtils.launchJob();
    assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
    LocalDateTime end = LocalDateTime.now();

    assertThat(jobExecution.getStartTime(), greaterThanOrEqualTo(start));
    assertThat(jobExecution.getStartTime(), lessThanOrEqualTo(end));

    assertNotNull(jobExecution.getCreateTime());
    assertThat(jobExecution.getCreateTime(), greaterThanOrEqualTo(start));
    assertThat(jobExecution.getCreateTime(), lessThanOrEqualTo(jobExecution.getStartTime()));
    assertThat(jobExecution.getCreateTime(), lessThanOrEqualTo(end));
  }

  @Test
  void createJobExecutions() throws Exception {
    List<JobExecution> jobExecutions = this.jobRepositoryTestUtils.createJobExecutions(4);
    assertThat(jobExecutions, hasSize(4));
  }

}
