package com.github.marschall.spring.batch.inmemory;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBatchTest
abstract class AbstractLoggingTests {

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Autowired
  private JobRepositoryTestUtils jobRepositoryTestUtils;

  @Test
  void launchJob() throws Exception {
    long start = System.currentTimeMillis();
    JobExecution jobExecution = this.jobLauncherTestUtils.launchJob();
    assertEquals(ExitStatus.COMPLETED, jobExecution.getExitStatus());
    long end = System.currentTimeMillis();

    assertThat(jobExecution.getStartTime(), greaterThanOrEqualTo(new Date(start)));
    assertThat(jobExecution.getStartTime(), lessThanOrEqualTo(new Date(end)));

    assertNotNull(jobExecution.getCreateTime());
    assertThat(jobExecution.getCreateTime(), greaterThanOrEqualTo(new Date(start)));
    assertThat(jobExecution.getCreateTime(), lessThanOrEqualTo(jobExecution.getStartTime()));
    assertThat(jobExecution.getCreateTime(), lessThanOrEqualTo(new Date(end)));
  }

  @Test
  void createJobExecutions() throws Exception {
    List<JobExecution> jobExecutions = this.jobRepositoryTestUtils.createJobExecutions(4);
    assertThat(jobExecutions, hasSize(4));
  }

}
