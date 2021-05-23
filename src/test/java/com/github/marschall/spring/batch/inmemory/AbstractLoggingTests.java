package com.github.marschall.spring.batch.inmemory;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.junit.MatcherAssert.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;

abstract class AbstractLoggingTests {

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
    assertThat(jobExecutions, hasSize(4));
  }

}
