package com.github.marschall.spring.batch.inmemory;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

@Transactional
@Rollback
@SpringBatchTest
abstract class AbstractJdbcTests {
  // TODO order tests to run #insertsAreRolledBack last

  @Autowired
  private JobLauncherTestUtils jobLauncherTestUtils;

  @Autowired
  private JobRepositoryTestUtils jobRepositoryTestUtils;

  @Autowired
  private JdbcOperations jdbcOperations;

  @Test
  void launchJob() throws Exception {
    this.jobLauncherTestUtils.launchJob();
  }

  @Test
  void createJobExecutions() throws Exception {
    List<JobExecution> jobExecutions = this.jobRepositoryTestUtils.createJobExecutions(4);
    assertThat(jobExecutions, hasSize(4));
  }

  @Test
  void insertsAreRolledBack() {
    assertEquals(0, this.countRows(), "rows present at start of test");
  }

  private int countRows() {
    return this.jdbcOperations.queryForObject("SELECT COUNT(*) FROM BATCH_TEST", Integer.class);
  }

}
