package com.github.marschall.spring.batch.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.infrastructure.item.ExecutionContext;

class NullJobRepositoryTests {

  private JobRepository jobRepository;
  private JobParameters jobParameters;
  private JobInstance jobInstance;
  private JobExecution jobExecution;

  @BeforeEach
  void setUp() {
    this.jobRepository = new NullJobRepository();

    this.jobParameters = new JobParametersBuilder().addString("bar", "test").toJobParameters();

    this.jobInstance = this.jobRepository.createJobInstance("RepositoryTest", this.jobParameters);
    this.jobExecution = this.jobRepository.createJobExecution(this.jobInstance, this.jobParameters, new ExecutionContext());
  }

  @Test
  void getJobNames() {
    List<String> jobNames = this.jobRepository.getJobNames();
    assertNotNull(jobNames, "jobNames");
    assertEquals(List.of(), jobNames);
  }

  /**
   * Save execution context clears the dirty flag.
   */
  @Test
  void updateRestesDirtyFlag() {
    jobExecution.setStartTime(LocalDateTime.now());
    var executionContext = new ExecutionContext();
    executionContext.put("crashedPosition", 7);
    jobExecution.setExecutionContext(executionContext);
    assertTrue(executionContext.isDirty(), "job execution context is dirty");
    jobRepository.updateExecutionContext(jobExecution);
    assertFalse(executionContext.isDirty(), "job execution context is dirty");

    var stepExecution = jobRepository.createStepExecution("setpName", jobExecution);
    executionContext = new ExecutionContext(executionContext);
    executionContext.put("crashedPosition", 8);
    stepExecution.setExecutionContext(executionContext);
    assertTrue(executionContext.isDirty(), "step execution context is dirty");
    jobRepository.updateExecutionContext(stepExecution);
    assertFalse(executionContext.isDirty(), "step execution context is dirty");
  }

}
