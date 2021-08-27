package com.github.marschall.spring.batch.inmemory;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;

class InMemoryJobRepositoryTests {

  private JobRepository jobRepository;

  private JobParameters jobParameters;

  private JobInstance jobInstance;

  private JobExecution jobExecution;

  @BeforeEach
  void setUp() throws JobExecutionException {

    this.jobRepository = new InMemoryJobRepository(new InMemoryJobStorage());

    this.jobParameters = new JobParametersBuilder().addString("bar", "test").toJobParameters();

    this.jobExecution = this.jobRepository.createJobExecution("RepositoryTest", this.jobParameters);
    this.jobInstance = this.jobExecution.getJobInstance();
  }

  @Test
  void saveOrUpdateInvalidJobExecution() {

    // failure scenario - must have job ID
    JobExecution jobExecution = new JobExecution((JobInstance) null, (JobParameters) null);
    assertThrows(NullPointerException.class, () -> this.jobRepository.update(jobExecution));
  }

  @Test
  void updateValidJobExecution() throws JobExecutionException, InterruptedException {

    Date before = this.jobExecution.getLastUpdated();
    Thread.sleep(2L);
    this.jobRepository.update(this.jobExecution);
    Date after = this.jobExecution.getLastUpdated();
    assertNotNull(after);
    assertThat(after, greaterThan(before));
  }

  @Test
  void saveOrUpdateStepExecutionException() {

    StepExecution stepExecution = new StepExecution("stepName", null);

    // failure scenario -- no step id set.
    assertThrows(NullPointerException.class, () -> this.jobRepository.add(stepExecution));
  }

  @Test
  void saveStepExecutionSetsLastUpdated(){

    StepExecution stepExecution = new StepExecution("stepName", this.jobExecution);

    long before = System.currentTimeMillis();

    this.jobRepository.add(stepExecution);

    assertNotNull(stepExecution.getLastUpdated());

    long lastUpdated = stepExecution.getLastUpdated().getTime();
    assertTrue(lastUpdated > (before - 1000));
  }

  @Test
  void saveStepExecutions() {
    List<StepExecution> stepExecutions = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      StepExecution stepExecution = new StepExecution("stepName" + i, this.jobExecution);
      stepExecutions.add(stepExecution);
    }

    this.jobRepository.addAll(stepExecutions);
    assertEquals(1, this.jobRepository.getStepExecutionCount(this.jobInstance, "stepName" + 0));
    assertEquals(1, this.jobRepository.getStepExecutionCount(this.jobInstance, "stepName" + 1));
    assertEquals(1, this.jobRepository.getStepExecutionCount(this.jobInstance, "stepName" + 2));
  }

  @Test
  void saveNullStepExecutions() {
    assertThrows(NullPointerException.class, () -> this.jobRepository.addAll(null));
  }

  @Test
  void updateStepExecutionSetsLastUpdated(){

    StepExecution stepExecution = new StepExecution("stepName", this.jobExecution);
    this.jobRepository.add(stepExecution);

    long before = System.currentTimeMillis();

    this.jobRepository.update(stepExecution);

    assertNotNull(stepExecution.getLastUpdated());

    long lastUpdated = stepExecution.getLastUpdated().getTime();
    assertTrue(lastUpdated > (before - 1000));
  }

  @Test
  void interrupted(){

    this.jobExecution.setStatus(BatchStatus.STOPPING);
    StepExecution stepExecution = new StepExecution("stepName", this.jobExecution);
    this.jobRepository.add(stepExecution);

    this.jobRepository.update(stepExecution);
    assertTrue(stepExecution.isTerminateOnly());
  }

  @Test
  void isJobInstanceFalse() {
    assertFalse(this.jobRepository.isJobInstanceExists("foo", new JobParameters()));
  }

  @Test
  void isJobInstanceTrue() {
    assertTrue(this.jobRepository.isJobInstanceExists(this.jobInstance.getJobName(), this.jobParameters));
  }

  @Test
  void createJobExecutionAlreadyRunning() {
    this.jobExecution.setStatus(BatchStatus.STARTED);
    this.jobExecution.setStartTime(new Date());
    this.jobExecution.setEndTime(null);

    this.jobRepository.update(this.jobExecution);

    assertThrows(JobExecutionAlreadyRunningException.class, () -> this.jobRepository.createJobExecution(this.jobInstance.getJobName(), this.jobParameters));
  }

  @Test
  void createJobExecutionStatusUnknown() {
    this.jobExecution.setStatus(BatchStatus.UNKNOWN);
    this.jobExecution.setEndTime(new Date());

    this.jobRepository.update(this.jobExecution);

    assertThrows(JobRestartException.class, () -> this.jobRepository.createJobExecution(this.jobInstance.getJobName(), this.jobParameters));
  }

  @Test
  void createJobExecutionAlreadyComplete() {
    this.jobExecution.setStatus(BatchStatus.COMPLETED);
    this.jobExecution.setEndTime(new Date());

    this.jobRepository.update(this.jobExecution);

    assertThrows(JobInstanceAlreadyCompleteException.class, () -> this.jobRepository.createJobExecution(this.jobInstance.getJobName(), this.jobParameters));
  }

  @Test
  void createJobExecutionInstanceWithoutExecutions() {
    String jobName = this.jobInstance.getJobName() + "1";
    this.jobRepository.createJobInstance(jobName, this.jobParameters);

    assertThrows(IllegalStateException.class, () -> this.jobRepository.createJobExecution(jobName, this.jobParameters));
  }

  @Test
  void getStepExecutionCount() {
    // Given
    int expectedResult = 1;
    String stepName = "stepName";
    StepExecution stepExecution = new StepExecution(stepName, this.jobExecution);
    this.jobRepository.add(stepExecution);

    // When
    int actualResult = this.jobRepository.getStepExecutionCount(this.jobInstance, stepName);

    // Then
    assertEquals(expectedResult, actualResult);
  }

}
