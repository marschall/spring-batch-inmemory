package com.github.marschall.spring.batch.inmemory;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.util.ArrayList;
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
  void getJobNames() throws JobExecutionException {
    String jobName = this.jobInstance.getJobName();
    List<String> jobNames = this.jobRepository.getJobNames();
    assertEquals(List.of(jobName), jobNames);

    String newJobName = jobName + "new";
    this.jobRepository.createJobExecution(newJobName, new JobParameters());
    jobNames = this.jobRepository.getJobNames();
    assertEquals(List.of(jobName, newJobName), jobNames);
  }

  @Test
  void findJobInstancesByName() {
    String jobName = this.jobInstance.getJobName();

    // prefix match
    String namePattern = jobName.substring(0, jobName.length() - 1) + "*";
    List<JobInstance> jobInstances = this.jobRepository.findJobInstancesByName(namePattern, 0, 2);
    assertEquals(List.of(this.jobInstance), jobInstances);

    // suffix match
    namePattern = "*" + jobName.substring(1);
    jobInstances = this.jobRepository.findJobInstancesByName(namePattern, 0, 2);
    assertEquals(List.of(this.jobInstance), jobInstances);
  }

  @Test
  void findJobExecutions() {
    List<JobExecution> jobExecutions = this.jobRepository.findJobExecutions(this.jobInstance);
    assertEquals(List.of(this.jobExecution), jobExecutions);
  }

  @Test
  void getJobInstance() {
    String jobName = this.jobInstance.getJobName();
    assertEquals(this.jobInstance, this.jobRepository.getJobInstance(jobName, this.jobParameters));
    JobParameters differentJobParameters = new JobParametersBuilder(this.jobParameters)
                                                      .addString("key", "value", true)
                                                      .toJobParameters();
    assertNull(this.jobRepository.getJobInstance(jobName, differentJobParameters));
  }

  @Test
  void saveOrUpdateInvalidJobExecution() {

    // failure scenario - must have job ID
    JobExecution jobExecution = new JobExecution((JobInstance) null, (JobParameters) null);
    assertThrows(NullPointerException.class, () -> this.jobRepository.update(jobExecution));
  }

  @Test
  void updateValidJobExecution() throws JobExecutionException, InterruptedException {

    LocalDateTime before = this.jobExecution.getLastUpdated();
    Thread.sleep(2L);
    this.jobRepository.update(this.jobExecution);
    LocalDateTime after = LocalDateTime.now();

    LocalDateTime lastUpdated = this.jobExecution.getLastUpdated();

    assertNotNull(lastUpdated);
    assertThat(lastUpdated, greaterThan(before));
    // TODO 2sec
    assertThat(lastUpdated, lessThanOrEqualTo(after));
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

    LocalDateTime before = LocalDateTime.now();
    this.jobRepository.add(stepExecution);
    LocalDateTime after = LocalDateTime.now();

    LocalDateTime lastUpdated = stepExecution.getLastUpdated();

    assertNotNull(lastUpdated);
    assertThat(lastUpdated, greaterThanOrEqualTo(before));
    assertThat(lastUpdated, lessThanOrEqualTo(after));
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

    LocalDateTime before = LocalDateTime.now();
    this.jobRepository.update(stepExecution);
    LocalDateTime after = LocalDateTime.now();

    LocalDateTime lastUpdated = stepExecution.getLastUpdated();

    assertNotNull(lastUpdated);
    assertThat(lastUpdated, greaterThanOrEqualTo(before));
    assertThat(lastUpdated, lessThanOrEqualTo(after));
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
  void iisJobInstanceExistsFalse() {
    assertFalse(this.jobRepository.isJobInstanceExists("foo", new JobParameters()));
  }

  @Test
  void isJobInstanceExistsTrue() {
    assertTrue(this.jobRepository.isJobInstanceExists(this.jobInstance.getJobName(), this.jobParameters));
  }

  @Test
  void createJobExecutionAlreadyRunning() {
    this.jobExecution.setStatus(BatchStatus.STARTED);
    this.jobExecution.setStartTime(LocalDateTime.now());
    this.jobExecution.setEndTime(null);

    this.jobRepository.update(this.jobExecution);

    assertThrows(JobExecutionAlreadyRunningException.class, () -> this.jobRepository.createJobExecution(this.jobInstance.getJobName(), this.jobParameters));
  }

  @Test
  void createJobExecutionStatusUnknown() {
    this.jobExecution.setStatus(BatchStatus.UNKNOWN);
    this.jobExecution.setEndTime(LocalDateTime.now());

    this.jobRepository.update(this.jobExecution);

    assertThrows(JobRestartException.class, () -> this.jobRepository.createJobExecution(this.jobInstance.getJobName(), this.jobParameters));
  }

  @Test
  void createJobExecutionAlreadyComplete() {
    this.jobExecution.setStatus(BatchStatus.COMPLETED);
    this.jobExecution.setEndTime(LocalDateTime.now());

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
    long expectedResult = 1L;
    String stepName = "stepName";
    StepExecution stepExecution = new StepExecution(stepName, this.jobExecution);
    this.jobRepository.add(stepExecution);

    // When
    long actualResult = this.jobRepository.getStepExecutionCount(this.jobInstance, stepName);

    // Then
    assertEquals(expectedResult, actualResult);
  }
  
  @Test
  void delete() {
    String stepName = "stepName";
    StepExecution stepExecution = new StepExecution(stepName, this.jobExecution);
    this.jobRepository.add(stepExecution);

    this.jobRepository.deleteStepExecution(stepExecution);
    // FIXME
//    assertEquals(0L, this.jobRepository.getStepExecutionCount(this.jobInstance, stepName));

    this.jobRepository.deleteJobExecution(this.jobExecution);
    assertEquals(List.of(), this.jobRepository.findJobExecutions(this.jobInstance));
//    this.jobRepository.getLastJobExecution(stepName, jobParameters)

    this.jobRepository.deleteJobInstance(this.jobInstance);
    assertNull(this.jobRepository.getJobInstance(this.jobInstance.getJobName(), this.jobParameters));
    // FIXME
//    assertEquals(0L, this.jobRepository.getStepExecutionCount(this.jobInstance, stepName));
  }

}
