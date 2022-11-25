package com.github.marschall.spring.batch.inmemory;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.junit.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobRepository;

class InMemoryJobExplorerTests {

  private JobExplorer jobExplorer;

  private JobInstance jobInstance;

  private JobExecution jobExecution;

  private JobRepository jobRepository;

  @BeforeEach
  void setUp() throws JobExecutionException {
    InMemoryJobStorage storage = new InMemoryJobStorage();
    this.jobExplorer = new InMemoryJobExplorer(storage);
    this.jobRepository = new InMemoryJobRepository(storage);
    this.jobExecution = this.jobRepository.createJobExecution("jobName", new JobParameters());
    this.jobInstance = this.jobExecution.getJobInstance();
  }

  @Test
  void getJobExecution() throws JobExecutionException {
    StepExecution stepExecution = new StepExecution("step1", this.jobExecution);
    this.jobRepository.add(stepExecution);

    JobExecution readBackJobExecution = this.jobExplorer.getJobExecution(this.jobExecution.getId());
    assertNotNull(readBackJobExecution);
    assertEquals(this.jobExecution, readBackJobExecution);

    Collection<StepExecution> readBackStepExecutions = readBackJobExecution.getStepExecutions();
    assertThat(readBackStepExecutions, hasSize(1));
    StepExecution readBackStepExecution = readBackStepExecutions.iterator().next();
    assertEquals(stepExecution, readBackStepExecution);
  }

  @Test
  void getLastJobExecution() throws JobExecutionException {
    JobExecution lastJobExecution = this.jobExplorer.getLastJobExecution(this.jobExecution.getJobInstance());
    assertEquals(this.jobExecution, lastJobExecution);
  }

  @Test
  void missingGetJobExecution() throws JobExecutionException {
    assertNull(this.jobExplorer.getJobExecution(123L));
  }

  @Test
  void getStepExecution() {
    StepExecution stepExecution = this.jobExecution.createStepExecution("foo");
    this.jobRepository.add(stepExecution);
    assertNotNull(stepExecution.getId());

    stepExecution = this.jobExplorer.getStepExecution(this.jobExecution.getId(), stepExecution.getId());

    assertEquals(this.jobInstance, stepExecution.getJobExecution().getJobInstance());
  }

  @Test
  void getStepExecutionMissing() {
    assertNull(this.jobExplorer.getStepExecution(this.jobExecution.getId(), 123L));
  }

  @Test
  void getStepExecutionMissingJobExecution() {
    assertNull(this.jobExplorer.getStepExecution(-1L, 123L));
  }

  @Test
  void findRunningJobExecutionsNotExisting() {
    Set<JobExecution> runningJobExecutions = this.jobExplorer.findRunningJobExecutions("notExisting");
    assertThat(runningJobExecutions, empty());
  }

  @Test
  void findRunningJobExecutions() {
    StepExecution stepExecution = this.jobExecution.createStepExecution("step");
    this.jobRepository.add(stepExecution);
    // switch status to running
    this.jobExecution.setStartTime(LocalDateTime.now());
    this.jobRepository.update(this.jobExecution);

    Set<JobExecution> runningJobExecutions = this.jobExplorer.findRunningJobExecutions(this.jobExecution.getJobInstance().getJobName());
    assertThat(runningJobExecutions, hasSize(1));
    JobExecution readBackJobExecutions = runningJobExecutions.iterator().next();
    assertEquals(this.jobExecution, readBackJobExecutions);

    Collection<StepExecution> readBackStepExecutions = readBackJobExecutions.getStepExecutions();
    assertThat(readBackStepExecutions, hasSize(1));
    StepExecution readBackStepExecution = readBackStepExecutions.iterator().next();
    assertEquals(stepExecution, readBackStepExecution);
  }

  @Test
  void findJobExecutions() {
    StepExecution stepExecution = this.jobExecution.createStepExecution("step");
    this.jobRepository.add(stepExecution);

    List<JobExecution> readBackJobExeuctions = this.jobExplorer.getJobExecutions(this.jobInstance);
    assertThat(readBackJobExeuctions, hasSize(1));
    JobExecution readBackJobExecutions = readBackJobExeuctions.iterator().next();
    assertEquals(this.jobExecution, readBackJobExecutions);

    Collection<StepExecution> readBackStepExecutions = readBackJobExecutions.getStepExecutions();
    assertThat(readBackStepExecutions, hasSize(1));
    StepExecution readBackStepExecution = readBackStepExecutions.iterator().next();
    assertEquals(stepExecution, readBackStepExecution);
  }

  @Test
  void getJobInstance() {
    assertEquals(this.jobInstance, this.jobExplorer.getJobInstance(this.jobInstance.getId()));
  }

  @Test
  void getLastJobInstances() {
    assertEquals(List.of(this.jobInstance), this.jobExplorer.getJobInstances(this.jobInstance.getJobName(), 0, 1));
  }

  @Test
  void getLastJobInstance() {
    JobInstance lastJobInstance = this.jobExplorer.getLastJobInstance(this.jobInstance.getJobName());
    assertEquals(this.jobInstance, lastJobInstance);
  }

  @Test
  void getJobNames() {
    assertEquals(List.of(this.jobInstance.getJobName()), this.jobExplorer.getJobNames());
  }

  @Test
  void getJobInstanceCount() throws NoSuchJobException {
    String jobName = "myJob";
    this.jobRepository.createJobInstance(jobName, new JobParametersBuilder().addLong("key", 1L).toJobParameters());
    this.jobRepository.createJobInstance(jobName, new JobParametersBuilder().addLong("key", 2L).toJobParameters());
    this.jobRepository.createJobInstance(jobName, new JobParametersBuilder().addLong("key", 3L).toJobParameters());
    this.jobRepository.createJobInstance(jobName, new JobParametersBuilder().addLong("key", 4L).toJobParameters());

    assertEquals(4, this.jobExplorer.getJobInstanceCount(jobName));
  }

  @Test
  void getJobInstanceCountException() {
    assertThrows(NoSuchJobException.class, () -> this.jobExplorer.getJobInstanceCount("throwException"));
  }

}
