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
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobExecutionException;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;

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
    JobParameters jobParameters = new JobParameters();
    this.jobInstance = this.jobRepository.createJobInstance("jobName", jobParameters);
    this.jobExecution = this.jobRepository.createJobExecution(this.jobInstance, jobParameters, new ExecutionContext());
  }

  @Test
  void getJobExecution() throws JobExecutionException {
    StepExecution stepExecution = this.jobRepository.createStepExecution("step1", this.jobExecution);

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
    StepExecution stepExecution = this.jobRepository.createStepExecution("foo", this.jobExecution);

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
    StepExecution stepExecution = this.jobRepository.createStepExecution("step", this.jobExecution);
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
  void findRunningJobExecutionsNull() {
    Set<JobExecution> runningJobExecutions = this.jobExplorer.findRunningJobExecutions(null);
    assertThat(runningJobExecutions, empty());
  }

  @Test
  void findJobExecutions() {
    StepExecution stepExecution = this.jobRepository.createStepExecution("step", this.jobExecution);

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

    String jobName = this.jobInstance.getJobName();
    assertEquals(this.jobInstance, this.jobExplorer.getJobInstance(jobName, new JobParameters()));
    JobParameters differentJobParameters = new JobParametersBuilder()
                                                      .addString("key", "value", true)
                                                      .toJobParameters();
    assertNull(this.jobExplorer.getJobInstance(jobName, differentJobParameters));
  }

  @Test
  void getJobInstances() {
    String jobName = this.jobInstance.getJobName();
    assertEquals(List.of(this.jobInstance), this.jobExplorer.getJobInstances(jobName, 0, 2));

    String namePattern = jobName.substring(0, jobName.length() - 1) + "*";
    assertEquals(List.of(), this.jobExplorer.getJobInstances(namePattern, 0, 2));
  }

  @Test
  void findJobInstancesByJobName() {
    String jobName = this.jobInstance.getJobName();
    assertEquals(List.of(this.jobInstance), this.jobExplorer.findJobInstancesByJobName(jobName, 0, 2));

    // prefix match
    String namePattern = jobName.substring(0, jobName.length() - 1) + "*";
    List<JobInstance> jobInstances = this.jobExplorer.findJobInstancesByJobName(namePattern, 0, 2);
    assertEquals(List.of(this.jobInstance), jobInstances);

    // suffix match
    namePattern = "*" + jobName.substring(1);
    jobInstances = this.jobExplorer.findJobInstancesByJobName(namePattern, 0, 2);
    assertEquals(List.of(this.jobInstance), jobInstances);
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
  void getJobInstanceCountNull() throws NoSuchJobException {
    assertThrows(NoSuchJobException.class, () -> this.jobExplorer.getJobInstanceCount(null));
  }

  @Test
  void getJobInstanceCountException() {
    assertThrows(NoSuchJobException.class, () -> this.jobExplorer.getJobInstanceCount("throwException"));
  }

}
