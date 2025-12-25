package com.github.marschall.spring.batch.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobExecutionException;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;

class SimpleJobRepositoryIntegrationTests {

  private static final String STEP_NAME = "step1";

  private JobRepository jobRepository;

  private static final String JOB_NAME = "SimpleJobRepositoryIntegrationTestsJob";

  private final JobParameters jobParameters = new JobParameters();

  @BeforeEach
  void setUp() {
    this.jobRepository = new InMemoryJobRepository(new InMemoryJobStorage());
  }

  /**
   * Create two job executions for same job+parameters tuple. Check both
   * executions belong to the same job instance and job.
   */
  @Test
  void createAndFind() {
    JobParameters jobParams = new JobParametersBuilder()
            .addString("stringKey", "stringValue")
            .addLong("longKey", 1L)
            .addDouble("doubleKey", 1.1)
            .addDate("dateKey", new Date(1L))
            .toJobParameters();

    var jobInstance = this.jobRepository.createJobInstance(JOB_NAME, jobParams);
    JobExecution firstExecution = this.jobRepository.createJobExecution(jobInstance, jobParams, new ExecutionContext());
    firstExecution.setStartTime(LocalDateTime.now());
    firstExecution.setStatus(BatchStatus.STOPPED);
    assertNotNull(firstExecution.getLastUpdated());

    assertEquals(JOB_NAME, firstExecution.getJobInstance().getJobName());

    this.jobRepository.update(firstExecution);
    firstExecution.setEndTime(LocalDateTime.now());
    this.jobRepository.update(firstExecution);
    JobExecution secondExecution = this.jobRepository.createJobExecution(jobInstance, jobParams, new ExecutionContext());

    assertEquals(firstExecution.getJobInstance(), secondExecution.getJobInstance());
    assertEquals(JOB_NAME, secondExecution.getJobInstance().getJobName());
  }

  /**
   * Create two job executions for same job+parameters tuple. Check both
   * executions belong to the same job instance and job.
   */
  @Test
  void createAndFindWithNoStartDate() throws JobExecutionException {

    var jobInstance = this.jobRepository.createJobInstance(JOB_NAME, this.jobParameters);
    JobExecution firstExecution = this.jobRepository.createJobExecution(jobInstance, this.jobParameters, new ExecutionContext());
    firstExecution.setStartTime(this.ofEpochMillis(0));
    firstExecution.setEndTime(this.ofEpochMillis(1));
    firstExecution.setStatus(BatchStatus.COMPLETED);
    this.jobRepository.update(firstExecution);
    JobExecution secondExecution = this.jobRepository.createJobExecution(jobInstance, this.jobParameters, new ExecutionContext());

    assertEquals(firstExecution.getJobInstance(), secondExecution.getJobInstance());
    assertEquals(JOB_NAME, secondExecution.getJobInstance().getJobName());
  }

  private LocalDateTime ofEpochMillis(long millis) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneOffset.UTC);
  }

  /**
   * Save multiple StepExecutions for the same step and check the returned
   * count and last execution are correct.
   */
  @Test
  void getStepExecutionCountAndLastStepExecution() throws JobExecutionException {
    // first execution
    JobExecution firstJobExec = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters, new ExecutionContext());
    StepExecution firstStepExec = new StepExecution(1L, JOB_NAME, firstJobExec);
    this.jobRepository.add(firstStepExec);

    assertEquals(1, this.jobRepository.getStepExecutionCount(firstJobExec.getJobInstance(), JOB_NAME));
    assertEquals(firstStepExec, this.jobRepository.getLastStepExecution(firstJobExec.getJobInstance(), JOB_NAME));

    // first execution failed
    firstJobExec.setStartTime(this.ofEpochMillis(4));
    firstStepExec.setStartTime(this.ofEpochMillis(5));
    firstStepExec.setStatus(BatchStatus.FAILED);
    firstStepExec.setEndTime(this.ofEpochMillis(6));
    this.jobRepository.update(firstStepExec);
    firstJobExec.setStatus(BatchStatus.FAILED);
    firstJobExec.setEndTime(this.ofEpochMillis(7));
    this.jobRepository.update(firstJobExec);

    // second execution
    JobExecution secondJobExec = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters, new ExecutionContext());
    StepExecution secondStepExec = new StepExecution(1L, JOB_NAME, secondJobExec);
    this.jobRepository.add(secondStepExec);

    assertEquals(2, this.jobRepository.getStepExecutionCount(secondJobExec.getJobInstance(), JOB_NAME));
    assertEquals(secondStepExec, this.jobRepository.getLastStepExecution(secondJobExec.getJobInstance(), JOB_NAME));
  }

  /**
   * Save execution context and retrieve it.
   */
  @Test
  void testSaveExecutionContext() throws JobExecutionException {
    ExecutionContext ctx = new ExecutionContext(Map.of("crashedPosition", 7));
    JobExecution jobExec = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters, new ExecutionContext());
    jobExec.setStartTime(this.ofEpochMillis(0));
    jobExec.setExecutionContext(ctx);
    StepExecution stepExec = new StepExecution(1L, JOB_NAME, jobExec);
    stepExec.setExecutionContext(ctx);

    this.jobRepository.add(stepExec);

    StepExecution retrievedStepExec = this.jobRepository.getLastStepExecution(jobExec.getJobInstance(), JOB_NAME);
    assertEquals(stepExec, retrievedStepExec);
    assertEquals(ctx, retrievedStepExec.getExecutionContext());

    // job execution would have to be updated
//    JobExecution retrievedJobExec = this.jobRepository.getLastJobExecution(JOB_NAME, jobExec.getJobParameters());
//    assertEquals(jobExec, retrievedJobExec);
//    assertEquals(ctx, retrievedJobExec.getExecutionContext());
  }

  /**
   * If JobExecution is already running, exception will be thrown in attempt
   * to create new execution.
   */
  @Test
  void onlyOneJobExecutionAllowedRunning() throws JobExecutionException {
    JobExecution jobExecution = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters, new ExecutionContext());

    //simulating a running job execution
    jobExecution.setStartTime(LocalDateTime.now());
    this.jobRepository.update(jobExecution);

    assertThrows(JobExecutionAlreadyRunningException.class, () -> this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters, new ExecutionContext()));
  }

  @Test
  void getLastJobExecution() throws JobExecutionException, InterruptedException {
    JobExecution jobExecution = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters, new ExecutionContext());
    jobExecution.setStatus(BatchStatus.FAILED);
    jobExecution.setEndTime(LocalDateTime.now());
    this.jobRepository.update(jobExecution);
    Thread.sleep(10);
    jobExecution = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters, new ExecutionContext());
    StepExecution stepExecution = new StepExecution(STEP_NAME, jobExecution);
    this.jobRepository.add(stepExecution);
    jobExecution.addStepExecutions(Arrays.asList(stepExecution));
    assertEquals(jobExecution, this.jobRepository.getLastJobExecution(JOB_NAME, this.jobParameters));
    assertEquals(stepExecution, jobExecution.getStepExecutions().iterator().next());
  }

  /**
   * Create two job executions for the same job+parameters tuple. Should ignore
   * non-identifying job parameters when identifying the job instance.
   */
  @Test
  void reExecuteWithSameJobParameters() throws JobExecutionException {
    JobParameters jobParameters = new JobParametersBuilder()
            .addString("name", "foo", false)
            .toJobParameters();
    JobExecution jobExecution1 = this.jobRepository.createJobExecution(JOB_NAME, jobParameters, new ExecutionContext());
    jobExecution1.setStatus(BatchStatus.COMPLETED);
    jobExecution1.setEndTime(LocalDateTime.now());
    this.jobRepository.update(jobExecution1);
    JobExecution jobExecution2 = this.jobRepository.createJobExecution(JOB_NAME, jobParameters, new ExecutionContext());
    assertNotNull(jobExecution1);
    assertNotNull(jobExecution2);
  }

}