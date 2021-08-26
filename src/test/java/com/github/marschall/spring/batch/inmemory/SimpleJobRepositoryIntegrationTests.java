package com.github.marschall.spring.batch.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;

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
  void createAndFind() throws JobExecutionException {
    JobParameters jobParams = new JobParametersBuilder()
            .addString("stringKey", "stringValue")
            .addLong("longKey", 1L)
            .addDouble("doubleKey", 1.1)
            .addDate("dateKey", new Date(1L))
            .toJobParameters();

    JobExecution firstExecution = this.jobRepository.createJobExecution(JOB_NAME, jobParams);
    firstExecution.setStartTime(new Date());
    assertNotNull(firstExecution.getLastUpdated());

    assertEquals(JOB_NAME, firstExecution.getJobInstance().getJobName());

    this.jobRepository.update(firstExecution);
    firstExecution.setEndTime(new Date());
    this.jobRepository.update(firstExecution);
    JobExecution secondExecution = this.jobRepository.createJobExecution(JOB_NAME, jobParams);

    assertEquals(firstExecution.getJobInstance(), secondExecution.getJobInstance());
    assertEquals(JOB_NAME, secondExecution.getJobInstance().getJobName());
  }

  /**
   * Create two job executions for same job+parameters tuple. Check both
   * executions belong to the same job instance and job.
   */
  @Test
  void createAndFindWithNoStartDate() throws JobExecutionException {
    JobExecution firstExecution = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters);
    firstExecution.setStartTime(new Date(0));
    firstExecution.setEndTime(new Date(1));
    this.jobRepository.update(firstExecution);
    JobExecution secondExecution = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters);

    assertEquals(firstExecution.getJobInstance(), secondExecution.getJobInstance());
    assertEquals(JOB_NAME, secondExecution.getJobInstance().getJobName());
  }

  /**
   * Save multiple StepExecutions for the same step and check the returned
   * count and last execution are correct.
   */
  @Test
  void getStepExecutionCountAndLastStepExecution() throws JobExecutionException {
    // first execution
    JobExecution firstJobExec = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters);
    StepExecution firstStepExec = new StepExecution(JOB_NAME, firstJobExec);
    this.jobRepository.add(firstStepExec);

    assertEquals(1, this.jobRepository.getStepExecutionCount(firstJobExec.getJobInstance(), JOB_NAME));
    assertEquals(firstStepExec, this.jobRepository.getLastStepExecution(firstJobExec.getJobInstance(), JOB_NAME));

    // first execution failed
    firstJobExec.setStartTime(new Date(4));
    firstStepExec.setStartTime(new Date(5));
    firstStepExec.setStatus(BatchStatus.FAILED);
    firstStepExec.setEndTime(new Date(6));
    this.jobRepository.update(firstStepExec);
    firstJobExec.setStatus(BatchStatus.FAILED);
    firstJobExec.setEndTime(new Date(7));
    this.jobRepository.update(firstJobExec);

    // second execution
    JobExecution secondJobExec = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters);
    StepExecution secondStepExec = new StepExecution(JOB_NAME, secondJobExec);
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
    JobExecution jobExec = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters);
    jobExec.setStartTime(new Date(0));
    jobExec.setExecutionContext(ctx);
    StepExecution stepExec = new StepExecution(JOB_NAME, jobExec);
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
    JobExecution jobExecution = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters);

    //simulating a running job execution
    jobExecution.setStartTime(new Date());
    this.jobRepository.update(jobExecution);

    assertThrows(JobExecutionAlreadyRunningException.class, () -> this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters));
  }

  @Test
  void getLastJobExecution() throws JobExecutionException, InterruptedException {
    JobExecution jobExecution = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters);
    jobExecution.setStatus(BatchStatus.FAILED);
    jobExecution.setEndTime(new Date());
    this.jobRepository.update(jobExecution);
    Thread.sleep(10);
    jobExecution = this.jobRepository.createJobExecution(JOB_NAME, this.jobParameters);
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
    JobExecution jobExecution1 = this.jobRepository.createJobExecution(JOB_NAME, jobParameters);
    jobExecution1.setStatus(BatchStatus.COMPLETED);
    jobExecution1.setEndTime(new Date());
    this.jobRepository.update(jobExecution1);
    JobExecution jobExecution2 = this.jobRepository.createJobExecution(JOB_NAME, jobParameters);
    assertNotNull(jobExecution1);
    assertNotNull(jobExecution2);
  }

}