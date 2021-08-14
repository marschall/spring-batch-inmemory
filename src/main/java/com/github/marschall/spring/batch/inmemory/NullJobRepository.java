package com.github.marschall.spring.batch.inmemory;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.lang.Nullable;

/**
 * Null implementation of {@link JobExplorer}
 */
public final class NullJobRepository implements JobRepository {

  private static final Log LOGGER = LogFactory.getLog(MethodHandles.lookup().lookupClass());

  private static final AtomicLong JOB_INSTANCE_ID = new AtomicLong();

  private static final AtomicLong JOB_EXECUTION_ID = new AtomicLong();

  private static final AtomicLong STEP_EXECUTION_ID = new AtomicLong();

  @Override
  public boolean isJobInstanceExists(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");
    return false;
  }

  @Override
  public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");

    JobInstance jobInstance = new JobInstance(JOB_INSTANCE_ID.incrementAndGet(), jobName);
    jobInstance.incrementVersion();

    return jobInstance;
  }

  @Override
  public JobExecution createJobExecution(JobInstance jobInstance, JobParameters jobParameters, String jobConfigurationLocation) {
    Objects.requireNonNull(jobInstance, "jobInstance");
    Objects.requireNonNull(jobParameters, "jobParameters");

    JobExecution jobExecution = new JobExecution(jobInstance, JOB_EXECUTION_ID.incrementAndGet(), jobParameters, jobConfigurationLocation);
    ExecutionContext executionContext = new ExecutionContext();
    jobExecution.setExecutionContext(executionContext);
    jobExecution.setLastUpdated(new Date());
    jobExecution.incrementVersion();

    return jobExecution;
  }

  @Override
  public JobExecution createJobExecution(String jobName, JobParameters jobParameters) {

    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");

    JobInstance jobInstance = new JobInstance(JOB_INSTANCE_ID.incrementAndGet(), jobName);
    jobInstance.incrementVersion();

    JobExecution jobExecution = new JobExecution(jobInstance, JOB_EXECUTION_ID.incrementAndGet(), jobParameters, null);
    jobExecution.incrementVersion();
    jobExecution.setExecutionContext(new ExecutionContext());
    jobExecution.setLastUpdated(new Date());

    return jobExecution;
  }

  @Override
  public void update(JobExecution jobExecution) {
    Objects.requireNonNull(jobExecution, "jobExecution");
    Objects.requireNonNull(jobExecution.getJobId(), "jobExecution.getJobId()");
    Objects.requireNonNull(jobExecution.getId(), "jobExecution.getId()");

    jobExecution.setLastUpdated(new Date());
    jobExecution.incrementVersion();
  }

  @Override
  public void add(StepExecution stepExecution) {
    validateStepExecution(stepExecution);

    stepExecution.setId(STEP_EXECUTION_ID.incrementAndGet());
    stepExecution.setLastUpdated(new Date());
    stepExecution.incrementVersion();
    this.checkForInterruption(stepExecution);
  }

  @Override
  public void addAll(Collection<StepExecution> stepExecutions) {
    for (StepExecution stepExecution : stepExecutions) { // implicit null check
      this.add(stepExecution);
    }
  }

  @Override
  public void update(StepExecution stepExecution) {
    validateStepExecution(stepExecution);
    Objects.requireNonNull(stepExecution.getId(), "StepExecution must already be saved (have an id assigned)");

    stepExecution.setLastUpdated(new Date());
    stepExecution.incrementVersion();
    this.checkForInterruption(stepExecution);
  }

  @Override
  public void updateExecutionContext(StepExecution stepExecution) {
    validateStepExecution(stepExecution);
    // nothing else

  }

  @Override
  public void updateExecutionContext(JobExecution jobExecution) {
    // nothing
  }

  @Nullable
  @Override
  public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
    return null;
  }

  @Override
  public int getStepExecutionCount(JobInstance jobInstance, String stepName) {
    return 0;
  }

  @Nullable
  @Override
  public JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
    return null;
  }

  private static void validateStepExecution(StepExecution stepExecution) {
    Objects.requireNonNull(stepExecution, "StepExecution cannot be null.");
    Objects.requireNonNull(stepExecution.getStepName(), "StepExecution's step name cannot be null.");
    Objects.requireNonNull(stepExecution.getJobExecutionId(), "StepExecution must belong to persisted JobExecution");
  }

  /**
   * Check to determine whether or not the JobExecution that is the parent of
   * the provided StepExecution has been interrupted. If, after synchronizing
   * the status with the database, the status has been updated to STOPPING,
   * then the job has been interrupted.
   *
   * @param stepExecution
   */
  private void checkForInterruption(StepExecution stepExecution) {
    JobExecution jobExecution = stepExecution.getJobExecution();
    if (jobExecution.isStopping()) {
      LOGGER.info("Parent JobExecution " + jobExecution.getId() + " is stopped, so passing message on to StepExecution " + stepExecution.getId());
      stepExecution.setTerminateOnly();
    }
  }

}
