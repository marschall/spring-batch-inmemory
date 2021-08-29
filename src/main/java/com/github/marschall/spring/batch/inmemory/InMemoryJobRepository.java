package com.github.marschall.spring.batch.inmemory;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.lang.Nullable;

/**
 * In-memory implementation of {@link JobExplorer} based on {@link JobRepository}.
 */
public final class InMemoryJobRepository implements JobRepository {

  private static final Log LOGGER = LogFactory.getLog(MethodHandles.lookup().lookupClass());

  private final InMemoryJobStorage storage;

  /**
   * Constructs a new {@link InMemoryJobRepository}.
   * 
   * @param storage the storage to use, not {@code null}
   */
  public InMemoryJobRepository(InMemoryJobStorage storage) {
    this.storage = storage;
  }

  @Override
  public boolean isJobInstanceExists(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");
    return this.storage.isJobInstanceExists(jobName, jobParameters);
  }

  @Override
  public JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");
    return this.storage.createJobInstance(jobName, jobParameters);
  }

  @Override
  public JobExecution createJobExecution(JobInstance jobInstance, JobParameters jobParameters, String jobConfigurationLocation) {
    Objects.requireNonNull(jobInstance, "jobInstance");
    Objects.requireNonNull(jobParameters, "jobParameters");
    return this.storage.createJobExecution(jobInstance, jobParameters, jobConfigurationLocation);
  }

  @Override
  public JobExecution createJobExecution(String jobName, JobParameters jobParameters)
          throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");
    return this.storage.createJobExecution(jobName, jobParameters);
  }

  @Override
  public void update(JobExecution jobExecution) {
    Objects.requireNonNull(jobExecution, "jobExecution");
    Objects.requireNonNull(jobExecution.getJobId(), "jobExecution.getJobId()");
    Objects.requireNonNull(jobExecution.getId(), "jobExecution.getId()");
    this.storage.update(jobExecution);
  }

  private static void validateStepExecution(StepExecution stepExecution) {
    Objects.requireNonNull(stepExecution, "StepExecution cannot be null.");
    Objects.requireNonNull(stepExecution.getStepName(), "StepExecution's step name cannot be null.");
    Objects.requireNonNull(stepExecution.getJobExecutionId(), "StepExecution must belong to persisted JobExecution");
  }

  @Override
  public void add(StepExecution stepExecution) {
    validateStepExecution(stepExecution);

    stepExecution.setLastUpdated(new Date());
    this.storage.addStepExecution(stepExecution);
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
    this.storage.updateStepExecution(stepExecution);
    this.checkForInterruption(stepExecution);
  }

  @Override
  public void updateExecutionContext(StepExecution stepExecution) {
    validateStepExecution(stepExecution);
    Objects.requireNonNull(stepExecution.getId(), "StepExecution must already be saved (have an id assigned)");
    this.storage.updateStepExecutionContext(stepExecution);
  }

  @Override
  public void updateExecutionContext(JobExecution jobExecution) {
    this.storage.updateJobExecutionContext(jobExecution);
  }

  @Nullable
  @Override
  public StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
    StepExecution lastExecution = this.storage.getLastStepExecution(jobInstance, stepName);

    if (lastExecution != null) {
      this.storage.setStepExecutionContext(lastExecution);
      this.storage.setJobExecutionContext(lastExecution.getJobExecution());
    }

    return lastExecution;
  }

  @Override
  public int getStepExecutionCount(JobInstance jobInstance, String stepName) {
    return this.storage.countStepExecutions(jobInstance, stepName);
  }

  @Nullable
  @Override
  public JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
    JobInstance jobInstance = this.storage.getJobInstance(jobName, jobParameters);
    if (jobInstance == null) {
      return null;
    }
    JobExecution jobExecution = this.storage.getLastJobExecution(jobInstance);

    if (jobExecution != null) {
      this.storage.setJobExecutionContext(jobExecution);
      List<StepExecution> stepExecutions = this.storage.getStepExecutions(jobExecution);
      jobExecution.addStepExecutions(stepExecutions);
    }
    return jobExecution;
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
    this.storage.synchronizeStatus(jobExecution);
    if (jobExecution.isStopping()) {
      LOGGER.info("Parent JobExecution " + jobExecution.getId() + " is stopped, so passing message on to StepExecution " + stepExecution.getId());
      stepExecution.setTerminateOnly();
    }
  }

}
