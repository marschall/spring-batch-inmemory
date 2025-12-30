package com.github.marschall.spring.batch.inmemory;

import java.lang.invoke.MethodHandles;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.step.NoSuchStepException;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;

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
  public JobExecution createJobExecution(JobInstance jobInstance, JobParameters jobParameters, ExecutionContext executionContext) {
    Objects.requireNonNull(jobInstance, "jobInstance");
    Objects.requireNonNull(jobParameters, "jobParameters");
    return this.storage.createJobExecution(jobInstance, jobParameters, executionContext);
  }
  
  @Override
  public StepExecution createStepExecution(String stepName, JobExecution jobExecution) {
    Objects.requireNonNull(stepName, "stepName");
    Objects.requireNonNull(jobExecution, "jobExecution");
    return this.storage.createStepExecution(stepName, jobExecution);
  }

  @Override
  public void update(JobExecution jobExecution) {
    Objects.requireNonNull(jobExecution, "jobExecution");
    this.storage.update(jobExecution);
  }

  private static void validateStepExecution(StepExecution stepExecution) {
    Objects.requireNonNull(stepExecution, "StepExecution cannot be null.");
    Objects.requireNonNull(stepExecution.getStepName(), "StepExecution's step name cannot be null.");
  }

  @Override
  public void update(StepExecution stepExecution) {
    validateStepExecution(stepExecution);

    stepExecution.setLastUpdated(LocalDateTime.now());
    this.storage.updateStepExecution(stepExecution);
    this.checkForInterruption(stepExecution);
  }

  @Override
  public void updateExecutionContext(StepExecution stepExecution) {
    validateStepExecution(stepExecution);
    this.storage.updateStepExecutionContext(stepExecution);
  }

  @Override
  public void updateExecutionContext(JobExecution jobExecution) {
    this.storage.updateJobExecutionContext(jobExecution);
  }

  @Override
  public @Nullable StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
    return this.storage.getLastStepExecution(jobInstance, stepName);
  }

  @Override
  public long getStepExecutionCount(JobInstance jobInstance, String stepName) throws NoSuchStepException {
    return this.storage.countStepExecutions(jobInstance, stepName);
  }

  @Override
  public @Nullable JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
    return this.storage.getLastJobExecution(jobName, jobParameters);
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

  @Override
  public List<String> getJobNames() {
    return this.storage.getJobNames();
  }

  @Override
  public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
    return this.getJobInstances(jobName, start, count);
  }
  
  @Override
  public List<JobInstance> findJobInstancesByJobName(String jobName, int start, int count) {
    return this.getJobInstances(jobName, start, count);
  }
  
  @Override
  public List<JobInstance> getJobInstances(String jobName, int start, int count) {
    Objects.requireNonNull(jobName, "jobName");
    if (start < 0) {
      throw new IllegalArgumentException("start: " + start + " must be positive");
    }
    if (count < 0) {
      throw new IllegalArgumentException("count: " + start + " must be positive");
    }
    if (count == 0) {
      return List.of();
    }
    return this.storage.findJobInstancesByJobName(jobName, start, count);
  }

  @Override
  public List<JobInstance> findJobInstances(String jobName) {
    return this.storage.findJobInstancesByJobName(jobName);
  }

  @Override
  public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
    return this.storage.findJobExecutions(jobInstance);
  }

  @Override
  public @Nullable JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
    return this.storage.getJobInstance(jobName, jobParameters);
  }

  @Override
  public @Nullable JobInstance getJobInstance(long jobInstanceId) {
    return this.storage.getJobInstance(jobInstanceId);
  }

  @Override
  public @Nullable JobInstance getLastJobInstance(String jobName) {
    return this.storage.getLastJobInstance(jobName);
  }

  @Override
  public long getJobInstanceCount(String jobName) throws NoSuchJobException {
    return this.storage.getJobInstanceCount(jobName);
  }

  @Override
  public void deleteStepExecution(StepExecution stepExecution) {
    this.storage.deleteStepExecution(stepExecution);
  }

  @Override
  public void deleteJobExecution(JobExecution jobExecution) {
    this.storage.deleteJobExecution(jobExecution);
  }

  @Override
  public void deleteJobInstance(JobInstance jobInstance) {
    this.storage.deleteJobExecution(jobInstance);
  }

  @Override
  public @Nullable JobExecution getJobExecution(long executionId) {
    return this.storage.getJobExecution(executionId);
  }

  @Override
  public List<JobExecution> getJobExecutions(JobInstance jobInstance) {
    // TODO hydration?
    return this.storage.getJobExecutions(jobInstance);
  }

  @Override
  public @Nullable JobExecution getLastJobExecution(JobInstance jobInstance) {
    return this.storage.getLastJobExecution(jobInstance);
  }

  @Override
  public Set<JobExecution> findRunningJobExecutions(String jobName) {
    return this.storage.findRunningJobExecutions(jobName);
  }

  @Override
  public @Nullable StepExecution getStepExecution(long jobExecutionId, long stepExecutionId) {
    return this.storage.getStepExecution(jobExecutionId, stepExecutionId);
  }

  @Override
  public @Nullable StepExecution getStepExecution(long stepExecutionId) {
    // TODO hydration?
    return this.storage.getStepExecution(stepExecutionId);
  }

}
