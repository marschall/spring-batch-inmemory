package com.github.marschall.spring.batch.inmemory;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.step.NoSuchStepException;
import org.springframework.batch.core.step.StepExecution;

/**
 * In-memory implementation of {@link JobExplorer} based on {@link InMemoryJobStorage}.
 */
public final class InMemoryJobExplorer implements JobExplorer {

  private final InMemoryJobStorage storage;

  /**
   * Constructs a new {@link InMemoryJobExplorer}.
   *
   * @param storage the storage to use, not {@code null}
   */
  public InMemoryJobExplorer(InMemoryJobStorage storage) {
    Objects.requireNonNull(storage, "storage");
    this.storage = storage;
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
    return this.storage.getJobInstances(jobName, start, count);
  }

  @Override
  public List<JobInstance> findJobInstancesByJobName(String jobName, int start, int count) {
    return this.getJobInstances(jobName, start, count);
  }



  @Override
  public long getJobInstanceCount(@Nullable String jobName) throws NoSuchJobException {
    if (jobName == null) {
      throw new NoSuchJobException("No job instances for job name null were found");
    }
    return this.storage.getJobInstanceCount(jobName);
  }

  @Override
  public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
    return this.getJobInstances(jobName, start, count);
  }

  @Nullable
  @Override
  public JobInstance getJobInstance(long jobInstanceId) {
    return this.storage.getJobInstance(jobInstanceId);
  }

  @Nullable
  @Override
  public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
    return this.storage.getJobInstance(jobName, jobParameters);
  }

  @Nullable
  @Override
  public JobExecution getJobExecution(long executionId) {
    return this.storage.getJobExecution(executionId);
  }

  @Override
  public JobExecution getLastJobExecution(JobInstance jobInstance) {
    Objects.requireNonNull(jobInstance, "jobInstance");
    return this.storage.getLastJobExecution(jobInstance);
  }

  @Nullable
  @Override
  public JobInstance getLastJobInstance(String jobName) {
    Objects.requireNonNull(jobName, "jobName");
    return this.storage.getLastJobInstance(jobName);
  }

  @Nullable
  @Override
  public StepExecution getStepExecution(long jobExecutionId, long stepExecutionId) {
    return this.storage.getStepExecution(jobExecutionId, stepExecutionId);
  }

  @Override
  public List<JobExecution> getJobExecutions(JobInstance jobInstance) {
    return this.storage.findJobExecutions(jobInstance);
  }

  @Override
  public Set<JobExecution> findRunningJobExecutions(@Nullable String jobName) {
    if (jobName == null) {
      return Set.of();
    }
    return this.storage.findRunningJobExecutions(jobName);
  }

  @Override
  public List<String> getJobNames() {
    return this.storage.getJobNames();
  }

  @Override
  public boolean isJobInstanceExists(String jobName, JobParameters jobParameters) {
    return this.storage.isJobInstanceExists(jobName, jobParameters);
  }

  @Override
  public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
    return this.getJobExecutions(jobInstance);
  }

  @Override
  public @Nullable JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
    return this.storage.getLastJobExecution(jobName, jobParameters);
  }

  @Override
  public @Nullable StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
    return this.storage.getLastStepExecution(jobInstance, stepName);
  }

  @Override
  public long getStepExecutionCount(JobInstance jobInstance, String stepName) throws NoSuchStepException {
    return this.storage.countStepExecutions(jobInstance, stepName);
  }

}
