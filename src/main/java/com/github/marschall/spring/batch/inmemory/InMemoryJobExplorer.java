package com.github.marschall.spring.batch.inmemory;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.lang.Nullable;

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

  @Nullable
  @Override
  public JobInstance getJobInstance(@Nullable Long jobInstanceId) {
    if (jobInstanceId == null) {
      return null;
    }
    return this.storage.getJobInstance(jobInstanceId);
  }

  @Nullable
  @Override
  public JobExecution getJobExecution(@Nullable Long executionId) {
    if (executionId == null) {
      return null;
    }
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
  public StepExecution getStepExecution(@Nullable Long jobExecutionId, @Nullable Long stepExecutionId) {
    if ((jobExecutionId == null) || (stepExecutionId == null)) {
      return null;
    }

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
  public long getJobInstanceCount(@Nullable String jobName) throws NoSuchJobException {
    if (jobName == null) {
      throw new NoSuchJobException("No job instances for job name null were found");
    }
    return this.storage.getJobInstanceCount(jobName);
  }

}
