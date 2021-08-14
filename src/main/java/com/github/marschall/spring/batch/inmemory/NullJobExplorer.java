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
 * Null implementation of {@link JobExplorer}
 */
public final class NullJobExplorer implements JobExplorer {

  @Override
  public List<JobInstance> getJobInstances(String jobName, int start, int count) {
    Objects.requireNonNull(jobName, "jobName");
    if (start < 0) {
      throw new IllegalArgumentException("start: " + start + " must be positive");
    }
    if (count < 0) {
      throw new IllegalArgumentException("count: " + start + " must be positive");
    }
    return List.of();
  }

  @Nullable
  @Override
  public JobInstance getLastJobInstance(String jobName) {
    Objects.requireNonNull(jobName, "jobName");
    return null;
  }

  @Nullable
  @Override
  public JobExecution getJobExecution(Long executionId) {
    return null;
  }

  @Nullable
  @Override
  public StepExecution getStepExecution(Long jobExecutionId, Long stepExecutionId) {
    return null;
  }

  @Nullable
  @Override
  public JobInstance getJobInstance(Long instanceId) {
    return null;
  }

  @Override
  public List<JobExecution> getJobExecutions(JobInstance jobInstance) {
    return List.of();
  }

  @Nullable
  @Override
  public JobExecution getLastJobExecution(JobInstance jobInstance) {
    return null;
  }

  @Override
  public Set<JobExecution> findRunningJobExecutions(String jobName) {
    return Set.of();
  }

  @Override
  public List<String> getJobNames() {
    return List.of();
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
    return List.of();
  }

  @Override
  public int getJobInstanceCount(String jobName) throws NoSuchJobException {
    if (jobName == null) {
      throw new NoSuchJobException("No job instances for job name null were found");
    }
    return 0;
  }

}
