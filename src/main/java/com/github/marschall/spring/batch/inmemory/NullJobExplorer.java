package com.github.marschall.spring.batch.inmemory;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.step.NoSuchStepException;
import org.springframework.batch.core.step.StepExecution;
import org.jspecify.annotations.Nullable;

/**
 * Null implementation of {@link JobExplorer}
 */
@SuppressWarnings("removal")
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

  @Override
  public @Nullable JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");
    return null;
  }
  
  @Nullable
  @Override
  public JobExecution getJobExecution(long executionId) {
    return null;
  }
  
  @Nullable
  @Override
  public StepExecution getStepExecution(long jobExecutionId, long stepExecutionId) {
    return null;
  }

  @Override
  public @Nullable StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
    Objects.requireNonNull(jobInstance, "jobInstance");
    Objects.requireNonNull(stepName, "stepName");
    return null;
  }

  @Override
  public long getStepExecutionCount(JobInstance jobInstance, String stepName) throws NoSuchStepException {
    Objects.requireNonNull(jobInstance, "jobInstance");
    Objects.requireNonNull(stepName, "stepName");
    throw new NoSuchStepException(stepName);
  }
  
  @Nullable
  @Override
  public JobInstance getJobInstance(long instanceId) {
    return null;
  }

  @Nullable
  @Override
  public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");
    return null;
  }

  @Override
  public boolean isJobInstanceExists(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");
    return false;
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
  public long getJobInstanceCount(String jobName) throws NoSuchJobException {
    if (jobName == null) {
      throw new NoSuchJobException("No job instances for job name null were found");
    }
    return 0L;
  }

  @Override
  public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
    Objects.requireNonNull(jobName, "jobName");
    return List.of();
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
  public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
    Objects.requireNonNull(jobInstance, "jobInstance");
    return List.of();
  }

  @Override
  public List<String> getJobNames() {
    return List.of();
  }

}
