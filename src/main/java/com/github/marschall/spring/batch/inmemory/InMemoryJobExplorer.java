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

public final class InMemoryJobExplorer implements JobExplorer {

  private final InMemoryJobStorage storage;

  public InMemoryJobExplorer(InMemoryJobStorage storage) {
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

  @Override
  public JobInstance getJobInstance(Long instanceId) {
    Objects.requireNonNull(instanceId, "instanceId");
    return this.storage.getJobInstance(instanceId);
  }

  @Override
  public JobExecution getJobExecution(Long executionId) {
    Objects.requireNonNull(executionId, "executionId");
    return this.storage.getJobExecution(executionId);
  }

  @Override
  public JobExecution getLastJobExecution(JobInstance jobInstance) {
    Objects.requireNonNull(jobInstance, "jobInstance");
    return this.storage.getLastJobExecution(jobInstance);
  }

  @Override
  public JobInstance getLastJobInstance(String jobName) {
    Objects.requireNonNull(jobName, "jobName");
    return this.storage.getLastJobInstance(jobName);
  }

  @Nullable
  @Override
  public StepExecution getStepExecution(@Nullable Long jobExecutionId, @Nullable Long stepExecutionId) {
    if (jobExecutionId == null || stepExecutionId == null) {
      return null;
    }
    
    JobExecution jobExecution = this.storage.getJobExecution(jobExecutionId);
    if (jobExecution == null) {
      return null;
    }
    setJobExecutionDependencies(jobExecution);
    StepExecution stepExecution = this.storage.getStepExecution(jobExecutionId, stepExecutionId);
    if (stepExecution == null) {
      return null;
    }
    setStepExecutionDependencies(stepExecution);
    return stepExecution;
  }

  @Override
  public List<JobExecution> getJobExecutions(JobInstance jobInstance) {
    List<JobExecution> executions = this.storage.findJobExecutions(jobInstance);
    for (JobExecution jobExecution : executions) {
      setJobExecutionDependencies(jobExecution);
      for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
        setStepExecutionDependencies(stepExecution);
      }
    }
    return executions;
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
  public int getJobInstanceCount(String jobName) throws NoSuchJobException {
    Objects.requireNonNull(jobName, "jobName");
    return this.storage.getJobInstanceCount(jobName);
  }

  /*
   * Find all dependencies for a JobExecution, including JobInstance (which
   * requires JobParameters) plus StepExecutions
   */
  private void setJobExecutionDependencies(JobExecution jobExecution) {
    List<StepExecution> stepExecutions = this.storage.getStepExecutions(jobExecution);
    jobExecution.addStepExecutions(stepExecutions);
    jobExecution.setExecutionContext(this.storage.getExecutionContext(jobExecution));
  }

  private void setStepExecutionDependencies(StepExecution stepExecution) {
    stepExecution.setExecutionContext(this.storage.getStepExecutionContext(stepExecution));
  }

}
