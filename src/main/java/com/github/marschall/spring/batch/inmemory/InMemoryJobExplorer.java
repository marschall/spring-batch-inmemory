package com.github.marschall.spring.batch.inmemory;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.NoSuchJobException;

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

  @Override
  public StepExecution getStepExecution(Long jobExecutionId, Long stepExecutionId) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<JobExecution> getJobExecutions(JobInstance jobInstance) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<JobExecution> findRunningJobExecutions(String jobName) {
    // TODO Auto-generated method stub
    return null;
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

}
