package com.github.marschall.spring.batch.inmemory;

import java.util.Collection;
import java.util.Objects;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;

public final class InMemoryJobRepository implements JobRepository {

  private final InMemoryJobStorage storage;

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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public JobExecution createJobExecution(String jobName, JobParameters jobParameters)
          throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void update(JobExecution jobExecution) {
    // TODO Auto-generated method stub

  }

  @Override
  public void add(StepExecution stepExecution) {
    // TODO Auto-generated method stub

  }

  @Override
  public void addAll(Collection<StepExecution> stepExecutions) {
    // TODO Auto-generated method stub

  }

  @Override
  public void update(StepExecution stepExecution) {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateExecutionContext(StepExecution stepExecution) {
    // TODO Auto-generated method stub

  }

  @Override
  public void updateExecutionContext(JobExecution jobExecution) {
    // TODO Auto-generated method stub

  }

  @Override
  public StepExecution getLastStepExecution(JobInstance jobInstance,
          String stepName) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getStepExecutionCount(JobInstance jobInstance, String stepName) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
    // TODO Auto-generated method stub
    return null;
  }

}
