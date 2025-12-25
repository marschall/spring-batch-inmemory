package com.github.marschall.spring.batch.inmemory;

import java.lang.invoke.MethodHandles;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.jspecify.annotations.Nullable;

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
  public JobExecution createJobExecution(JobInstance jobInstance, JobParameters jobParameters, ExecutionContext executionContext) {

    Objects.requireNonNull(jobInstance, "jobInstance");
    Objects.requireNonNull(jobParameters, "jobParameters");

    JobExecution jobExecution = new JobExecution(JOB_EXECUTION_ID.incrementAndGet(), jobInstance, jobParameters);
    jobExecution.incrementVersion();
    jobExecution.setLastUpdated(LocalDateTime.now());
    
    jobInstance.addJobExecution(jobExecution);

    return jobExecution;
  }

  @Override
  public void update(JobExecution jobExecution) {
    Objects.requireNonNull(jobExecution, "jobExecution");
    Objects.requireNonNull(jobExecution.getId(), "jobExecution.getId()");

    jobExecution.setLastUpdated(LocalDateTime.now());
    jobExecution.incrementVersion();
  }

  @Override
  public void update(StepExecution stepExecution) {
    validateStepExecution(stepExecution);
    Objects.requireNonNull(stepExecution.getId(), "StepExecution must already be saved (have an id assigned)");

    stepExecution.setLastUpdated(LocalDateTime.now());
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
  public long getStepExecutionCount(JobInstance jobInstance, String stepName) {
    return 0L;
  }

  @Nullable
  @Override
  public JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
    return null;
  }

  @Override
  public List<String> getJobNames() {
    return List.of();
  }

  @Override
  public List<JobInstance> findJobInstancesByName(String jobName, int start, int count) {
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
  public List<JobExecution> findJobExecutions(JobInstance jobInstance) {
    Objects.requireNonNull(jobInstance, "jobInstance");
    return List.of();
  }

  @Nullable
  @Override
  public JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");
    return null;
  }

  @Override
  public void deleteStepExecution(StepExecution stepExecution) {
    Objects.requireNonNull(stepExecution, "stepExecution");
  }

  @Override
  public void deleteJobExecution(JobExecution jobExecution) {
    Objects.requireNonNull(jobExecution, "jobExecution");
  }

  @Override
  public void deleteJobInstance(JobInstance jobInstance) {
    Objects.requireNonNull(jobInstance, "jobInstance");
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

  @Override
  public List<JobInstance> findJobInstancesByJobName(String jobName, int start,
          int count) {
    // TODO Auto-generated method stub
    return JobRepository.super.findJobInstancesByJobName(jobName, start, count);
  }

  @Override
  public List<JobInstance> getJobInstances(String jobName, int start,
          int count) {
    // TODO Auto-generated method stub
    return JobRepository.super.getJobInstances(jobName, start, count);
  }

  @Override
  public List<JobInstance> findJobInstances(String jobName) {
    // TODO Auto-generated method stub
    return JobRepository.super.findJobInstances(jobName);
  }

  @Override
  public @Nullable JobInstance getJobInstance(long jobInstanceId) {
    // TODO Auto-generated method stub
    return JobRepository.super.getJobInstance(jobInstanceId);
  }

  @Override
  public @Nullable JobInstance getLastJobInstance(String jobName) {
    // TODO Auto-generated method stub
    return JobRepository.super.getLastJobInstance(jobName);
  }

  @Override
  public long getJobInstanceCount(String jobName) throws NoSuchJobException {
    // TODO Auto-generated method stub
    return JobRepository.super.getJobInstanceCount(jobName);
  }

  @Override
  public @Nullable JobExecution getJobExecution(long executionId) {
    // TODO Auto-generated method stub
    return JobRepository.super.getJobExecution(executionId);
  }

  @Override
  public List<JobExecution> getJobExecutions(JobInstance jobInstance) {
    // TODO Auto-generated method stub
    return JobRepository.super.getJobExecutions(jobInstance);
  }

  @Override
  public @Nullable JobExecution getLastJobExecution(JobInstance jobInstance) {
    // TODO Auto-generated method stub
    return JobRepository.super.getLastJobExecution(jobInstance);
  }

  @Override
  public Set<JobExecution> findRunningJobExecutions(String jobName) {
    // TODO Auto-generated method stub
    return JobRepository.super.findRunningJobExecutions(jobName);
  }

  @Override
  public @Nullable StepExecution getStepExecution(long jobExecutionId,
          long stepExecutionId) {
    // TODO Auto-generated method stub
    return JobRepository.super.getStepExecution(jobExecutionId, stepExecutionId);
  }

  @Override
  public @Nullable StepExecution getStepExecution(long stepExecutionId) {
    // TODO Auto-generated method stub
    return JobRepository.super.getStepExecution(stepExecutionId);
  }

  @Override
  public StepExecution createStepExecution(String stepName,
          JobExecution jobExecution) {
    // TODO Auto-generated method stub
    return JobRepository.super.createStepExecution(stepName, jobExecution);
  }
  
  // FIXME
  
  

}
