package com.github.marschall.spring.batch.inmemory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.dao.OptimisticLockingFailureException;

public final class InMemoryJobStorage {

  private final Map<Long, JobInstance> instancesById;
  private final Map<String, List<JobInstanceAndParameters>> instancesByName;
  private final Map<Long, JobExecution> jobExecutionsById;
  private final Map<Long, ExecutionContext> jobExecutionContextsById;

  private final ReentrantReadWriteLock instanceLock;
  private long nextJobInstanceId;
  private long nextJobExecutionId;

  public InMemoryJobStorage() {
    this.instancesById = new HashMap<>();
    this.instancesByName = new HashMap<>();
    this.jobExecutionsById = new HashMap<>();
    this.jobExecutionContextsById = new HashMap<>();
    this.instanceLock = new ReentrantReadWriteLock();
    this.nextJobInstanceId = 1L;
    this.nextJobExecutionId = 1L;
  }

  JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");

    WriteLock writeLock = this.instanceLock.writeLock();
    try {
      List<JobInstanceAndParameters> instancesAndParameters = this.instancesByName.get(jobName);
      if (instancesAndParameters != null) {
        for (JobInstanceAndParameters instanceAndParametes : instancesAndParameters) {
          if (instanceAndParametes.getJobParameters().equals(jobParameters)) {
            throw new IllegalStateException("JobInstance must not already exist");
          }
        }
      }

      JobInstance jobInstance = new JobInstance(this.nextJobInstanceId++, jobName);
      jobInstance.incrementVersion();

      this.instancesById.put(jobInstance.getId(), jobInstance);
      if (instancesAndParameters == null) {
        instancesAndParameters = new ArrayList<>();
        this.instancesByName.put(jobName, instancesAndParameters);
      }
      instancesAndParameters.add(new JobInstanceAndParameters(jobInstance, jobParameters));

      return jobInstance;
    } finally {
      writeLock.unlock();
    }
  }

  JobExecution createJobExecution(JobInstance jobInstance, JobParameters jobParameters, String jobConfigurationLocation) {

    Long jobExecutionId = this.nextJobExecutionId++;
    JobExecution jobExecution = new JobExecution(jobInstance, jobExecutionId, jobParameters, jobConfigurationLocation);
    ExecutionContext executionContext = new ExecutionContext();
    jobExecution.setExecutionContext(executionContext);
    jobExecution.setLastUpdated(new Date());
    jobExecution.incrementVersion();

    WriteLock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {
      this.jobExecutionsById.put(jobExecutionId, copyJobExecution(jobExecution));
      this.jobExecutionContextsById.put(jobExecutionId, copyExecutionContext(executionContext));
    } finally {
      writeLock.unlock();
    }

    return jobExecution;
  }

  void update(JobExecution jobExecution) {

    jobExecution.setLastUpdated(new Date());
    this.synchronizeStatus(jobExecution);
    Long id = jobExecution.getId();

    WriteLock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {
      JobExecution persisted = this.jobExecutionsById.get(id);
      if (persisted == null) {
        throw new IllegalArgumentException("JobExecution must already be saved");
      }

      // copy and pasted from MapJobExecutionDao
      // unclear when synchronized is needed
      synchronized (jobExecution) {
        if (!persisted.getVersion().equals(jobExecution.getVersion())) {
          throw new OptimisticLockingFailureException("Attempt to update job execution id=" + id
              + " with wrong version (" + jobExecution.getVersion() + "), where current version is "
              + persisted.getVersion());
        }
        jobExecution.incrementVersion();
        this.jobExecutionsById.put(id, copyJobExecution(jobExecution));
      }
    } finally {
      writeLock.unlock();
    }
  }

  private void synchronizeStatus(JobExecution jobExecution) {
    ReadLock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      JobExecution presisted = this.jobExecutionsById.get(jobExecution.getId());
      // copy and pasted from MapJobExecutionDao
      // not clear of synchronized is needed
      if (presisted.getVersion().intValue() != jobExecution.getVersion().intValue()) {
        jobExecution.upgradeStatus(presisted.getStatus());
        jobExecution.setVersion(presisted.getVersion());
      }

    } finally {
      readLock.unlock();
    }
  }

  JobInstance getJobInstance(Long instanceId) {
    Objects.requireNonNull(instanceId, "instanceId");
    ReadLock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      return this.instancesById.get(instanceId);
    } finally {
      readLock.unlock();
    }
  }

  boolean isJobInstanceExists(String jobName, JobParameters jobParameters) {
    ReadLock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      List<JobInstanceAndParameters> instancesAndParameters = this.instancesByName.get(jobName);
      if(instancesAndParameters != null) {
        for (JobInstanceAndParameters instanceAndParametes : instancesAndParameters) {
          if (instanceAndParametes.getJobParameters().equals(jobParameters)) {
            return true;
          }
        }
      }
      return false;
    } finally {
      readLock.unlock();
    }
  }

  List<JobInstance> getJobInstances(String jobName, int start, int count) {
    Objects.requireNonNull(jobName, "jobName");
    if (start < 0) {
      throw new IllegalArgumentException("start is negative");
    }
    // TODO Auto-generated method stub
    return null;
  }

  List<String> getJobNames() {
    List<String> jobNames = new ArrayList<>();
    ReadLock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      jobNames = new ArrayList<>(this.instancesByName.keySet());
    } finally {
      readLock.unlock();
    }
    jobNames.sort(null);
    return jobNames;
  }

  int getJobInstanceCount(String jobName) throws NoSuchJobException {
    ReadLock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      List<JobInstanceAndParameters> instancesAndParameters = this.instancesByName.get(jobName);
      if(instancesAndParameters == null) {
        throw new NoSuchJobException("No job instances for job name " + jobName + " were found");
      } else {
        return instancesAndParameters.size();
      }
    } finally {
      readLock.unlock();
    }
  }

  private static ExecutionContext copyExecutionContext(ExecutionContext original) {
    return new ExecutionContext(original);
  }

  private static JobExecution copyJobExecution(JobExecution original) {
    return new JobExecution(original);
  }

  static final class JobInstanceAndParameters {

    private final JobInstance jobInstance;

    private final JobParameters jobParameters;

    JobInstanceAndParameters(JobInstance jobInstance, JobParameters jobParameters) {
      this.jobInstance = jobInstance;
      this.jobParameters = jobParameters;
    }

    JobInstance getJobInstance() {
      return this.jobInstance;
    }

    JobParameters getJobParameters() {
      return this.jobParameters;
    }

  }

}
