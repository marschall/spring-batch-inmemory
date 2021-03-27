package com.github.marschall.spring.batch.inmemory;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Pattern;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.dao.OptimisticLockingFailureException;

public final class InMemoryJobStorage {

  private final Map<Long, JobInstance> instancesById;
  private final Map<String, List<JobInstanceAndParameters>> jobInstancesByName;
  private final Map<Long, JobExecution> jobExecutionsById;
  private final Map<Long, List<Long>> jobInstanceToExecutions;
  private final Map<Long, ExecutionContext> jobExecutionContextsById;

  private final ReentrantReadWriteLock instanceLock;
  private long nextJobInstanceId;
  private long nextJobExecutionId;

  public InMemoryJobStorage() {
    this.instancesById = new HashMap<>();
    this.jobInstancesByName = new HashMap<>();
    this.jobExecutionsById = new HashMap<>();
    this.jobExecutionContextsById = new HashMap<>();
    this.jobInstanceToExecutions = new HashMap<>();
    this.instanceLock = new ReentrantReadWriteLock();
    this.nextJobInstanceId = 1L;
    this.nextJobExecutionId = 1L;
  }

  JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");

    WriteLock writeLock = this.instanceLock.writeLock();
    try {
      List<JobInstanceAndParameters> instancesAndParameters = this.jobInstancesByName.get(jobName);
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
        this.jobInstancesByName.put(jobName, instancesAndParameters);
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
      this.jobInstanceToExecutions.computeIfAbsent(jobInstance.getInstanceId(), id -> new ArrayList<>()).add(jobExecutionId);
    } finally {
      writeLock.unlock();
    }

    return jobExecution;
  }

  JobExecution createJobExecution(String jobName, JobParameters jobParameters)
          throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
    WriteLock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {
      // Find all jobs matching the runtime information.
      JobInstance jobInstance = this.getJobInstanceUnlocked(jobName, jobParameters);


      // existing job instance found
      if (jobInstance != null) {
        List<Long> executionIds = this.jobInstanceToExecutions.get(jobInstance.getInstanceId());
        if (executionIds != null) {
          for (Long executionId : executionIds) {
            JobExecution jobExecution = this.jobExecutionsById.get(executionId);
            if (jobExecution.isRunning() || jobExecution.isStopping()) {
              throw new JobExecutionAlreadyRunningException("A job execution for this job is already running: " + jobInstance);
            }
            BatchStatus status = jobExecution.getStatus();
            if (status == BatchStatus.UNKNOWN) {
              throw new JobRestartException("Cannot restart job from UNKNOWN status. "
                      + "The last execution ended with a failure that could not be rolled back, "
                      + "so it may be dangerous to proceed. Manual intervention is probably necessary.");
            }
            if (!jobExecution.getJobParameters().getParameters().isEmpty() && ((status == BatchStatus.COMPLETED) || (status == BatchStatus.ABANDONED))) {
              throw new JobInstanceAlreadyCompleteException(
                      "A job instance already exists and is complete for parameters=" + jobParameters
                      + ".  If you want to run this job again, change the parameters.");
            }
          }
        }


      } else {
        // no job found, create one

      }
    } finally {
      writeLock.unlock();
    }
  }

  private JobExecution getLastJobExecutionUnlocked(JobInstance jobInstance) {
    JobExecution lastJobExecution = null;
    List<Long> executionIds = this.jobInstanceToExecutions.get(jobInstance.getId());
    if (executionIds != null) {
      for (Long executionId : executionIds) {
        JobExecution jobExecution = this.jobExecutionsById.get(executionId);
        if (lastJobExecution == null) {
          lastJobExecution = jobExecution;
        } else if (lastJobExecution.getCreateTime().before(jobExecution.getCreateTime())) {
          lastJobExecution = jobExecution;
        }
      }
    }
    if (lastJobExecution != null) {
      return copyJobExecution(lastJobExecution);
    } else {
      return null;
    }
  }

  JobExecution getLastJobExecution(JobInstance jobInstance) {
    ReadLock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      return this.getLastJobExecutionUnlocked(jobInstance);
    } finally {
      readLock.unlock();
    }
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

  private JobInstance getJobInstanceUnlocked(String jobName, JobParameters jobParameters) {
    List<JobInstanceAndParameters> instancesAndParameters = this.jobInstancesByName.get(jobName);
    if(instancesAndParameters != null) {
      for (JobInstanceAndParameters instanceAndParametes : instancesAndParameters) {
        if (instanceAndParametes.getJobParameters().equals(jobParameters)) {
          return instanceAndParametes.getJobInstance();
        }
      }
    }
    return null;
  }

  boolean isJobInstanceExists(String jobName, JobParameters jobParameters) {
    ReadLock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      return this.getJobInstanceUnlocked(jobName, jobParameters) != null;
    } finally {
      readLock.unlock();
    }
  }

  JobInstance getLastJobInstance(String jobName) {
    ReadLock readLock = this.instanceLock.readLock();
    readLock.lock();

    try {
      List<JobInstanceAndParameters> jobInstances = this.jobInstancesByName.get(jobName);
      if ((jobInstances == null) || jobInstances.isEmpty()) {
        return null;
      }
      if (jobInstances.size() == 1) {
        return jobInstances.get(0).getJobInstance();
      } else {
        // the last one should have the highest id
        JobInstance jobInstanceWithHighestId = jobInstances.get(0).getJobInstance();
        for (int i = 1; i < jobInstances.size(); i++) {
          JobInstance jobInstance = jobInstances.get(i).getJobInstance();
          if (jobInstance.getInstanceId() > jobInstanceWithHighestId.getInstanceId()) {
            jobInstanceWithHighestId = jobInstance;
          }
        }
        return jobInstanceWithHighestId;
      }
    } finally {
      readLock.unlock();
    }
  }


  JobExecution getJobExecution(Long executionId) {
    // TODO Auto-generated method stub
    return null;
  }

  List<JobInstance> findJobInstancesByJobName(String jobName, int start, int count) {
    return this.getJobInstancesByNamePattern(jobName, start, count);
  }

  List<JobInstance> getJobInstances(String jobName, int start, int count) {

    boolean exactPattern = isExactPattern(jobName);
    if (exactPattern) {
      return this.getJobInstancesByNameExact(jobName, start, count);
    } else {
      return this.getJobInstancesByNamePattern(jobName, start, count);
    }
  }

  List<JobInstance> getJobInstancesByNameExact(String jobName, int start, int count) {
    if ((start == 0) && (count == 1)) {
      JobInstance lastJobInstance = this.getLastJobInstance(jobName);
      if (lastJobInstance != null) {
        return List.of(lastJobInstance);
      } else {
        return List.of();
      }
    }

    ReadLock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      List<JobInstanceAndParameters> jobInstances = this.jobInstancesByName.get(jobName);
      if ((jobInstances == null) || jobInstances.isEmpty()) {
        return List.of();
      }
      return jobInstances.stream()
                         .map(JobInstanceAndParameters::getJobInstance)
                         .sorted(Comparator.comparingLong(JobInstance::getInstanceId))
                         .skip(start)
                         .limit(count)
                         .collect(toList());
    } finally {
      readLock.unlock();
    }
  }



  List<JobInstance> getJobInstancesByNamePattern(String jobName, int start, int count) {
    Pattern pattern = Pattern.compile(jobName.replaceAll("\\*", ".*"));
    List<JobInstance> jobInstancesUnstorted = new ArrayList<>();

    ReadLock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      for (Entry<String, List<JobInstanceAndParameters>> entries : this.jobInstancesByName.entrySet()) {
        if (pattern.matcher(entries.getKey()).matches()) {
          for (JobInstanceAndParameters jobInstanceAndParameters : entries.getValue()) {
            jobInstancesUnstorted.add(jobInstanceAndParameters.getJobInstance());
          }
        }
      }
    } finally {
      readLock.unlock();
    }

    return jobInstancesUnstorted.stream()
                                .sorted(Comparator.comparingLong(JobInstance::getInstanceId))
                                .skip(start)
                                .limit(count)
                                .collect(toList());
  }

  private static boolean isExactPattern(String s) {
    // only * seems to be officially supported
    // MapJobInstanceDao and JdbcJobInstanceDao behave differently for _
    return (s.indexOf('*') == -1) && (s.indexOf('_') == -1);
  }

  List<String> getJobNames() {
    List<String> jobNames = new ArrayList<>();
    ReadLock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      jobNames = new ArrayList<>(this.jobInstancesByName.keySet());
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
      List<JobInstanceAndParameters> instancesAndParameters = this.jobInstancesByName.get(jobName);
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
