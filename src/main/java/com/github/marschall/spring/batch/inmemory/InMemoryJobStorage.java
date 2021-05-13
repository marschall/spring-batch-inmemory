package com.github.marschall.spring.batch.inmemory;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
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
  private final Map<Long, ExecutionContext> stepExecutionContextsById;
  private final Map<Long, Map<Long, StepExecution>> stepExecutionsByJobExecutionId;

  private final ReadWriteLock instanceLock;
  private long nextJobInstanceId;
  private long nextJobExecutionId;
  private long nextStepExecutionId;

  public InMemoryJobStorage() {
    this.instancesById = new HashMap<>();
    this.jobInstancesByName = new HashMap<>();
    this.jobExecutionsById = new HashMap<>();
    this.jobExecutionContextsById = new HashMap<>();
    this.stepExecutionContextsById = new HashMap<>();
    this.jobInstanceToExecutions = new HashMap<>();
    this.stepExecutionsByJobExecutionId = new HashMap<>();
    this.instanceLock = new ReentrantReadWriteLock();
    this.nextJobInstanceId = 1L;
    this.nextJobExecutionId = 1L;
    this.nextStepExecutionId = 1L;
  }

  JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");

    Lock writeLock = this.instanceLock.writeLock();
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

    Long jobExecutionId = this.nextJobExecutionId++;//FIXME
    JobExecution jobExecution = new JobExecution(jobInstance, jobExecutionId, jobParameters, jobConfigurationLocation);
    ExecutionContext executionContext = new ExecutionContext();
    jobExecution.setExecutionContext(executionContext);
    jobExecution.setLastUpdated(new Date());
    jobExecution.incrementVersion();

    Lock writeLock = this.instanceLock.writeLock();
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

  ExecutionContext getExecutionContext(JobExecution jobExecution) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      ExecutionContext executionContext = this.jobExecutionContextsById.get(jobExecution.getId());
      return copyExecutionContext(executionContext);
    } finally {
      readLock.unlock();
    }
  }

  JobExecution createJobExecution(String jobName, JobParameters jobParameters)
      throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
    Lock writeLock = this.instanceLock.writeLock();
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
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      return this.getLastJobExecutionUnlocked(jobInstance);
    } finally {
      readLock.unlock();
    }
  }

  Set<JobExecution> findRunningJobExecutions(String jobName) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    Set<JobExecution> runningJobExecutions = new HashSet<>();
    try {
      List<JobInstanceAndParameters> jobInstancesAndParameters = this.jobInstancesByName.get(jobName);
      for (JobInstanceAndParameters jobInstanceAndParameters : jobInstancesAndParameters) {
        List<Long> jobExecutionIds = this.jobInstanceToExecutions.get(jobInstanceAndParameters.getJobInstance().getId());
        for (Long jobExecutionId : jobExecutionIds) {
          JobExecution jobExecution = this.jobExecutionsById.get(jobExecutionId);
          if (jobExecution.isRunning()) {
            runningJobExecutions.add(copyJobExecution(jobExecution));
          }
        }
      }
      return runningJobExecutions;
    } finally {
      readLock.unlock();
    }
  }

  List<JobExecution> findJobExecutions(JobInstance jobInstance) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      List<Long> jobExecutionIds = this.jobInstanceToExecutions.get(jobInstance.getId());
      List<JobExecution> jobExecutions = new ArrayList<>(jobExecutionIds.size());
      for (Long jobExecutionId : jobExecutionIds) {
        JobExecution jobExecution = this.jobExecutionsById.get(jobExecutionId);
        jobExecutions.add(copyJobExecution(jobExecution));
      }
      jobExecutions.sort(Comparator.comparing(JobExecution::getId));
      return jobExecutions;
    } finally {
      readLock.unlock();
    }
  }

  void update(JobExecution jobExecution) {

    jobExecution.setLastUpdated(new Date());
    this.synchronizeStatus(jobExecution);
    Long id = jobExecution.getId();

    Lock writeLock = this.instanceLock.writeLock();
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

  void updateJobExecutionContext(JobExecution jobExecution) {
    Long jobExecutionId = jobExecution.getId();
    ExecutionContext jobExecutionContext = jobExecution.getExecutionContext();
    if (jobExecutionContext != null) {
      ExecutionContext jobExecutionContextCopy = copyExecutionContext(jobExecutionContext);
      Lock writeLock = this.instanceLock.writeLock();
      try {
        this.jobExecutionContextsById.put(jobExecutionId, jobExecutionContextCopy);
      } finally {
        writeLock.unlock();
      }
    }
  }

  void synchronizeStatus(JobExecution jobExecution) {
    Lock readLock = this.instanceLock.readLock();
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
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      return this.instancesById.get(instanceId);
    } finally {
      readLock.unlock();
    }
  }

  JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      return this.getJobInstanceUnlocked(jobName, jobParameters);
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
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      return this.getJobInstanceUnlocked(jobName, jobParameters) != null;
    } finally {
      readLock.unlock();
    }
  }

  JobInstance getLastJobInstance(String jobName) {
    Lock readLock = this.instanceLock.readLock();
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

  JobExecution getJobExecution(Long jobExecutionId) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    JobExecution jobExecution;
    try {
      jobExecution = this.jobExecutionsById.get(jobExecutionId);
    } finally {
      readLock.unlock();
    }
    if (jobExecution != null) {
      return copyJobExecution(jobExecution);
    } else {
      return null;
    }
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

    Lock readLock = this.instanceLock.readLock();
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

    Lock readLock = this.instanceLock.readLock();
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
    Lock readLock = this.instanceLock.readLock();
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
    Lock readLock = this.instanceLock.readLock();
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

  List<StepExecution> getStepExecutions(JobExecution jobExecution) {
    Long jobExecutionId = jobExecution.getId();
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      Map<Long, StepExecution> executions = this.stepExecutionsByJobExecutionId.get(jobExecutionId);
      if ((executions == null) || executions.isEmpty()) {
        List.of();
      }
      List<StepExecution> stepExecutions = new ArrayList<>(executions.values());
      stepExecutions.sort(Comparator.comparing(StepExecution::getId));
      for (int i = 0; i < stepExecutions.size(); i++) {
        StepExecution stepExecution = stepExecutions.get(i);
        stepExecutions.set(i, copyStepExecution(stepExecution));
      }
      return stepExecutions;
    } finally {
      readLock.unlock();
    }
  }

  StepExecution getStepExecution(Long jobExecutionId, Long stepExecutionId) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    StepExecution stepExecution;
    try {
      Map<Long, StepExecution> stepExecutions = this.stepExecutionsByJobExecutionId.get(jobExecutionId);
      if (stepExecutions == null) {
        return null;
      }
      stepExecution = stepExecutions.get(stepExecutionId);
    } finally {
      readLock.unlock();
    }
    if (stepExecution != null) {
      return copyStepExecution(stepExecution);
    } else {
      return null;
    }
  }

  ExecutionContext getStepExecutionContext(StepExecution stepExecution) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      ExecutionContext executionContext = this.stepExecutionContextsById.get(stepExecution.getId());
      return copyExecutionContext(executionContext);
    } finally {
      readLock.unlock();
    }
  }


  StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
    StepExecution latest = null;
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      List<Long> jobExecutionIds = this.jobInstanceToExecutions.get(jobInstance.getInstanceId());
      for (Long jobExecutionId : jobExecutionIds) {
        for (StepExecution stepExecution : this.stepExecutionsByJobExecutionId.get(jobExecutionId).values()) {
          if (stepExecution.getStepName().equals(stepName)) {
            if (latest == null) {
              latest = stepExecution;
            } else if (latest.getStartTime().before(stepExecution.getStartTime())) {
              latest = stepExecution;
            } else if (latest.getStartTime().equals(stepExecution.getStartTime())
                // Use step execution ID as the tie breaker if start time is identical
                && (latest.getId() < stepExecution.getId())) {
              latest = stepExecution;
            }
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    if (latest != null) {
      return copyStepExecution(latest);
    } else {
      return null;
    }
  }

  void addStepExecution(StepExecution stepExecution) {
    Long jobExecutionId = stepExecution.getJobExecutionId();
    Lock writeLock = this.instanceLock.writeLock();
    try {
      Map<Long, StepExecution> stepExecutions = this.stepExecutionsByJobExecutionId.get(jobExecutionId);
      if (stepExecutions == null) {
        stepExecutions = new HashMap<>();
        this.stepExecutionsByJobExecutionId.put(jobExecutionId, stepExecutions);
      }

      Long stepExecutionId = this.nextStepExecutionId;
      stepExecution.setId(this.nextStepExecutionId++);//FIXME
      stepExecution.incrementVersion();

      StepExecution stepExecutionCopy = copyStepExecution(stepExecution);
      stepExecutions.put(stepExecutionId, stepExecutionCopy);

      ExecutionContext stepExecutionContext = stepExecutionCopy.getExecutionContext();
      if (stepExecutionContext != null) {
        // copying could in theory be done before acquiring the lock
        this.stepExecutionContextsById.put(stepExecutionId, copyExecutionContext(stepExecutionContext));
      }
    } finally {
      writeLock.unlock();
    }
  }

  void updateStepExecution(StepExecution stepExecution) {
    Long jobExecutionId = stepExecution.getJobExecutionId();
    Long stepExecutionId = stepExecution.getId();
    Lock writeLock = this.instanceLock.writeLock();
    try {
      Map<Long, StepExecution> setpExecutions = this.stepExecutionsByJobExecutionId.get(jobExecutionId);
      Objects.requireNonNull(setpExecutions, "step executions for given job execution are expected to be already saved");

      StepExecution persistedExecution = setpExecutions.get(stepExecutionId);
      Objects.requireNonNull(persistedExecution, "step execution is expected to be already saved");

      if (!persistedExecution.getVersion().equals(stepExecution.getVersion())) {
        throw new OptimisticLockingFailureException("Attempt to update step execution id="
            + stepExecutionId + " with wrong version (" + stepExecution.getVersion()
            + "), where current version is " + persistedExecution.getVersion());
      }

      stepExecution.incrementVersion();

      StepExecution stepExecutionCopy = copyStepExecution(stepExecution);
      setpExecutions.put(stepExecutionId, stepExecutionCopy);
    } finally {
      writeLock.unlock();
    }
  }

  void updateStepExecutionContext(StepExecution stepExecution) {
    Long stepExecutionId = stepExecution.getId();
    ExecutionContext stepExecutionContext = stepExecution.getExecutionContext();
    if (stepExecutionContext != null) {
      ExecutionContext stepExecutionContextCopy = copyExecutionContext(stepExecutionContext);
      Lock writeLock = this.instanceLock.writeLock();
      try {
        this.stepExecutionContextsById.put(stepExecutionId, stepExecutionContextCopy);
      } finally {
        writeLock.unlock();
      }
    }
  }

  int countStepExecutions(JobInstance jobInstance, String stepName) {
    int count = 0;
    Lock readLock = this.instanceLock.readLock();
    try {
      List<Long> jobExecutionIds = this.jobInstanceToExecutions.get(jobInstance.getInstanceId());
      for (Long jobExecutionId : jobExecutionIds) {
        for (StepExecution stepExecution : this.stepExecutionsByJobExecutionId.get(jobExecutionId).values()) {
          if (stepExecution.getStepName().equals(stepName)) {
            count++;
          }
        }
      }
    } finally {
      readLock.unlock();
    }
    return count;
  }

  private static ExecutionContext copyExecutionContext(ExecutionContext original) {
    return new ExecutionContext(original);
  }

  private static JobExecution copyJobExecution(JobExecution original) {
    // FIXME maybe we should clear the execution context
    return new JobExecution(original);
  }

  private static StepExecution copyStepExecution(StepExecution original) {
    StepExecution copy = new StepExecution(original.getStepName(), copyJobExecution(original.getJobExecution()), original.getId());
    copy.setStatus(original.getStatus());

    copy.setReadCount(original.getReadCount());
    copy.setWriteCount(original.getWriteCount());
    copy.setCommitCount(original.getCommitCount());
    copy.setRollbackCount(original.getRollbackCount());
    copy.setReadSkipCount(original.getReadSkipCount());
    copy.setProcessSkipCount(original.getProcessSkipCount());
    copy.setWriteSkipCount(original.getWriteSkipCount());

    copy.setStartTime(original.getStartTime());
    copy.setEndTime(original.getEndTime());
    copy.setLastUpdated(original.getLastUpdated());

    // FIXME likely we should not set this here as it gets set later
    copy.setExecutionContext(copyExecutionContext(original.getExecutionContext()));
    copy.setExitStatus(original.getExitStatus());
    if (original.isTerminateOnly()) {
      copy.setTerminateOnly();
    }
    copy.setFilterCount(original.getFilterCount());

    return copy;
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
