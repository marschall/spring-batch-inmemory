package com.github.marschall.spring.batch.inmemory;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Collection;
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
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.lang.Nullable;

/**
 * Backing store for {@link InMemoryJobRepository} and {@link InMemoryJobExplorer}.
 * <p>
 * You should create a {@link InMemoryJobRepository} and {@link InMemoryJobExplorer}
 * that share the same {@link InMemoryJobRepository}.
 * <p>
 * Instances of this class are thread-safe.
 */
public final class InMemoryJobStorage {

  private final Map<Long, JobInstance> instancesById;
  private final Map<String, List<JobInstanceAndParameters>> jobInstancesByName;
  private final Map<Long, JobExecution> jobExecutionsById;
  private final Map<Long, JobExecutions> jobInstanceToExecutions;
  private final Map<Long, ExecutionContext> jobExecutionContextsById;
  private final Map<Long, ExecutionContext> stepExecutionContextsById;
  private final Map<Long, Map<Long, StepExecution>> stepExecutionsByJobExecutionId;
  private final Map<StepExecutionKey, StepExecutionStatistics> stepExecutionStatistics;

  private final ReadWriteLock instanceLock;
  private long nextJobInstanceId;
  private long nextJobExecutionId;
  private long nextStepExecutionId;

  /**
   * Default constructor of {@link InMemoryJobRepository}.
   */
  public InMemoryJobStorage() {
    this.instancesById = new HashMap<>();
    this.jobInstancesByName = new HashMap<>();
    this.jobExecutionsById = new HashMap<>();
    this.jobExecutionContextsById = new HashMap<>();
    this.stepExecutionContextsById = new HashMap<>();
    this.jobInstanceToExecutions = new HashMap<>();
    this.stepExecutionsByJobExecutionId = new HashMap<>();
    this.stepExecutionStatistics = new HashMap<>();
    this.instanceLock = new ReentrantReadWriteLock();
    this.nextJobInstanceId = 1L;
    this.nextJobExecutionId = 1L;
    this.nextStepExecutionId = 1L;
  }

  private List<Long> getExecutionIds(JobInstance jobInstance) {
    JobExecutions jobExecutions = this.jobInstanceToExecutions.get(jobInstance.getId());
    if (jobExecutions != null) {
      return jobExecutions.getJobExecutionIds();
    } else {
      return List.of();
    }
  }

  private JobExecutions getJobExecutionsUnlocked(JobInstance jobInstance) {
    return this.jobInstanceToExecutions.get(jobInstance.getId());
  }

  JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");

    Lock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {
      this.verifyNoJobInstanceUnlocked(jobName, jobParameters);
      return this.createJobInstanceUnlocked(jobName, jobParameters);
    } finally {
      writeLock.unlock();
    }
  }

  private void verifyNoJobInstanceUnlocked(String jobName, JobParameters jobParameters) {
    JobInstance jobInstance = this.getJobInstance(jobName, jobParameters);
    if (jobInstance != null) {
      throw new IllegalStateException("JobInstance must not already exist");
    }
  }

  private JobInstance createJobInstanceUnlocked(String jobName, JobParameters jobParameters) {
    List<JobInstanceAndParameters> instancesAndParameters = this.jobInstancesByName.get(jobName);
    JobInstance jobInstance = new JobInstance(this.nextJobInstanceId++, jobName);
    jobInstance.incrementVersion();

    this.instancesById.put(jobInstance.getId(), jobInstance);
    if (instancesAndParameters == null) {
      instancesAndParameters = new ArrayList<>();
      this.jobInstancesByName.put(jobName, instancesAndParameters);
    }
    Map<String, JobParameter> identifyingJoParameters = jobParameters.getParameters().entrySet().stream()
                                 .filter(entry -> entry.getValue().isIdentifying())
                                 .collect(toMap(Entry::getKey, Entry::getValue));
    instancesAndParameters.add(new JobInstanceAndParameters(jobInstance, identifyingJoParameters));

    return jobInstance;
  }

  JobExecution createJobExecution(JobInstance jobInstance, JobParameters jobParameters, String jobConfigurationLocation) {

    Lock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {
      Long jobExecutionId = this.nextJobExecutionId++;
      JobExecution jobExecution = new JobExecution(jobInstance, jobExecutionId, jobParameters, jobConfigurationLocation);
      ExecutionContext executionContext = new ExecutionContext();
      jobExecution.setExecutionContext(executionContext);
      jobExecution.setLastUpdated(new Date());
      jobExecution.incrementVersion();

      this.jobExecutionsById.put(jobExecutionId, copyJobExecution(jobExecution));
      this.jobExecutionContextsById.put(jobExecutionId, copyExecutionContext(executionContext));
      this.mapJobExecutionUnlocked(jobInstance, jobExecution);
      return jobExecution;
    } finally {
      writeLock.unlock();
    }

  }

  private void mapJobExecutionUnlocked(JobInstance jobInstance, JobExecution jobExecution) {
    JobExecutions jobExecutions = this.jobInstanceToExecutions.computeIfAbsent(jobInstance.getId(), jobInstanceId -> new JobExecutions());
    jobExecutions.addJobExecutionId(jobExecution.getId());
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
      ExecutionContext executionContext = null;


      // existing job instance found
      if (jobInstance != null) {
        JobExecutions jobExecutions = this.getJobExecutionsUnlocked(jobInstance);
        if (jobExecutions == null) {
          throw new IllegalStateException("Cannot find any job execution for job instance: " + jobInstance);
        }

        if (jobExecutions.hasBlockingJobExecutionId()) {
          // will throw
          this.reportBlockingJobExecution(jobInstance, jobParameters, jobExecutions);
        } else {
          Long lastJobExecutionId = jobExecutions.getLastJobExecutionId();
          JobExecution lastJobExecution = this.jobExecutionsById.get(lastJobExecutionId);
          executionContext = copyExecutionContext(this.jobExecutionContextsById.get(lastJobExecution.getId()));
        }
      } else {
        // no job found, create one
        jobInstance = this.createJobInstanceUnlocked(jobName, jobParameters);
        executionContext = new ExecutionContext();
      }

      JobExecution jobExecution = new JobExecution(jobInstance, jobParameters, null);
      jobExecution.setExecutionContext(executionContext);
      jobExecution.setLastUpdated(new Date());

      // Save the JobExecution so that it picks up an ID (useful for clients
      // monitoring asynchronous executions):
      this.saveJobExecutionUnlocked(jobExecution);
      this.updateJobExecutionContextUnlocked(jobExecution.getId(), executionContext);
      this.mapJobExecutionUnlocked(jobInstance, jobExecution);

      return jobExecution;
    } finally {
      writeLock.unlock();
    }
  }

  private void reportBlockingJobExecution(JobInstance jobInstance, JobParameters jobParameters, JobExecutions jobExecutions)
          throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {
    List<Long> executionIds = jobExecutions.getJobExecutionIds();
    if (executionIds.isEmpty()) {
      throw new IllegalStateException("Cannot find any job execution for job instance: " + jobInstance);
    }

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
      if (((status == BatchStatus.COMPLETED) || (status == BatchStatus.ABANDONED)) && hasIdentifyingParameter(jobExecution)) {
        throw new JobInstanceAlreadyCompleteException(
                "A job instance already exists and is complete for parameters=" + jobParameters
                + ".  If you want to run this job again, change the parameters.");
      }
    }
  }

  private static boolean hasIdentifyingParameter(JobExecution jobExecution) {
    JobParameters jobParameters = jobExecution.getJobParameters();
    if (jobParameters.isEmpty()) {
      return false;
    }
    Map<String, JobParameter> parameterMap = jobParameters.getParameters();
    for (JobParameter jobParameter : parameterMap.values()) {
      if (jobParameter.isIdentifying()) {
        return true;
      }
    }
    return false;
  }

  private void saveJobExecutionUnlocked(JobExecution jobExecution) {
    long jobExecutionId = this.nextJobExecutionId++;
    jobExecution.setId(jobExecutionId);
    jobExecution.incrementVersion();
    this.jobExecutionsById.put(jobExecutionId, copyJobExecution(jobExecution));
  }

  private JobExecution getLastJobExecutionUnlocked(JobInstance jobInstance) {
    // last should have greatest id
    JobExecutions jobExecutions = this.getJobExecutionsUnlocked(jobInstance);
    if (jobExecutions != null) {
      Long lastExecutionId = jobExecutions.getLastJobExecutionId();
      JobExecution lastJobExecution = this.jobExecutionsById.get(lastExecutionId);
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
    try {
      List<JobInstanceAndParameters> jobInstancesAndParameters = this.jobInstancesByName.get(jobName);
      if (jobInstancesAndParameters == null) {
        return Set.of();
      }
      Set<JobExecution> runningJobExecutions = new HashSet<>();
      for (JobInstanceAndParameters jobInstanceAndParameters : jobInstancesAndParameters) {
        JobExecutions jobExecutions = this.getJobExecutionsUnlocked(jobInstanceAndParameters.getJobInstance());
        if (jobExecutions != null) {
          // running is a blocking status, therefore we can limit the number of job executions we check
          Set<Long> jobExecutionIds = jobExecutions.getBlockingJobExecutionIds();
          for (Long jobExecutionId : jobExecutionIds) {
            JobExecution jobExecution = this.jobExecutionsById.get(jobExecutionId);
            if (jobExecution.isRunning()) {
              runningJobExecutions.add(copyJobExecution(jobExecution));
            }
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
      List<Long> jobExecutionIds = this.getExecutionIds(jobInstance);
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
        this.updateBlockingStatusUnlocked(jobExecution);
      }
    } finally {
      writeLock.unlock();
    }
  }

  void updateJobExecutionContext(JobExecution jobExecution) {
    Long jobExecutionId = jobExecution.getId();
    ExecutionContext jobExecutionContext = jobExecution.getExecutionContext();
    if (jobExecutionContext != null) {
      Lock writeLock = this.instanceLock.writeLock();
      writeLock.lock();
      try {
        this.updateJobExecutionContextUnlocked(jobExecutionId, jobExecutionContext);
      } finally {
        writeLock.unlock();
      }
    }
  }

  private void updateJobExecutionContextUnlocked(Long jobExecutionId, ExecutionContext jobExecutionContext) {
    ExecutionContext jobExecutionContextCopy = copyExecutionContext(jobExecutionContext);
    this.jobExecutionContextsById.put(jobExecutionId, jobExecutionContextCopy);
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
        if (instanceAndParametes.areIdentifyingJoParametersEqualTo(jobParameters)) {
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
        return jobInstances.get(jobInstances.size() - 1).getJobInstance();
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
        return List.of();
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

  @Nullable
  private Collection<StepExecution> getStepExecutionsOfJobExecutionUnlocked(Long jobExecutionId) {
    Map<Long, StepExecution> stepExecutions = this.stepExecutionsByJobExecutionId.get(jobExecutionId);
    if (stepExecutions != null) {
      return stepExecutions.values();
    } else {
      // the iterator shows up during profiling
      return null;
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
      StepExecutionStatistics statistics = this.stepExecutionStatistics.get(new StepExecutionKey(jobInstance.getInstanceId(), stepName));
      if (statistics == null) {
        return null;
      }
      Long lastJobExecutionId = statistics.getLastJobExecutionId();
      Long lastStepExecutionId = statistics.getLastStepExecutionId();
      latest = this.stepExecutionsByJobExecutionId.get(lastJobExecutionId).get(lastStepExecutionId);
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
    writeLock.lock();
    try {
      Map<Long, StepExecution> stepExecutions = this.stepExecutionsByJobExecutionId.get(jobExecutionId);
      if (stepExecutions == null) {
        stepExecutions = new HashMap<>();
        this.stepExecutionsByJobExecutionId.put(jobExecutionId, stepExecutions);
      }

      Long stepExecutionId = this.nextStepExecutionId++;
      stepExecution.setId(stepExecutionId);
      stepExecution.incrementVersion();

      StepExecution stepExecutionCopy = copyStepExecution(stepExecution);
      stepExecutions.put(stepExecutionId, stepExecutionCopy);

      ExecutionContext stepExecutionContext = stepExecution.getExecutionContext();
      if (stepExecutionContext != null) {
        // copying could in theory be done before acquiring the lock
        this.stepExecutionContextsById.put(stepExecutionId, copyExecutionContext(stepExecutionContext));
      }

      Long jobInstanceId = stepExecution.getJobExecution().getJobInstance().getId();
      StepExecutionKey executionKey = new StepExecutionKey(jobInstanceId, stepExecution.getStepName());
      StepExecutionStatistics statistics = this.stepExecutionStatistics.get(executionKey);
      if (statistics == null) {
        this.stepExecutionStatistics.put(executionKey, new StepExecutionStatistics(stepExecutionId, jobExecutionId));
      } else {
        statistics.setLastExecutionIds(stepExecutionId, jobExecutionId);
        statistics.incrementStepExecutionCount();
      }
    } finally {
      writeLock.unlock();
    }
  }

  void updateStepExecution(StepExecution stepExecution) {
    Long jobExecutionId = stepExecution.getJobExecutionId();
    Long stepExecutionId = stepExecution.getId();
    Lock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
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
      writeLock.lock();
      try {
        this.stepExecutionContextsById.put(stepExecutionId, stepExecutionContextCopy);
      } finally {
        writeLock.unlock();
      }
    }
  }

  int countStepExecutions(JobInstance jobInstance, String stepName) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      StepExecutionStatistics statistics = this.stepExecutionStatistics.get(new StepExecutionKey(jobInstance.getInstanceId(), stepName));
      if (statistics != null) {
        return statistics.getStepExecutionCount();
      } else {
        return 0;
      }
    } finally {
      readLock.unlock();
    }
  }

  private static ExecutionContext copyExecutionContext(ExecutionContext original) {
    return new ExecutionContext(original);
  }

  private static JobExecution copyJobExecution(JobExecution original) {
    JobExecution copy = new JobExecution(original.getJobInstance(), original.getId(), original.getJobParameters(), original.getJobConfigurationName());
    copy.setVersion(original.getVersion());
    copy.setStatus(original.getStatus());
    copy.setStartTime(original.getStartTime());
    copy.setCreateTime(original.getCreateTime());
    copy.setEndTime(original.getEndTime());
    copy.setLastUpdated(original.getLastUpdated());
    copy.setExitStatus(original.getExitStatus());
    for (Throwable failureException : original.getFailureExceptions()) {
      copy.addFailureException(failureException);
    }
    // do not set the exeuction context
    // do not add step exeuctions
    return copy;
  }

  private static StepExecution copyStepExecution(StepExecution original) {
    StepExecution copy = new StepExecution(original.getStepName(), copyJobExecution(original.getJobExecution()), original.getId());
    copy.setVersion(original.getVersion());
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

    // do not set the execution context here as it gets set later
    copy.setExitStatus(original.getExitStatus());
    if (original.isTerminateOnly()) {
      copy.setTerminateOnly();
    }
    copy.setFilterCount(original.getFilterCount());

    return copy;
  }

  private void updateBlockingStatusUnlocked(JobExecution jobExecution) {
    JobExecutions jobExecutions = this.jobInstanceToExecutions.get(jobExecution.getJobInstance().getId());
    if (jobExecutions == null) {
      throw new IllegalStateException("JobExecution must already be registered");
    }
    boolean blocking = hasBlockingStatus(jobExecution);
    Long jobExecutionId = jobExecution.getId();
    if (blocking) {
      jobExecutions.addBlockingJobExecutionId(jobExecutionId);
    } else {
      jobExecutions.removeBlockingJobExecutionId(jobExecutionId);
    }
  }

  private static boolean hasBlockingStatus(JobExecution jobExecution) {
    if (jobExecution.isRunning() || jobExecution.isStopping()) {
      return true;
    }

    switch (jobExecution.getStatus()) {
      case UNKNOWN:
        return true;
      case COMPLETED:
      case ABANDONED:
        return hasIdentifyingParameter(jobExecution);
      default:
        return false;
    }
  }

  /**
   * Clears all data, affects all {@link InMemoryJobRepository} and {@link InMemoryJobExplorer}
   * backed by this object.
   */
  public void clear() {
    Lock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {
      this.instancesById.clear();
      this.jobInstancesByName.clear();
      this.jobExecutionsById.clear();
      this.jobExecutionContextsById.clear();
      this.stepExecutionContextsById.clear();
      this.jobInstanceToExecutions.clear();
      this.stepExecutionsByJobExecutionId.clear();
      this.stepExecutionStatistics.clear();
      this.nextJobInstanceId = 1L;
      this.nextJobExecutionId = 1L;
      this.nextStepExecutionId = 1L;
    } finally {
      writeLock.unlock();
    }
  }

  static final class StepExecutionKey {

    private final long jobInstanceId;

    private final String stepName;

    StepExecutionKey(long jobInstanceId, String stepName) {
      this.jobInstanceId = jobInstanceId;
      this.stepName = stepName;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.jobInstanceId, this.stepName);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof StepExecutionKey)) {
        return false;
      }
      StepExecutionKey other = (StepExecutionKey) obj;
      return (this.jobInstanceId == other.jobInstanceId)
              && Objects.equals(this.stepName, other.stepName);
    }

  }

  static final class StepExecutionStatistics {

    private Long lastStepExecutionId;

    private Long lastJobExecutionId;

    private int stepExecutionCount;

    StepExecutionStatistics(Long lastStepExecutionId, Long lastJobExecutionId) {
      this.lastStepExecutionId = lastStepExecutionId;
      this.lastJobExecutionId = lastJobExecutionId;
      this.stepExecutionCount = 1;
    }

    Long getLastStepExecutionId() {
      return this.lastStepExecutionId;
    }

    Long getLastJobExecutionId() {
      return this.lastJobExecutionId;
    }

    void setLastExecutionIds(Long lastStepExecutionId, Long lastJobExecutionId) {
      this.lastStepExecutionId = lastStepExecutionId;
      this.lastJobExecutionId = lastJobExecutionId;
    }

    int getStepExecutionCount() {
      return this.stepExecutionCount;
    }

    void incrementStepExecutionCount() {
      this.stepExecutionCount += 1;
    }

  }

  static final class JobInstanceAndParameters {

    private final JobInstance jobInstance;

    private final Map<String, JobParameter> identifyingJoParameters;

    JobInstanceAndParameters(JobInstance jobInstance, Map<String, JobParameter> identifyingJoParameters) {
      this.jobInstance = jobInstance;
      this.identifyingJoParameters =identifyingJoParameters;
    }

    JobInstance getJobInstance() {
      return this.jobInstance;
    }

    boolean areIdentifyingJoParametersEqualTo(JobParameters otherJobParameters) {
      Map<String, JobParameter> otherJobParametersMap = otherJobParameters.getParameters();
      for (Entry<String, JobParameter> identifyingJoParameter : this.identifyingJoParameters.entrySet()) {
        if (!identifyingJoParameter.getValue().equals(otherJobParametersMap.get(identifyingJoParameter.getKey()))) {
          return false;
        }
      }
      for (Entry<String, JobParameter> identifyingJoParameter : otherJobParametersMap.entrySet()) {
        JobParameter identifyingJoParameterValue = identifyingJoParameter.getValue();
        if (identifyingJoParameterValue.isIdentifying()) {
          if (!identifyingJoParameterValue.equals(this.identifyingJoParameters.get(identifyingJoParameter.getKey()))) {
            return false;
          }
        }
      }
      return true;
    }

  }

  static final class JobExecutions {

    private final Set<Long> blockingJobExecutionIds;

    private final List<Long> jobExecutionIds;

    JobExecutions() {
      this.blockingJobExecutionIds = new HashSet<>();
      this.jobExecutionIds = new ArrayList<>();
    }

    Set<Long> getBlockingJobExecutionIds() {
      return this.blockingJobExecutionIds;
    }

    void addBlockingJobExecutionId(Long jobExecutionId) {
      this.blockingJobExecutionIds.add(jobExecutionId);
    }

    void addJobExecutionId(Long jobExecutionId) {
      this.jobExecutionIds.add(jobExecutionId);
    }

    void removeBlockingJobExecutionId(Long jobExecutionId) {
      this.blockingJobExecutionIds.remove(jobExecutionId);
    }

    boolean hasBlockingJobExecutionId() {
      return !this.blockingJobExecutionIds.isEmpty();
    }

    Long getLastJobExecutionId() {
      return this.jobExecutionIds.get(this.jobExecutionIds.size() - 1);
    }

    List<Long> getJobExecutionIds() {
      return this.jobExecutionIds;
    }

  }

}
