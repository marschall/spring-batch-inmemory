package com.github.marschall.spring.batch.inmemory;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameter;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.step.NoSuchStepException;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.dao.OptimisticLockingFailureException;

/**
 * Backing store for {@link InMemoryJobRepository} and {@link InMemoryJobExplorer}.
 * <p>
 * You should create a {@link InMemoryJobRepository} and {@link InMemoryJobExplorer}
 * that share the same {@link InMemoryJobRepository}.
 * <p>
 * Instances of this class are thread-safe.
 */
public final class InMemoryJobStorage {

  /**
   * Since creating an {@link ExecutionContext} is quite heavy as it involves creating a new
   * {@link ConcurrentHashMap} we use this as a sentinel for an empty execution context.
   * We use this instance only internally and never pass it out or modify it.
   */
  private static final ExecutionContext EMPTY_EXECUTION_CONTEXT = new ExecutionContext();

  private final Map<Long, JobInstance> instancesById;
  private final Map<String, List<JobInstanceAndParameters>> jobInstancesByName;
  private final Map<Long, JobExecution> jobExecutionsById;
  private final Map<Long, JobExecutions> jobInstanceToExecutions;
  private final Map<Long, ExecutionContext> jobExecutionContextsById;
  private final Map<Long, ExecutionContext> stepExecutionContextsById;
  private final Map<Long, Map<Long, StepExecution>> stepExecutionsByJobExecutionId;
  private final Map<Long, StepExecution> stepExecutionsById;
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
    this.stepExecutionsById = new HashMap<>();
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

  List<JobExecution> getJobExecutions(JobInstance jobInstance) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    List<JobExecution> jobExecutions;
    try {
      List<Long> jobExecutionIds = this.getJobExecutionsUnlocked(jobInstance).jobExecutionIds();
      jobExecutions = new ArrayList<>(jobExecutionIds.size());
      for (Long jobExecutionId : jobExecutionIds) {
        JobExecution jobExecution = this.jobExecutionsById.get(jobExecutionId);
        jobExecutions.add(this.copyJobExecutionWithDependencies(jobExecution));
      }
    } finally {
      readLock.unlock();
    }
    return jobExecutions;
  }

  private JobExecutions getJobExecutionsUnlocked(JobInstance jobInstance) {
    return this.jobInstanceToExecutions.get(jobInstance.getId());
  }

  JobInstance createJobInstance(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");

    Map<String, JobParameter<?>> identifyingJobParameters = identifyingParametersMap(jobParameters);
    Lock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {
      this.verifyNoJobInstanceUnlocked(jobName, identifyingJobParameters);
      return this.createJobInstanceUnlocked(jobName, jobParameters, identifyingJobParameters);
    } finally {
      writeLock.unlock();
    }
  }

  private void verifyNoJobInstanceUnlocked(String jobName, Map<String, JobParameter<?>> identifyingJobParameters) {
    JobInstance jobInstance = this.getJobInstanceUnlocked(jobName, identifyingJobParameters);
    if (jobInstance != null) {
      throw new IllegalStateException("JobInstance must not already exist");
    }
  }

  private JobInstance createJobInstanceUnlocked(String jobName, JobParameters jobParameters, Map<String, JobParameter<?>> identifyingJobParameters) {
    List<JobInstanceAndParameters> instancesAndParameters = this.jobInstancesByName.get(jobName);
    JobInstance jobInstance = new JobInstance(this.nextJobInstanceId++, jobName);
    jobInstance.incrementVersion();

    this.instancesById.put(jobInstance.getId(), jobInstance);
    if (instancesAndParameters == null) {
      instancesAndParameters = new ArrayList<>();
      this.jobInstancesByName.put(jobName, instancesAndParameters);
    }
    instancesAndParameters.add(new JobInstanceAndParameters(jobInstance, identifyingJobParameters));

    return jobInstance;
  }

  private static Map<String, JobParameter<?>> identifyingParametersMap(JobParameters jobParameters) {
    return jobParameters.parameters().stream()
                                     .filter(JobParameter::identifying)
                                     .collect(toMap(JobParameter::name, Function.identity()));
  }

  private void mapJobExecutionUnlocked(JobInstance jobInstance, JobExecution jobExecution) {
    JobExecutions jobExecutions = this.jobInstanceToExecutions.computeIfAbsent(jobInstance.getId(), jobInstanceId -> new JobExecutions());
    jobExecutions.addJobExecutionId(jobExecution.getId());
  }

  private void storeJobExecutionContextUnlocked(JobExecution jobExecution) {
    ExecutionContext executionContext = jobExecution.getExecutionContext();
    if (executionContext.isEmpty()) {
      this.jobExecutionContextsById.put(jobExecution.getId(), EMPTY_EXECUTION_CONTEXT);
    } else {
      this.jobExecutionContextsById.put(jobExecution.getId(), copyExecutionContext(executionContext));
    }
  }

  private ExecutionContext loadJobExecutionContextUnlocked(JobExecution jobExecution) {
    ExecutionContext executionContext = this.jobExecutionContextsById.get(jobExecution.getId());
    // we assume the job was initialized with an empty execution context
    // therefore if the stored one is also empty there is no need to update it
    if (!executionContext.isEmpty()) {
      return copyExecutionContext(executionContext);
    } else {
      return null;
    }
  }

  private void setJobExecutionContextUnlocked(JobExecution jobExecution) {
    ExecutionContext executionContext = this.loadJobExecutionContextUnlocked(jobExecution);
    if (executionContext != null) {
      // we assume the job was initialized with an empty execution context
      // therefore if the stored one is also empty there is no need to update it
      jobExecution.setExecutionContext(executionContext);
    }
  }

  private void storeStepExecutionContextUnlocked(StepExecution stepExecution) {
    ExecutionContext executionContext = stepExecution.getExecutionContext();
    if (executionContext.isEmpty()) {
      this.stepExecutionContextsById.put(stepExecution.getId(), EMPTY_EXECUTION_CONTEXT);
    } else {
      this.stepExecutionContextsById.put(stepExecution.getId(), copyExecutionContext(executionContext));
    }
  }

  private ExecutionContext loadStepExecutionContextUnlocked(StepExecution stepExecution) {
    ExecutionContext executionContext = this.stepExecutionContextsById.get(stepExecution.getId());
    // we assume the job was initialized with an empty execution context
    // therefore if the stored one is also empty there is no need to update it
    if (!executionContext.isEmpty()) {
      return copyExecutionContext(executionContext);
    } else {
      return null;
    }
  }

  private void setStepExecutionContextUnlocked(StepExecution stepExecution) {
    ExecutionContext executionContext = this.loadStepExecutionContextUnlocked(stepExecution);
    if (executionContext != null) {
      // we assume the job was initialized with an empty execution context
      // therefore if the stored one is also empty there is no need to update it
      stepExecution.setExecutionContext(executionContext);
    }
  }

  JobExecution createJobExecution(JobInstance jobInstance, JobParameters jobParameters, ExecutionContext executionContext) {
    Lock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {

      JobExecution jobExecution = new JobExecution(this.nextJobExecutionId++, jobInstance, jobParameters);
      jobExecution.setExecutionContext(executionContext);
      jobExecution.setLastUpdated(LocalDateTime.now());

      // Save the JobExecution so that it picks up an ID (useful for clients
      // monitoring asynchronous executions):
      this.saveJobExecutionUnlocked(jobExecution);
      this.storeJobExecutionContextUnlocked(jobExecution);
      this.mapJobExecutionUnlocked(jobInstance, jobExecution);
      
      jobInstance.addJobExecution(jobExecution);

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
    return jobParameters.parameters().stream()
            .anyMatch(JobParameter::identifying);
  }

  private void saveJobExecutionUnlocked(JobExecution jobExecution) {
    jobExecution.incrementVersion();
    this.jobExecutionsById.put(jobExecution.getId(), copyJobExecution(jobExecution));
  }

  private JobExecution getLastJobExecutionUnlocked(JobInstance jobInstance) {
    // last should have greatest id
    JobExecutions jobExecutions = this.getJobExecutionsUnlocked(jobInstance);
    if (jobExecutions != null) {
      Long lastExecutionId = jobExecutions.getLastJobExecutionId();
      JobExecution lastJobExecution = this.jobExecutionsById.get(lastExecutionId);
      if (lastJobExecution != null) {
        return this.copyJobExecutionWithDependencies(lastJobExecution);
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  private JobExecution copyJobExecutionWithDependencies(JobExecution jobExecution) {
    JobExecution copy = copyJobExecution(jobExecution);
    this.setJobExecutionContextUnlocked(copy);
    this.loadStepExecutionsUnlocked(copy);
    for (StepExecution stepExecution : copy.getStepExecutions()) {
      this.setStepExecutionContextUnlocked(stepExecution);
    }
    return copy;
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

  JobExecution getLastJobExecution(String jobName, JobParameters jobParameters) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      JobInstance jobInstance = this.getJobInstanceUnlocked(jobName, jobParameters);
      if (jobInstance == null) {
        return null;
      }
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
        JobExecutions jobExecutions = this.getJobExecutionsUnlocked(jobInstanceAndParameters.jobInstance());
        if (jobExecutions != null) {
          // running is a blocking status, therefore we can limit the number of job executions we check
          Set<Long> jobExecutionIds = jobExecutions.blockingJobExecutionIds();
          for (Long jobExecutionId : jobExecutionIds) {
            JobExecution jobExecution = this.jobExecutionsById.get(jobExecutionId);
            if (jobExecution.isRunning()) {
              runningJobExecutions.add(this.copyJobExecutionWithDependencies(jobExecution));
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
        jobExecutions.add(this.copyJobExecutionWithDependencies(jobExecution));
      }
      jobExecutions.sort(Comparator.comparing(JobExecution::getId));
      return jobExecutions;
    } finally {
      readLock.unlock();
    }
  }

  void update(JobExecution jobExecution) {

    jobExecution.setLastUpdated(LocalDateTime.now());
    this.synchronizeStatus(jobExecution);
    long id = jobExecution.getId();

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
        if (persisted.getVersion().intValue() != jobExecution.getVersion().intValue()) {
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
    ExecutionContext jobExecutionContext = jobExecution.getExecutionContext();
    if (jobExecutionContext != null) {
      Lock writeLock = this.instanceLock.writeLock();
      writeLock.lock();
      try {
        this.storeJobExecutionContextUnlocked(jobExecution);
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
      // not clear if synchronized is needed
      if (presisted.getVersion().intValue() != jobExecution.getVersion().intValue()) {
        jobExecution.upgradeStatus(presisted.getStatus());
        jobExecution.setVersion(presisted.getVersion());
      }

    } finally {
      readLock.unlock();
    }
  }

  JobInstance getJobInstance(long instanceId) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      return this.instancesById.get(instanceId);
    } finally {
      readLock.unlock();
    }
  }

  JobInstance getJobInstance(String jobName, JobParameters jobParameters) {
    Objects.requireNonNull(jobName, "jobName");
    Objects.requireNonNull(jobParameters, "jobParameters");
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      return this.getJobInstanceUnlocked(jobName, jobParameters);
    } finally {
      readLock.unlock();
    }
  }

  private JobInstance getJobInstanceUnlocked(String jobName, JobParameters jobParameters) {
    return getJobInstanceUnlocked(jobName, identifyingParametersMap(jobParameters));
  }

  private JobInstance getJobInstanceUnlocked(String jobName, Map<String, JobParameter<?>> identifyingJobParameters) {
    List<JobInstanceAndParameters> instancesAndParameters = this.jobInstancesByName.get(jobName);
    if(instancesAndParameters != null) {
      for (JobInstanceAndParameters instanceAndParametes : instancesAndParameters) {
        if (instanceAndParametes.areIdentifyingJobParametersEqualTo(identifyingJobParameters)) {
          return instanceAndParametes.jobInstance();
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
      return this.getLastJobInstanceUnlocked(jobName);
    } finally {
      readLock.unlock();
    }
  }

  private JobInstance getLastJobInstanceUnlocked(String jobName) {
    List<JobInstanceAndParameters> jobInstances = this.jobInstancesByName.get(jobName);
    if ((jobInstances == null) || jobInstances.isEmpty()) {
      return null;
    }
    if (jobInstances.size() == 1) {
      return jobInstances.get(0).jobInstance();
    } else {
      // the last one should have the highest id
      return jobInstances.get(jobInstances.size() - 1).jobInstance();
    }
  }

  JobExecution getJobExecution(long jobExecutionId) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      JobExecution jobExecution = this.jobExecutionsById.get(jobExecutionId);
      if (jobExecution != null) {
        return this.copyJobExecutionWithDependencies(jobExecution);
      } else {
        return null;
      }
    } finally {
      readLock.unlock();
    }
  }

  List<JobInstance> findJobInstancesByJobName(String jobName, int start, int count) {
    return this.getJobInstancesByNamePattern(jobName, start, count);
  }
  
  List<JobInstance> findJobInstancesByJobName(String jobName) {
    return this.getJobInstancesByNamePattern(jobName);
  }

  List<JobInstance> getJobInstances(String jobName, int start, int count) {
    return this.getJobInstancesByNamePattern(jobName, start, count);
  }

  private List<JobInstance> getJobInstancesByNameExact(String jobName, int start, int count) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      if ((start == 0) && (count == 1)) {
        JobInstance lastJobInstance = this.getLastJobInstanceUnlocked(jobName);
        if (lastJobInstance != null) {
          return List.of(lastJobInstance);
        } else {
          return List.of();
        }
      }

      List<JobInstanceAndParameters> jobInstances = this.jobInstancesByName.get(jobName);
      if ((jobInstances == null) || jobInstances.isEmpty()) {
        return List.of();
      }
      return jobInstances.stream()
                         .map(JobInstanceAndParameters::jobInstance)
                         .sorted(Comparator.comparingLong(JobInstance::getInstanceId))
                         .skip(start)
                         .limit(count)
                         .collect(toList());
    } finally {
      readLock.unlock();
    }
  }

  private static @Nullable Pattern getJobNamePattern(String jobName) {
    boolean exactPattern = isExactPattern(jobName);
    return exactPattern ? null : Pattern.compile(jobName.replaceAll("\\*", ".*"));
  }

  private List<JobInstance> getJobInstancesByNamePattern(String jobName, int start, int count) {
    Pattern pattern = getJobNamePattern(jobName);
    List<JobInstance> jobInstancesUnstorted;

    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      jobInstancesUnstorted = getJobInstancesUnsorted(jobName, pattern);
    } finally {
      readLock.unlock();
    }

    return jobInstancesUnstorted.stream()
                                .sorted(Comparator.comparingLong(JobInstance::getInstanceId))
                                .skip(start)
                                .limit(count)
                                .toList();
  }
  
  private List<JobInstance> getJobInstancesByNamePattern(String jobName) {
    Pattern pattern = getJobNamePattern(jobName);
    List<JobInstance> jobInstancesUnstorted;
    
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      jobInstancesUnstorted = getJobInstancesUnsorted(jobName, pattern);
    } finally {
      readLock.unlock();
    }
    
    return jobInstancesUnstorted.stream()
            .sorted(Comparator.comparingLong(JobInstance::getInstanceId))
            .toList();
  }

  // TODO matcher
  private List<JobInstance> getJobInstancesUnsorted(String jobName, @Nullable Pattern pattern) {
    List<JobInstance> jobInstances = new ArrayList<>();
    for (Entry<String, List<JobInstanceAndParameters>> entries : this.jobInstancesByName.entrySet()) {
      boolean matches;
      if (pattern != null) {
        matches = pattern.matcher(entries.getKey()).matches();
      } else {
        matches = entries.getKey().equals(jobName);
      }
      if (matches) {
        for (JobInstanceAndParameters jobInstanceAndParameters : entries.getValue()) {
          jobInstances.add(jobInstanceAndParameters.jobInstance());
        }
      }
    }
    return jobInstances;
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

  long getJobInstanceCount(String jobName) throws NoSuchJobException {
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

  private void loadStepExecutionsUnlocked(JobExecution jobExecution) {
    long jobExecutionId = jobExecution.getId();
    Map<Long, StepExecution> executions = this.stepExecutionsByJobExecutionId.get(jobExecutionId);
    if ((executions == null) || executions.isEmpty()) {
      return;
    }
    List<StepExecution> stepExecutions = new ArrayList<>(executions.values());
    stepExecutions.sort(Comparator.comparing(StepExecution::getId));
    for (int i = 0; i < stepExecutions.size(); i++) {
      StepExecution stepExecution = stepExecutions.get(i);
      StepExecution copy = copyStepExecutionForReading(stepExecution, jobExecution);
      stepExecutions.set(i, copy);
    }
    jobExecution.addStepExecutions(stepExecutions);
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

  StepExecution getStepExecution(long stepExecutionId) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {;
    return this.getStepExecutionUnlocked(stepExecutionId);
    } finally {
      readLock.unlock();
    }
  }

  StepExecution getStepExecution(long jobExecutionId, long stepExecutionId) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {;
      return this.getStepExecutionUnlocked(jobExecutionId, stepExecutionId);
    } finally {
      readLock.unlock();
    }
  }

  private StepExecution getStepExecutionUnlocked(long jobExecutionId, long stepExecutionId) {
    JobExecution jobExecution = this.jobExecutionsById.get(jobExecutionId);
    if (jobExecution == null) {
      return null;
    }
    JobExecution parent = copyJobExecution(jobExecution);
    // only load the job execution context, not all steps
    this.setJobExecutionContextUnlocked(parent);


    Map<Long, StepExecution> stepExecutions = this.stepExecutionsByJobExecutionId.get(jobExecution.getId());
    if (stepExecutions == null) {
      return null;
    }
    StepExecution stepExecution = stepExecutions.get(stepExecutionId);
    if (stepExecution != null) {
      return copyStepExecutionForReading(stepExecution, jobExecution);
    } else {
      return null;
    }
  }

  private StepExecution getStepExecutionUnlocked(long stepExecutionId) {
    StepExecution stepExecution = stepExecutionsById.get(stepExecutionId);
    if (stepExecution == null) {
      return null;
    }

    JobExecution jobExecution = this.jobExecutionsById.get(stepExecution.getJobExecutionId());
    if (jobExecution == null) {
      throw new IllegalStateException("job execution not found");
    }
    JobExecution parent = copyJobExecution(jobExecution);
    // only load the job execution context, not all steps
    this.setJobExecutionContextUnlocked(parent);

    return copyStepExecutionForReading(stepExecution, jobExecution);
  }

  StepExecution getLastStepExecution(JobInstance jobInstance, String stepName) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      StepExecutionStatistics statistics = this.stepExecutionStatistics.get(new StepExecutionKey(jobInstance.getInstanceId(), stepName));
      if (statistics == null) {
        return null;
      }
      long lastJobExecutionId = statistics.getLastJobExecutionId();
      long lastStepExecutionId = statistics.getLastStepExecutionId();
      JobExecution latestJobExecution = this.jobExecutionsById.get(lastJobExecutionId);
      StepExecution latestStepExecution = this.stepExecutionsByJobExecutionId.get(lastJobExecutionId).get(lastStepExecutionId);
      if (latestStepExecution != null) {
        StepExecution result = copyStepExecutionForReading(latestStepExecution, copyJobExecution(latestJobExecution));
        this.setJobExecutionContextUnlocked(result.getJobExecution());
        this.setStepExecutionContextUnlocked(result);
        return result;
      } else {
        return null;
      }
    } finally {
      readLock.unlock();
    }
  }

  StepExecution createStepExecution(String stepName, JobExecution jobExecution) {
    Lock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    StepExecution stepExecution;
    try {
      stepExecution = new StepExecution(this.nextStepExecutionId++, stepName, jobExecution);
      this.addStepExecutionUnlocked(stepExecution, jobExecution);
    } finally {
      writeLock.unlock();
    }
    return stepExecution;
  }

  private void addStepExecutionUnlocked(StepExecution stepExecution, JobExecution jobExecution) {
    long jobExecutionId = stepExecution.getJobExecutionId();

    // TODO no need to look up for every step of the name job
    Map<Long, StepExecution> stepExecutions = this.stepExecutionsByJobExecutionId.get(jobExecutionId);
    if (stepExecutions == null) {
      stepExecutions = new HashMap<>();
      this.stepExecutionsByJobExecutionId.put(jobExecutionId, stepExecutions);
    }

    long jobInstanceId = jobExecution.getJobInstance().getId();
    long stepExecutionId = stepExecution.getId();
    stepExecution.incrementVersion();

    StepExecution stepExecutionCopy = copyStepExecutionForStorage(stepExecution, jobExecution);
    stepExecutions.put(stepExecutionId, stepExecutionCopy);

    this.storeStepExecutionContextUnlocked(stepExecution);

    StepExecutionKey executionKey = new StepExecutionKey(jobInstanceId, stepExecution.getStepName());
    StepExecutionStatistics statistics = this.stepExecutionStatistics.get(executionKey);
    if (statistics == null) {
      this.stepExecutionStatistics.put(executionKey, new StepExecutionStatistics(stepExecutionId, jobExecutionId));
    } else {
      statistics.setLastExecutionIds(stepExecutionId, jobExecutionId);
      statistics.incrementStepExecutionCount();
    }
    
    jobExecution.addStepExecution(stepExecution);
  }

  void updateStepExecution(StepExecution stepExecution) {
    long jobExecutionId = stepExecution.getJobExecutionId();
    long stepExecutionId = stepExecution.getId();
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

      StepExecution stepExecutionCopy = copyStepExecutionForStorage(stepExecution, stepExecution.getJobExecution());
      setpExecutions.put(stepExecutionId, stepExecutionCopy);
    } finally {
      writeLock.unlock();
    }
  }

  void deleteStepExecution(StepExecution stepExecution) {
    long jobExecutionId = stepExecution.getJobExecutionId();
    long stepExecutionId = stepExecution.getId();
    Lock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {
      this.stepExecutionContextsById.remove(stepExecutionId);
      Map<Long, StepExecution> stepExecutions = this.stepExecutionsByJobExecutionId.get(jobExecutionId);
      if (stepExecutions != null) {
        if (stepExecutions.remove(stepExecutionId) != null) {
          if (stepExecutions.isEmpty()) {
            stepExecutionsByJobExecutionId.remove(jobExecutionId);
          }
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  void deleteJobExecution(JobExecution jobExecution) {
    Lock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {
      long jobExecutionId = jobExecution.getId();
      Map<Long, StepExecution> stepExecutions = this.stepExecutionsByJobExecutionId.get(jobExecutionId);
      if (stepExecutions != null && !stepExecutions.isEmpty()) {
        // violates API contract but consistent with SimpleJobRepository
        throw new IllegalStateException("found existing job executions");
        
      }
      this.jobExecutionsById.remove(jobExecutionId);
      JobExecutions jobExecutions = this.jobInstanceToExecutions.get(jobExecutionId);
      if (jobExecutions != null) {
        jobExecutions.removeJobExecutionId(jobExecutionId);
        jobExecutions.removeBlockingJobExecutionId(jobExecutionId);
      }
      this.jobExecutionContextsById.remove(jobExecutionId);
      // TODO delete rest of graph
    } finally {
      writeLock.unlock();
    }
  }

  void deleteJobExecution(JobInstance jobInstance) {
    Lock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {
      JobExecutions jobExecutions = this.jobInstanceToExecutions.get(jobInstance.getInstanceId());
      if (jobExecutions != null && !jobExecutions.isEmpty()) {
        // API contract is unclear
        throw new IllegalStateException("found existing job executions");
      }
      // TODO delete stepExecutionStatistics
      // TODO delete tree
      this.instancesById.remove(jobInstance.getInstanceId());
      this.jobInstancesByName.remove(jobInstance.getJobName());
    } finally {
      writeLock.unlock();
    }
  }

  void updateStepExecutionContext(StepExecution stepExecution) {
    ExecutionContext stepExecutionContext = stepExecution.getExecutionContext();
    if (stepExecutionContext != null) {
      Lock writeLock = this.instanceLock.writeLock();
      writeLock.lock();
      try {
        this.storeStepExecutionContextUnlocked(stepExecution);
      } finally {
        writeLock.unlock();
      }
    }
  }

  long countStepExecutions(JobInstance jobInstance, String stepName) throws NoSuchStepException {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      StepExecutionStatistics statistics = this.stepExecutionStatistics.get(new StepExecutionKey(jobInstance.getInstanceId(), stepName));
      if (statistics != null) {
        return statistics.getStepExecutionCount();
      } else {
        // FIXME org.springframework.batch.core.job.SimpleStepHandler.shouldStart(StepExecution, JobExecution, Step) is broken
        return 0;
//      throw new NoSuchStepException(stepName);
      }
    } finally {
      readLock.unlock();
    }
  }

  private static ExecutionContext copyExecutionContext(ExecutionContext original) {
    return new ExecutionContext(original);
  }

  private static JobExecution copyJobExecution(JobExecution original) {
    JobExecution copy = new JobExecution(original.getId(), original.getJobInstance(), original.getJobParameters());
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
    // do not set the execution context
    // do not add step executions
    return copy;
  }

  // does not add the StepExecution to the JobExecution
  private static StepExecution copyStepExecutionForStorage(StepExecution original, JobExecution jobExecution) {
    StepExecution copy = new StepExecution(original.getId(), original.getStepName(), jobExecution);
    copyStepProperties(original, copy);
    return copy;
  }

  private static StepExecution copyStepExecutionForReading(StepExecution original, JobExecution jobExecution) {
    StepExecution copy = new StepExecution(original.getId(), original.getStepName(), jobExecution);
    copyStepProperties(original, copy);
    return copy;
  }

  private static void copyStepProperties(StepExecution original, StepExecution copy) {
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
  }

  private void updateBlockingStatusUnlocked(JobExecution jobExecution) {
    JobExecutions jobExecutions = this.jobInstanceToExecutions.get(jobExecution.getJobInstance().getId());
    if (jobExecutions == null) {
      throw new IllegalStateException("JobExecution must already be registered");
    }
    boolean blocking = hasBlockingStatus(jobExecution);
    long jobExecutionId = jobExecution.getId();
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

    return switch (jobExecution.getStatus()) {
      case UNKNOWN -> true;
      case COMPLETED, ABANDONED -> hasIdentifyingParameter(jobExecution);
      default -> false;
    };
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
      this.stepExecutionsById.clear();
      this.stepExecutionStatistics.clear();
      this.nextJobInstanceId = 1L;
      this.nextJobExecutionId = 1L;
      this.nextStepExecutionId = 1L;
    } finally {
      writeLock.unlock();
    }
  }

  record StepExecutionKey(long jobInstanceId, String stepName) {

  }

  static final class StepExecutionStatistics {

    private long lastStepExecutionId;

    private long lastJobExecutionId;

    private long stepExecutionCount;

    StepExecutionStatistics(long lastStepExecutionId, long lastJobExecutionId) {
      this.lastStepExecutionId = lastStepExecutionId;
      this.lastJobExecutionId = lastJobExecutionId;
      this.stepExecutionCount = 1;
    }

    long getLastStepExecutionId() {
      return this.lastStepExecutionId;
    }

    long getLastJobExecutionId() {
      return this.lastJobExecutionId;
    }

    void setLastExecutionIds(long lastStepExecutionId, long lastJobExecutionId) {
      this.lastStepExecutionId = lastStepExecutionId;
      this.lastJobExecutionId = lastJobExecutionId;
    }

    long getStepExecutionCount() {
      return this.stepExecutionCount;
    }

    void incrementStepExecutionCount() {
      this.stepExecutionCount += 1L;
    }

  }

  record JobInstanceAndParameters(JobInstance jobInstance, Map<String, JobParameter<?>> identifyingJobParameters) {

    boolean areIdentifyingJobParametersEqualTo(Map<String, JobParameter<?>> otherJobParametersMap) {


      if (!this.identifyingJobParameters.keySet().equals(otherJobParametersMap.keySet())) {
        return false;
      }
      // JobParameter equals ignores value
      for (Entry<String, JobParameter<?>> entry : this.identifyingJobParameters.entrySet()) {
        JobParameter<?> thisParameter = entry.getValue();
        String parameterName = entry.getKey();
        JobParameter<?> otherParameter = otherJobParametersMap.get(parameterName);
        if (!otherParameter.value().equals(thisParameter.value())) {
          return false;
        }
      }
      return true;
    }

  }

  record JobExecutions(Set<Long> blockingJobExecutionIds, List<Long> jobExecutionIds) {

    JobExecutions() {
      this(new HashSet<>(), new ArrayList<>());
    }

    void addBlockingJobExecutionId(long jobExecutionId) {
      this.blockingJobExecutionIds.add(jobExecutionId);
    }

    void addJobExecutionId(long jobExecutionId) {
      this.jobExecutionIds.add(jobExecutionId);
    }

    void removeBlockingJobExecutionId(long jobExecutionId) {
      this.blockingJobExecutionIds.remove(jobExecutionId);
    }
    
    void removeJobExecutionId(long jobExecutionId) {
      this.jobExecutionIds.remove(jobExecutionId);
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
    
    boolean isEmpty() {
      return this.jobExecutionIds.isEmpty() && this.blockingJobExecutionIds.isEmpty();
    }

  }

}
