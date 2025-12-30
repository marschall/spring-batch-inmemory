package com.github.marschall.spring.batch.inmemory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.configuration.DuplicateJobException;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.job.Job;

/**
 * Implementation of {@link JobRegistry} that uses a {@link HashMap}
 * and {@link ReadWriteLock} instead of {@link ConcurrentHashMap} to
 * save memory.
 */
public final class InMemoryJobRegistry implements JobRegistry {

  private final ReadWriteLock instanceLock;
  private final Map<String, Job> jobMap;

  public InMemoryJobRegistry() {
    this.instanceLock = new ReentrantReadWriteLock();
    this.jobMap = new HashMap<>();
  }

  @Override
  public @Nullable Job getJob(String name) {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      return this.jobMap.get(name);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Collection<String> getJobNames() {
    Lock readLock = this.instanceLock.readLock();
    readLock.lock();
    try {
      return List.copyOf(this.jobMap.keySet());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void register(Job job) throws DuplicateJobException {
    Objects.requireNonNull(job, "job");
    Lock writeLock = this.instanceLock.writeLock();
    Job previous;
    writeLock.lock();
    try {
      previous = this.jobMap.putIfAbsent(job.getName(), job);
    } finally {
      writeLock.unlock();
    }
    if (previous != null) {
      throw new DuplicateJobException(job.getName());
    }
  }

  @Override
  public void unregister(String jobName) {
    Objects.requireNonNull(jobName, "jobName");
    Lock writeLock = this.instanceLock.writeLock();
    writeLock.lock();
    try {
      this.jobMap.remove(jobName);
    } finally {
      writeLock.unlock();
    }
  }

}
