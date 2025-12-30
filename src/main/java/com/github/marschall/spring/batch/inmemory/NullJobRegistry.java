package com.github.marschall.spring.batch.inmemory;

import java.util.Collection;
import java.util.List;

import org.jspecify.annotations.Nullable;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.job.Job;

final class NullJobRegistry implements JobRegistry {

  @Override
  public @Nullable Job getJob(String name) {
    return null;
  }

  @Override
  public Collection<String> getJobNames() {
    return List.of();
  }

  @Override
  public void register(Job job) {
    // empty

  }

  @Override
  public void unregister(String jobName) {
    // empty
  }

}
