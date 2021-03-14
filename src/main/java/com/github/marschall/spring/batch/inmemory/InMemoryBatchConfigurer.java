package com.github.marschall.spring.batch.inmemory;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;

abstract class InMemoryBatchConfigurer implements BatchConfigurer {

  private final JobExplorer jobExplorer;

  private final JobRepository jobRepository;

  protected InMemoryBatchConfigurer() {
    InMemoryJobStorage storge = new InMemoryJobStorage();
    this.jobExplorer = new InMemoryJobExplorer(storge);
    this.jobRepository = new InMemoryJobRepository(storge);
  }

  @Override
  public JobExplorer getJobExplorer() {
    return this.jobExplorer;
  }

  @Override
  public JobRepository getJobRepository() {
    return this.jobRepository;
  }

}
