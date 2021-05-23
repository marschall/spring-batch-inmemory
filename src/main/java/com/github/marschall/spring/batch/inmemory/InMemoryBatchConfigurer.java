package com.github.marschall.spring.batch.inmemory;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

@Component
public class InMemoryBatchConfigurer implements BatchConfigurer {

  private final JobExplorer jobExplorer;

  private final JobRepository jobRepository;

  private final PlatformTransactionManager transactionManager;

  public InMemoryBatchConfigurer(PlatformTransactionManager transactionManager) {
    this.transactionManager = transactionManager;
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

  @Override
  public PlatformTransactionManager getTransactionManager() throws Exception {
    return this.transactionManager;
  }

  @Override
  public JobLauncher getJobLauncher() throws Exception {
    SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
    jobLauncher.setJobRepository(this.jobRepository);
    jobLauncher.afterPropertiesSet();
    return jobLauncher;
  }

}
