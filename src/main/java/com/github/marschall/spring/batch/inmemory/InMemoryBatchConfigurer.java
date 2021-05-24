package com.github.marschall.spring.batch.inmemory;

import java.util.Objects;

import org.springframework.batch.core.configuration.annotation.AbstractBatchConfiguration;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * A {@link BatchConfigurer} for use with {@link AbstractBatchConfiguration}
 * that sets up Spring Batch with a in-memory {@link JobRepository} and
 * {@link JobExplorer}.
 * <p>
 * Requires a {@link PlatformTransactionManager} is defined somewhere.
 */
@Component
public class InMemoryBatchConfigurer implements BatchConfigurer {

  private final JobExplorer jobExplorer;

  private final JobRepository jobRepository;

  private final PlatformTransactionManager transactionManager;

  /**
   * Constructs a new {@link InMemoryBatchConfigurer}.
   * 
   * @param transactionManager the transaction manager to use,
   *                           should be the transaction manager used by the tests,
   *                           if you tests don't or need a transaction manager then use {@link ResourcelessTransactionManager},
   *                           not {@code null}
   */
  public InMemoryBatchConfigurer(PlatformTransactionManager transactionManager) {
    Objects.requireNonNull(transactionManager, "transactionManager");
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
  public PlatformTransactionManager getTransactionManager() {
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
