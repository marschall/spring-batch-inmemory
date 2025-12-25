package com.github.marschall.spring.batch.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.JobExecutionException;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.FlowStep;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.job.flow.support.StateTransition;
import org.springframework.batch.core.job.flow.support.state.EndState;
import org.springframework.batch.core.job.flow.support.state.StepState;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.explore.JobExplorer;
import org.springframework.batch.core.repository.explore.support.SimpleJobExplorer;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.StepExecution;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.ExecutionContext;
import org.springframework.batch.infrastructure.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Integration test for the BATCH-2034 issue.
 * The {@link FlowStep} execution should not fail in the remote partitioning use case because the {@link SimpleJobExplorer}
 * doesn't retrieve the {@link JobInstance} from the {@link JobRepository}.
 * To illustrate the issue the test simulates the behavior of the {@code StepExecutionRequestHandler}
 * from the spring-batch-integration project.
 */
@SpringJUnitConfig
class SimpleJobExplorerIntegrationTests {

  @Configuration
  @Import(InMemoryBatchConfiguration.class)
  @EnableBatchProcessing
  static class Config {

    @Autowired
    private JobRepository jobRepository;

    @Bean
    Step flowStep() throws Exception {
      return new StepBuilder("flowStep", this.jobRepository)
          .flow(this.simpleFlow())
          .build();
    }

    @Bean
    Step dummyStep() {
      return new DummyStep("dummyStep");
    }

    @Bean
    SimpleFlow simpleFlow() {
      SimpleFlow simpleFlow = new SimpleFlow("simpleFlow");
      List<StateTransition> transitions = new ArrayList<>();
      transitions.add(StateTransition.createStateTransition(new StepState(this.dummyStep()), "end0"));
      transitions.add(StateTransition.createEndStateTransition(new EndState(FlowExecutionStatus.COMPLETED, "end0")));
      simpleFlow.setStateTransitions(transitions);
      return simpleFlow;
    }

    @Bean
    PlatformTransactionManager transactionManager() {
      return new ResourcelessTransactionManager();
    }

  }

  @Autowired
  private JobRepository jobRepository;

  @Autowired
  private JobExplorer jobExplorer;

  @Autowired
  private FlowStep flowStep;

  @Test
  void getStepExecution() throws JobExecutionException {

    // Prepare the jobRepository for the test
    var jobParameters = new JobParameters();
    var jobInstance = this.jobRepository.createJobInstance("myJob", jobParameters);
    var jobExecution = this.jobRepository.createJobExecution(jobInstance, jobParameters, new ExecutionContext());
    var stepExecution = this.jobRepository.createStepExecution("flowStep", jobExecution);

    // Executed on the remote end in remote partitioning use case
    StepExecution jobExplorerStepExecution = this.jobExplorer.getStepExecution(jobExecution.getId(), stepExecution.getId());
    this.flowStep.execute(jobExplorerStepExecution);

    assertEquals(BatchStatus.COMPLETED, jobExplorerStepExecution.getStatus());
  }

}
