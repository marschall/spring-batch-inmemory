package com.github.marschall.spring.batch.inmemory;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcOperations;

public class InsertTasklet implements Tasklet {

  private final JdbcOperations jdbcOperations;

  public InsertTasklet(JdbcOperations jdbcOperations) {
    this.jdbcOperations = jdbcOperations;
  }

  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
    this.jdbcOperations.update("INSERT INTO BATCH_TEST() VALUES ()");
    return RepeatStatus.FINISHED;
  }

}
