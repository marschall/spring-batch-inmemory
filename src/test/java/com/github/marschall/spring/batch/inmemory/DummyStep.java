package com.github.marschall.spring.batch.inmemory;

import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;

public class DummyStep implements Step {

  private final String name;

  public DummyStep(String name) {
    this.name = name;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public void execute(StepExecution stepExecution) throws JobInterruptedException {
    System.out.println("EXECUTING " + this.getName());
  }

  @Override
  public int getStartLimit() {
    return 100;
  }

  @Override
  public boolean isAllowStartIfComplete() {
    return false;
  }
}