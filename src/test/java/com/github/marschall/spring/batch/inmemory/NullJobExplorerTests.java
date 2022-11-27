package com.github.marschall.spring.batch.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.explore.JobExplorer;

class NullJobExplorerTests {

  private JobExplorer jobExplorer;

  @BeforeEach
  void setUp() {
    this.jobExplorer = new NullJobExplorer();
  }

  @Test
  void getJobNames() {
    List<String> jobNames = this.jobExplorer.getJobNames();
    assertNotNull(jobNames, "jobNames");
    assertEquals(List.of(), jobNames);
  }

  @Test
  void getLastJobInstance() {
    JobInstance lastJobInstance = this.jobExplorer.getLastJobInstance("transaction-processing");
    assertNull(lastJobInstance, "lastJobInstance");
  }
  
  @Test
  void getJobInstances() {
    List<JobInstance> jobInstances = this.jobExplorer.getJobInstances("transaction-processing", 0, 1);
    assertNotNull(jobInstances, "jobInstances");
    assertEquals(List.of(), jobInstances);
  }

}
