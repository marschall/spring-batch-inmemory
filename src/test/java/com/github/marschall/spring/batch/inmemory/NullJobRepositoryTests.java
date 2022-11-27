package com.github.marschall.spring.batch.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.repository.JobRepository;

class NullJobRepositoryTests {

  private JobRepository jobRepository;

  @BeforeEach
  void setUp() {
    this.jobRepository = new NullJobRepository();
  }

  @Test
  void getJobNames() {
    List<String> jobNames = this.jobRepository.getJobNames();
    assertNotNull(jobNames, "jobNames");
    assertEquals(List.of(), jobNames);
  }

}
