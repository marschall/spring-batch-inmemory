package com.github.marschall.spring.batch.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.sql.DataSource;

import org.junit.jupiter.api.RepeatedTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

import com.github.marschall.spring.batch.inmemory.configuration.H2Configuration;
import com.github.marschall.spring.batch.inmemory.configuration.InsertingJobConfiguration;

class JdbcRollbackTests {

  @Autowired
  private JdbcOperations jdbcOperations;

  @RepeatedTest(value = 10)
  void insert() {
    assertEquals(0, this.countRows());
  }

  private int countRows() {
    return this.jdbcOperations.queryForObject("SELECT COUNT(*) FROM BATCH_TEST", Integer.class);
  }

  @Configuration
  @Import({InsertingJobConfiguration.class, H2Configuration.class})
  static class ContextConfiguration {

    @Autowired
    private DataSource dataSource;

    @Bean
    public JdbcOperations jdbcOperations() {
      return new JdbcTemplate(this.dataSource);
    }

  }

}
