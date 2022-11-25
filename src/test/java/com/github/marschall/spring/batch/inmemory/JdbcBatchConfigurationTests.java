package com.github.marschall.spring.batch.inmemory;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import com.github.marschall.spring.batch.inmemory.configuration.H2Configuration;
import com.github.marschall.spring.batch.inmemory.configuration.JdbcInsertingJobConfiguration;

class JdbcBatchConfigurationTests extends AbstractJdbcTests {

  @Configuration
  @Import({
    H2Configuration.class,
    JdbcInsertingJobConfiguration.class,
    InMemoryBatchConfiguration.class
  })
  static class ContextConfiguration {

    @Autowired
    private DataSource dataSource;

    // used to roll back the inserts
    @Bean
    public PlatformTransactionManager transactionManager() {
      return new DataSourceTransactionManager(this.dataSource);
    }

    @Bean
    public JdbcOperations jdbcOperations() {
      return new JdbcTemplate(this.dataSource);
    }

  }

}
