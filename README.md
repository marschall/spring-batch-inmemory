Spring Batch In-Memory
======================

In memory implementations of the Spring Batch `JobRepository` and `JobExplorer` interfaces as the map based DAO implementations (`MapJobInstanceDao`, `MapJobExecutionDao`, `MapStepExecutionDao` and `MapExecutionContextDao`) are deprecated.

This project allows to run Spring Batch integration tests either without JDBC or with rolling back all DML operations.

```xml
<dependency>
  <groupId>com.github.marschall</groupId>
  <artifactId>spring-batch-inmemory</artifactId>
  <version>0.2.0</version>
</dependency>
```

There are two ways this project can be used, either through `SimpleBatchConfiguration` and `BatchConfigurer` or through `InMemoryBatchConfiguration`.

SimpleBatchConfiguration and BatchConfigurer
--------------------------------------------

```java
@Transactional // only needed of you have JDBC DML operations that you want to rollback
@Rollback // only needed of you have JDBC DML operations that you want to rollback
@SpringBatchTest
class MySpringBatchIntegrationTests {

  @Configuration
  @EnableBatchProcessing
  @Import({
    MyJobConfiguration.class, // the configuration class of the Spring Batch job or step you want to test
    SimpleBatchConfiguration.class
  })
  static class ContextConfiguration {

    @Bean
    BatchConfigurer batchConfigurer() {
      return new InMemoryBatchConfigurer(this.txManager());
    }

    @Bean
    public DataSource dataSource() {
      // if your job requires data access through JDBC replace this with the actual DataSource
      return new NullDataSource();
    }

    @Bean
    public PlatformTransactionManager txManager() {
      // if your job requires data access through JDBC replace this with the appropriate transaction manager, eg. DataSourceTransactionManager
      return new ResourcelessTransactionManager();
    }

  }

}
```

InMemoryBatchConfiguration
--------------------------

`InMemoryBatchConfiguration` replaces `SimpleBatchConfiguration` and `BatchConfigurer`.


```java
@Transactional // only needed of you have JDBC DML operations that you want to rollback
@Rollback // only needed of you have JDBC DML operations that you want to rollback
@SpringBatchTest
class MySpringBatchIntegrationTests {

  @Configuration
  @Import({
    MyJobConfiguration.class, // the configuration class of the Spring Batch job or step you want to test
    InMemoryBatchConfiguration.class
  })
  static class ContextConfiguration {

    @Bean
    public DataSource dataSource() {
      // if your job requires data access through JDBC replace this with the actual DataSource
      return new NullDataSource();
    }

    // if you want to used to roll back JDBC DML operations you also need to define an appropriate transaction manager, eg. DataSourceTransactionManager

  }

}
```


Implementation Notes
--------------------

- As this is intended for testing purposes the implementation is based on `HashMap`s rather than `ConcurrentHashMap` in order to save memory. Thread safety is instead provided through a `ReadWriteLock`.
- There is no transaction isolation so the behavior is similar to `read uncommitted`.
- Copying of contexts (`ExecutionContext`, `JobExecution` and `StepExecution`) is implemented through copy constructors instead of serialization and deserialization which should result in small efficiency gains.

