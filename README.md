Spring Batch In-Memory
======================

Alternative implementations of the Spring Batch `JobRepository` and `JobExplorer` interfaces that don't require committing JDBC transactions. This allows you to roll back all DML operations in integration tests.

We offer two different implementations:

- Null implementations that do not save any data and therefore do not require any cleanup. This is ideal for integration tests where you don't want to have to clean up previous job executions.
- In memory implementations that save all data in memory. These are intended as replacements for the deprecated the map based DAO implementations (`MapJobInstanceDao`, `MapJobExecutionDao`, `MapStepExecutionDao` and `MapExecutionContextDao`). These require clean up of previous job executions.

We also offer a `NullDataSource` that allows running integration tests without a database at all.

```xml
<dependency>
  <groupId>com.github.marschall</groupId>
  <artifactId>spring-batch-inmemory</artifactId>
  <version>1.0.0</version>
</dependency>
```

Compared to `MapJobRepositoryFactoryBean`:

- our implemenations allow rolling back transactions in integration tests
- our implemenations should perform much better in situations of jobs with many steps
- our `JobRepository` and `JobExplorer` implementations are not deprecated and work with Spring Batch 5


Configuration
-------------

There are two ways this project can be used, either through `SimpleBatchConfiguration` and `BatchConfigurer` or through `NullBatchConfiguration`/`InMemoryBatchConfiguration`.

### SimpleBatchConfiguration and BatchConfigurer


We offer the `NullBatchConfigurer` and `InMemoryBatchConfigurer` implementations of `BatchConfigurer` that can be used together with `SimpleBatchConfiguration`.

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
      // you can also use NullBatchConfigurer here
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

### InMemoryBatchConfiguration and NullBatchConfiguration

In addition we offer `NullBatchConfiguration` and `InMemoryBatchConfiguration` which completley replace `SimpleBatchConfiguration` and `BatchConfigurer`.

```java
@Transactional // only needed of you have JDBC DML operations that you want to rollback
@Rollback // only needed of you have JDBC DML operations that you want to rollback
@SpringBatchTest
class MySpringBatchIntegrationTests {

  @Configuration
  @Import({
    MyJobConfiguration.class, // the configuration class of the Spring Batch job or step you want to test
    InMemoryBatchConfiguration.class // or use NullBatchConfiguration
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

Clearing the Repository
-----------------------

If you want to automatically clear the job repository of job instances and executions after a test you can use `InMemoryBatchConfiguration` together with `@ClearJobRepository`.

```java
@SpringBatchTest
@ClearJobRepository(AFTER_TEST)
class MySpringBatchIntegrationTests {

}
```

If you're using `NullBatchConfigurer` or `NullBatchConfiguration` then there is no need for clearing as nothing is stored.

Limitations
-----------

- Objects in a `ExecutionContext` are not copied. You should therefore refrain from mutating objects in a `ExecutionContext` but rather replace them with updated ones.


Implementation Notes
--------------------

- As this is intended for testing purposes the implementation is based on `HashMap`s rather than `ConcurrentHashMap` in order to save memory. Thread safety is instead provided through a `ReadWriteLock`.
- The `JobRepository` and `JobExplorer` implementations offer consistent views for single method calls.
- Copying of contexts (`ExecutionContext`, `JobExecution` and `StepExecution`) is implemented through copy constructors instead of serialization and deserialization which should result in small efficiency gains.
- Compared to the map based implementations (`MapJobInstanceDao`, `MapJobExecutionDao`, `MapStepExecutionDao` and `MapExecutionContextDao`) you should see reductions if memory footprint as we store each object (`JobExecution`, `StepExecution`, `ExecutionContext`) only once. This should be especially noticeable if you have a `JobExecution` with many `StepExecutions`.
