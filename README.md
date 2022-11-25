Spring Batch In-Memory
======================

Alternative implementations of the Spring Batch `JobRepository` and `JobExplorer` interfaces that don't require committing JDBC transactions. This allows you to roll back all DML operations in integration tests.

We offer two different implementations:

- Null implementations that do not save any data and therefore do not require any cleanup. This is ideal for integration tests where you don't want to have to clean up previous job executions.
- In memory implementations that save all data in memory. These are intended as replacements for the Jdbc based DAO implementations (`JdbcJobInstanceDao`, `JdbcJobExecutionDao`, `JdbcStepExecutionDao` and `JdbcExecutionContextDao`). These require clean up of previous job executions.

```xml
<dependency>
  <groupId>com.github.marschall</groupId>
  <artifactId>spring-batch-inmemory</artifactId>
  <version>2.0.0</version>
</dependency>
```

Compared to `MapJobRepositoryFactoryBean`:

- our implementations allow rolling back transactions in integration tests
- our implementations should perform much better in situations of jobs with many steps
- our `JobRepository` and `JobExplorer` implementations are not deprecated and work with Spring Batch 5

Versions 1.x of this project support Spring Batch 4.x.

Configuration
-------------

We offer two predefined configuration classes (`NullBatchConfiguration` and `InMemoryBatchConfiguration`) for easy setup.

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
    public PlatformTransactionManager txManager() {
      // if your job requires data access through JDBC replace this with DataSourceTransactionManager
      // to roll back JDBC DML operations 
      return new ResourcelessTransactionManager();
    }

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
