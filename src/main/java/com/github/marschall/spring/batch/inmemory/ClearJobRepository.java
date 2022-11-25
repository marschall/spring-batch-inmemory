package com.github.marschall.spring.batch.inmemory;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

/**
 * Annotation to be put on a test to cause a {@link InMemoryJobRepository} to be cleared
 * after test execution.
 * <p>
 * A bean of type {@link InMemoryJobStorage} needs to be present, {@link InMemoryBatchConfiguration}
 * exposes such a bean.
 *
 * @see InMemoryJobStorage#clear()
 */
@Inherited
@Retention(RUNTIME)
@Target(TYPE)
public @interface ClearJobRepository {

  /**
   * Determines when the {@link InMemoryJobStorage} should be cleared.
   *
   * @return when the {@link InMemoryJobStorage} should be cleared
   */
  ClearPoint value();

}
