package com.github.marschall.spring.batch.inmemory;

/**
 * Time points when a {@link InMemoryJobStorage} is cleared.
 *
 * @see ClearJobRepository
 */
public enum ClearPoint {

  /**
   * {@link InMemoryJobStorage} should be cleared after every test execution.
   */
  AFTER_TEST,

  /**
   * {@link InMemoryJobStorage} should be cleared after all tests in all tests in a class ran.
   */
  AFTER_CLASS;

}
