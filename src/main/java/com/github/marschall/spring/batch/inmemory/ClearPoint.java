package com.github.marschall.spring.batch.inmemory;

/**
 * Time points when a {@link InMemoryJobStorage} is cleared.
 *
 * @see ClearJobRepository
 */
public enum ClearPoint {

  AFTER_TEST,

  AFTER_CLASS;

}
