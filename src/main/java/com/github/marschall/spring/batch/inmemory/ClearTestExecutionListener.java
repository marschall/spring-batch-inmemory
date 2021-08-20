package com.github.marschall.spring.batch.inmemory;

import java.lang.invoke.MethodHandles;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;

/**
 * Clears a {@link InMemoryJobStorage} when so instructed by the presence of a {@link ClearJobRepository}
 * annotation. Will get automatically added to tests by the Spring TestContext Framework.
 */
public class ClearTestExecutionListener extends AbstractTestExecutionListener {

  private static final Log LOGGER = LogFactory.getLog(MethodHandles.lookup().lookupClass());

  @Override
  public int getOrder() {
    return 10_000;
  }

  private static boolean hasClearAnnotation(TestContext testContext, ClearPoint when) {
    Class<?> testClass = testContext.getTestClass();
    ClearJobRepository clearJobRepository = AnnotationUtils.findAnnotation(testClass, ClearJobRepository.class);
    if (clearJobRepository != null) {
      return clearJobRepository.value() == ClearPoint.AFTER_TEST;
    }
    return false;
  }

  private void clearJobStorage(TestContext testContext) {
    ApplicationContext applicationContext = testContext.getApplicationContext();
    InMemoryJobStorage jobStorage;
    try {
      jobStorage = applicationContext.getBean(InMemoryJobStorage.class);
    } catch (NoSuchBeanDefinitionException e) {
      LOGGER.warn("not bean of type: " + InMemoryJobStorage.class + " found, not clearing", e);
      return;
    }
    jobStorage.clear();
  }

  @Override
  public void afterTestExecution(TestContext testContext) {
    if (hasClearAnnotation(testContext, ClearPoint.AFTER_TEST)) {
      this.clearJobStorage(testContext);
    }
  }

  @Override
  public void afterTestClass(TestContext testContext) {
    if (hasClearAnnotation(testContext, ClearPoint.AFTER_CLASS)) {
      this.clearJobStorage(testContext);
    }
  }

}
