package com.github.marschall.spring.batch.nulldatasource;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.ConnectionBuilder;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

/**
 * A {@link DataSource} that doesn't perform any database access.
 * <p>
 * Intended for use with JobRepositoryTestUtils which requires a {@link DataSource},
 * this is a work around for <a href="https://github.com/spring-projects/spring-batch/issues/3767">#3767</a>.
 * <p>
 * Instances of this class are thread safe but created objects are not.
 */
public final class NullDataSource implements DataSource {

  private volatile int loginTimeout = 0;

  public NullDataSource() {
    super();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return new NullConnection();
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return new NullConnection(username);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    if (seconds <= 0) {
      throw new IllegalArgumentException();
    }
    this.loginTimeout = seconds;
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return this.loginTimeout;
  }

  @Override
  public ConnectionBuilder createConnectionBuilder() throws SQLException {
    return new NullConnectionBuilder();
  }

}
