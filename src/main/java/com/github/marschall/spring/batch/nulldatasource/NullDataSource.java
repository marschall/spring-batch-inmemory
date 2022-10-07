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
 * @deprecated No longer needed as of Spring Batch 5 as JobRepositoryTestUtils no longer requires a {@link DataSource}
 */
@Deprecated
public final class NullDataSource implements DataSource {

  private volatile int loginTimeout = 0;
  private volatile PrintWriter logWriter;

  /**
   * Constructs a new {@link NullDataSource}.
   */
  public NullDataSource() {
    super();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return Logger.getLogger("");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == DataSource.class) {
      return iface.cast(this);
    }
    throw new SQLException("unsupported interface: " + iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface == DataSource.class;
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
    return this.logWriter;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    this.logWriter = out;
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
