package com.github.marschall.spring.batch.inmemory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

class NullStatement implements Statement {

  private boolean closed;
  private final NullConnection connection;
  private final List<ResultSet> closeables;

  NullStatement(NullConnection connection) {
    super();
    this.connection = connection;
    this.closed = false;
    this.closeables = new ArrayList<>();
  }

  void closedCheck() throws SQLException {
    if (this.closed) {
      throw new SQLException("closed result set");
    }
  }

  ResultSet addCloseable(ResultSet closable) {
    this.closeables.add(closable);
    return closable;
  }

  void removeCloseable(ResultSet closable) {
    this.closeables.remove(closable);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    this.closedCheck();
    if (this.isWrapperFor(iface)) {
      return iface.cast(this);
    } else {
      throw new SQLException("unsupported interface: " + iface);
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    this.closedCheck();
    return iface == Statement.class;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void close() throws SQLException {
    this.connection.removeCloseable(this);
    // copy because #close will trigger modification
    List<ResultSet> toClose = new ArrayList<>(this.closeables);
    for (ResultSet resultSet : toClose) {
      resultSet.close();
    }
    this.closed = true;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getMaxRows() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getQueryTimeout() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void cancel() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setCursorName(String name) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean execute(String sql) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getFetchDirection() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getFetchSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getResultSetType() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void clearBatch() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int[] executeBatch() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Connection getConnection() throws SQLException {
    this.closedCheck();
    return this.connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys)
          throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes)
          throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames)
          throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys)
          throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isPoolable() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public long getLargeUpdateCount() throws SQLException {
    // TODO Auto-generated method stub
    return Statement.super.getLargeUpdateCount();
  }

  @Override
  public void setLargeMaxRows(long max) throws SQLException {
    // TODO Auto-generated method stub
    Statement.super.setLargeMaxRows(max);
  }

  @Override
  public long getLargeMaxRows() throws SQLException {
    // TODO Auto-generated method stub
    return Statement.super.getLargeMaxRows();
  }

  @Override
  public long[] executeLargeBatch() throws SQLException {
    // TODO Auto-generated method stub
    return Statement.super.executeLargeBatch();
  }

  @Override
  public long executeLargeUpdate(String sql) throws SQLException {
    // TODO Auto-generated method stub
    return Statement.super.executeLargeUpdate(sql);
  }

  @Override
  public long executeLargeUpdate(String sql, int autoGeneratedKeys)
          throws SQLException {
    // TODO Auto-generated method stub
    return Statement.super.executeLargeUpdate(sql, autoGeneratedKeys);
  }

  @Override
  public long executeLargeUpdate(String sql, int[] columnIndexes)
          throws SQLException {
    // TODO Auto-generated method stub
    return Statement.super.executeLargeUpdate(sql, columnIndexes);
  }

  @Override
  public long executeLargeUpdate(String sql, String[] columnNames)
          throws SQLException {
    // TODO Auto-generated method stub
    return Statement.super.executeLargeUpdate(sql, columnNames);
  }

  @Override
  public String enquoteLiteral(String val) throws SQLException {
    // TODO Auto-generated method stub
    return Statement.super.enquoteLiteral(val);
  }

  @Override
  public String enquoteIdentifier(String identifier, boolean alwaysQuote)
          throws SQLException {
    // TODO Auto-generated method stub
    return Statement.super.enquoteIdentifier(identifier, alwaysQuote);
  }

  @Override
  public boolean isSimpleIdentifier(String identifier) throws SQLException {
    // TODO Auto-generated method stub
    return Statement.super.isSimpleIdentifier(identifier);
  }

  @Override
  public String enquoteNCharLiteral(String val) throws SQLException {
    // TODO Auto-generated method stub
    return Statement.super.enquoteNCharLiteral(val);
  }



}
