package com.github.marschall.spring.batch.inmemory;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

final class NullConnection implements Connection {

  private static final Set<Integer> RESULT_SET_TYPES = Set.of(ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE);

  private static final Set<Integer> RESULT_SET_CONCURRENCIES = Set.of(ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE);
  
  private static final Set<Integer> HOLDABILITIES = Set.of(ResultSet.HOLD_CURSORS_OVER_COMMIT, ResultSet.CLOSE_CURSORS_AT_COMMIT);

  private final String username;

  private final List<Statement> closeables;

  private boolean closed;

  private boolean autoCommit;

  private boolean readOnly;

  private String schema;

  private Properties properties;

  private Map<String, Class<?>> typeMap;

  private String catalog;

  private int holdability;

  NullConnection() {
    this(null);
  }

  NullConnection(String username) {
    this.username = username;
    this.closed = false;
    this.readOnly = false;
    this.autoCommit = true;
    this.holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    this.properties = new Properties();
    this.typeMap = new HashMap<>();
    this.closeables = new ArrayList<>();
  }

  void closedCheck() throws SQLException {
    if (this.closed) {
      throw new SQLException("closed result set");
    }
  }

  private <S extends Statement> S addCloseable(S closable) {
    this.closeables.add(closable);
    return closable;
  }

  void removeCloseable(Statement closable) {
    this.closeables.remove(closable);
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    this.closedCheck();
    if (iface == Connection.class) {
      return iface.cast(this);
    } else {
      throw new SQLException("unsupported interface: " + iface);
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    this.closedCheck();
    return iface == Connection.class;
  }

  @Override
  public Statement createStatement() throws SQLException {
    this.closedCheck();
    return this.addCloseable(new NullStatement(this, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, this.holdability));
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    this.closedCheck();
    return this.addCloseable(new NullPreparedStatement(this, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, this.holdability));
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    this.closedCheck();
    return this.addCloseable(new NullCallableStatement(this, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, this.holdability));
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    this.closedCheck();
    return sql;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    this.closedCheck();
    this.autoCommit = autoCommit;
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    this.closedCheck();
    return this.autoCommit;
  }

  @Override
  public void commit() throws SQLException {
    this.closedCheck();
  }

  @Override
  public void rollback() throws SQLException {
    this.closedCheck();
  }

  @Override
  public void close() throws SQLException {
    this.closed = true;
    // copy because #close will trigger modification
    List<Statement> toClose = new ArrayList<>(this.closeables);
    for (Statement statement : toClose) {
      statement.close();
    }
  }

  @Override
  public boolean isClosed() throws SQLException {
    return this.closed;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new NullDatabaseMetaData(this.username, this);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    this.closedCheck();
    this.readOnly = readOnly;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    this.closedCheck();
    return this.readOnly;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    this.closedCheck();
    this.catalog = catalog;
  }

  @Override
  public String getCatalog() throws SQLException {
    this.closedCheck();
    return this.catalog;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    // TODO Auto-generated method stub
    this.closedCheck();
    return Connection.TRANSACTION_NONE;
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
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    this.closedCheck();
    if (!RESULT_SET_TYPES.contains(resultSetType)) {
      throw new SQLException("unsupported result set type: " + resultSetType);
    }
    if (!RESULT_SET_CONCURRENCIES.contains(resultSetConcurrency)) {
      throw new SQLException("unsupported result set concurrency: " + resultSetConcurrency);
    }
    return this.addCloseable(new NullStatement(this, resultSetType, resultSetConcurrency, this.holdability));
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    this.closedCheck();
    return new HashMap<>(this.typeMap);
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    this.closedCheck();
    this.typeMap.clear();
    if (map != null) {
      this.typeMap.putAll(map);
    }
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    this.closedCheck();
    if (!HOLDABILITIES.contains(holdability)) {
      throw new SQLException("unsupported holdability: " + holdability);
    }
    this.holdability = holdability;
  }

  @Override
  public int getHoldability() throws SQLException {
    this.closedCheck();
    return this.holdability;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency,
          int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Clob createClob() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    if (timeout < 0) {
      throw new SQLException("negative timeout");
    }
    return !this.closed;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    try {
      this.closedCheck();
    } catch (SQLException e) {
      throw new SQLClientInfoException();
    }
    if (value != null) {
      this.properties.setProperty(name, value);
    } else {
      this.properties.remove(name);
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    try {
      this.closedCheck();
    } catch (SQLException e) {
      throw new SQLClientInfoException();
    }
    Objects.requireNonNull(properties, "properties");
    this.properties.clear();
    this.properties.putAll(properties);
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    this.closedCheck();
    return this.properties.getProperty(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    this.closedCheck();
    return (Properties) this.properties.clone();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    this.closedCheck();
    this.schema = schema;
  }

  @Override
  public String getSchema() throws SQLException {
    this.closedCheck();
    return this.schema;
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void beginRequest() throws SQLException {
    // TODO Auto-generated method stub
    Connection.super.beginRequest();
  }

  @Override
  public void endRequest() throws SQLException {
    // TODO Auto-generated method stub
    Connection.super.endRequest();
  }

}
