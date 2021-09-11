package com.github.marschall.spring.batch.nulldatasource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType.H2;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;

class NullDataSourceTests {

  // avoid recreating H2DataSource
  private static DataSource h2DataSource;

  @BeforeAll
  static void startH2() {
    h2DataSource = h2DataSource();
  }

  static List<DataSource> dataSources() {
    return List.of(h2DataSource, new NullDataSource());
  }

  @ParameterizedTest
  @MethodSource("dataSources")
  void nullMetadata(DataSource dataSource) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {

      DatabaseMetaData metaData = connection.getMetaData();
      assertNotNull(metaData);
      assertTrue(metaData.isWrapperFor(DatabaseMetaData.class));
      assertSame(metaData, metaData.unwrap(DatabaseMetaData.class));

      // assertNull(metaData.getUserName());
      // jdbc:h2:mem:66258a27-dbee-4b11-a551-9f035ed12d6c
      assertNotNull(metaData.getURL());
      assertNotNull(metaData.getDriverName());
      assertNotNull(metaData.getDriverVersion());
      assertNotNull(metaData.getDatabaseProductName());
      assertNotNull(metaData.getDatabaseProductVersion());
    }
  }

  @ParameterizedTest
  @MethodSource("dataSources")
  void nullConnection(DataSource dataSource) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {

      assertTrue(connection.isValid(1));
      assertTrue(connection.isValid(0));

      assertTrue(connection.isWrapperFor(Connection.class));
      assertSame(connection, connection.unwrap(Connection.class));

      assertFalse(connection.isClosed());

      assertTrue(connection.getAutoCommit());
      assertFalse(connection.isReadOnly());
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, connection.getHoldability());

      assertNotNull(connection.nativeSQL("SELECT 1 FROM dual WHERE 1 = 2"));
      assertNotNull(connection.getClientInfo());

      assertNull(connection.getWarnings());

      //  assertNull(connection.getCatalog());
      //  assertEquals(Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());

      Map<String, Class<?>> typeMap = connection.getTypeMap();
      if (typeMap != null) {
        // is null for H"
        assertTrue(typeMap.isEmpty());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("dataSources")
  void nullStatement(DataSource dataSource) throws SQLException {
    try (Connection connection = dataSource.getConnection();
         Statement statement = connection.createStatement()) {

      assertTrue(statement.isWrapperFor(Statement.class));
      assertFalse(statement.isWrapperFor(PreparedStatement.class));
      assertSame(statement, statement.unwrap(Statement.class));

      assertFalse(statement.isClosed());

      try (ResultSet resultSet = statement.executeQuery("SELECT 1 FROM dual WHERE 1 = 2")) {
        assertNotNull(resultSet);
        assertTrue(resultSet.isWrapperFor(ResultSet.class));
        assertSame(resultSet, resultSet.unwrap(ResultSet.class));
        assertFalse(resultSet.next());
      }
    }
  }

  @ParameterizedTest
  @MethodSource("dataSources")
  void nullPreparedStatement(DataSource dataSource) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1 FROM dual WHERE 1 = 2")) {

      assertTrue(preparedStatement.isWrapperFor(Statement.class));
      assertTrue(preparedStatement.isWrapperFor(PreparedStatement.class));
      assertSame(preparedStatement, preparedStatement.unwrap(Statement.class));
      assertSame(preparedStatement, preparedStatement.unwrap(PreparedStatement.class));

      assertFalse(preparedStatement.isClosed());

      assertEquals(ResultSet.TYPE_FORWARD_ONLY, preparedStatement.getResultSetType());
      assertEquals(ResultSet.CONCUR_READ_ONLY, preparedStatement.getResultSetConcurrency());
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, preparedStatement.getResultSetHoldability());
      assertEquals(ResultSet.FETCH_FORWARD, preparedStatement.getFetchDirection());
      assertEquals(0, preparedStatement.getQueryTimeout());
      assertEquals(100, preparedStatement.getFetchSize());

      assertThrows(SQLException.class, () -> preparedStatement.executeQuery("SELECT 1 FROM dual WHERE 1 = 2"));
    }
  }

  @ParameterizedTest
  @MethodSource("dataSources")
  void nullPreparedStatementUpdate(DataSource dataSource) throws SQLException {
    try (Connection connection = dataSource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement("UPDATE batch_test SET id = ? WHERE id < 0")) {

      preparedStatement.setLong(1, 1L);
      assertEquals(0, preparedStatement.executeUpdate());
      preparedStatement.setLong(1, 2L);
      assertEquals(0L, preparedStatement.executeLargeUpdate());

      assertThrows(SQLException.class, () -> preparedStatement.executeUpdate("UPDATE batch_test SET id = id + 1 WHERE id < 0"));
      assertThrows(SQLException.class, () -> preparedStatement.executeLargeUpdate("UPDATE batch_test SET id = id + 1 WHERE id < 0"));

      preparedStatement.setLong(1, 3L);
      preparedStatement.clearBatch();

      preparedStatement.setLong(1, 4L);
      preparedStatement.addBatch();

      assertArrayEquals(new int[] {0}, preparedStatement.executeBatch());

      preparedStatement.setLong(1, 5L);
      preparedStatement.addBatch();

      assertArrayEquals(new long[] {0L}, preparedStatement.executeLargeBatch());
    }
  }
  
  @ParameterizedTest
  @MethodSource("dataSources")
  void nullStatementUpdate(DataSource dataSource) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {

      assertEquals(0, statement.executeUpdate("UPDATE batch_test SET id = id + 1 WHERE id < 0"));
      assertEquals(0L, statement.executeLargeUpdate("UPDATE batch_test SET id = id + 1 WHERE id < 0"));
    }
  }

  @ParameterizedTest
  @MethodSource("dataSources")
  void emptyResultSet(DataSource dataSource) throws SQLException {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1 FROM dual WHERE 1 = 2");
        ResultSet resultSet = preparedStatement.executeQuery()) {

      assertTrue(resultSet.isWrapperFor(ResultSet.class));
      assertSame(resultSet, resultSet.unwrap(ResultSet.class));

      assertFalse(resultSet.isClosed());

      assertEquals(ResultSet.TYPE_FORWARD_ONLY, resultSet.getType());
      assertEquals(ResultSet.CONCUR_READ_ONLY, resultSet.getConcurrency());
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, resultSet.getHoldability());
      assertTrue(resultSet.getFetchSize() == preparedStatement.getFetchSize() || resultSet.getFetchSize() == 0);
      assertEquals(ResultSet.FETCH_FORWARD, resultSet.getFetchDirection());

      assertFalse(resultSet.isBeforeFirst());
      assertFalse(resultSet.isFirst());
      assertFalse(resultSet.isLast());
      assertFalse(resultSet.isAfterLast());

      assertFalse(resultSet.next());

      assertFalse(resultSet.isBeforeFirst());
      assertFalse(resultSet.isFirst());
      assertFalse(resultSet.isLast());
      assertFalse(resultSet.isAfterLast());
    }
  }

  @Test
  void removeJobExecutions() {
    JobRepositoryTestUtils testUtils = new JobRepositoryTestUtils();
    testUtils.setDataSource(new NullDataSource());
    testUtils.removeJobExecutions();
  }

  private static DataSource h2DataSource() {
    return new EmbeddedDatabaseBuilder()
        .generateUniqueName(true)
        .addScript("classpath:sql/h2-schema.sql")
        .setType(H2)
        .build();
  }

}
