package com.github.marschall.spring.batch.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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

//      assertNull(metaData.getUserName());
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

//      assertNull(connection.getCatalog());
//      assertEquals(Connection.TRANSACTION_READ_COMMITTED, connection.getTransactionIsolation());

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
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT 1 FROM dual WHERE 1 = 2")) {

      assertTrue(preparedStatement.isWrapperFor(Statement.class));
      assertTrue(preparedStatement.isWrapperFor(PreparedStatement.class));
      assertSame(preparedStatement, preparedStatement.unwrap(Statement.class));
      assertSame(preparedStatement, preparedStatement.unwrap(PreparedStatement.class));

      assertFalse(preparedStatement.isClosed());

      assertEquals(ResultSet.TYPE_FORWARD_ONLY, preparedStatement.getResultSetType());
      assertEquals(ResultSet.CONCUR_READ_ONLY, preparedStatement.getResultSetConcurrency());
      assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, preparedStatement.getResultSetHoldability());
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
        .setType(H2)
        .build();
  }

}
