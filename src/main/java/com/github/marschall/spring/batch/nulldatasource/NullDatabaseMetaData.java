package com.github.marschall.spring.batch.nulldatasource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

final class NullDatabaseMetaData implements DatabaseMetaData {

  private final String username;
  private final Connection connection;

  NullDatabaseMetaData(String username, Connection connection) {
    this.username = username;
    this.connection = connection;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == DatabaseMetaData.class) {
      return iface.cast(this);
    } else {
      throw new SQLException("unsupported interface: " + iface);
    }
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return iface == DatabaseMetaData.class;
  }

  @Override
  public boolean allProceduresAreCallable() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean allTablesAreSelectable() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getURL() throws SQLException {
    return "jdbc:null";
  }

  @Override
  public String getUserName() throws SQLException {
    return this.username;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getDatabaseProductName() throws SQLException {
    return "Null Database";
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    return "1.0";
  }

  @Override
  public String getDriverName() throws SQLException {
    return "Null Driver";
  }

  @Override
  public String getDriverVersion() throws SQLException {
    return "1.0";
  }

  @Override
  public int getDriverMajorVersion() {
    return 1;
  }

  @Override
  public int getDriverMinorVersion() {
    return 0;
  }

  @Override
  public boolean usesLocalFiles() {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() {
    return false;
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getStringFunctions() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsColumnAliasing() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean nullPlusNonNullIsNull() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsConvert() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsOrderByUnrelated() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsGroupBy() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsGroupByUnrelated() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsLikeEscapeClause() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsMultipleResultSets() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsCoreSQLGrammar() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsOuterJoins() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsFullOuterJoins() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsLimitedOuterJoins() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isCatalogAtStart() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getCatalogSeparator() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsPositionedDelete() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsSubqueriesInExists() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsSubqueriesInIns() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsUnion() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsUnionAll() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxConnections() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int getMaxStatementLength() {
    return 0;
  }

  @Override
  public int getMaxStatements() {
    return 0;
  }

  @Override
  public int getMaxTableNameLength() {
    return 0;
  }

  @Override
  public int getMaxTablesInSelect() {
    return 0;
  }

  @Override
  public int getMaxUserNameLength() {
    return 0;
  }

  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean supportsTransactions() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level)
          throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions()
          throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly()
          throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getSchemas() {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getCatalogs() {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getTableTypes() {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getTypeInfo() {
    return new EmptyResultSet();
  }

  @Override
  public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) {
    return new EmptyResultSet();
  }

  @Override
  public boolean supportsResultSetType(int type) {
    return NullConnection.RESULT_SET_TYPES.contains(type);
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency)
          throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsBatchUpdates() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) {
    return new EmptyResultSet();
  }

  @Override
  public Connection getConnection() throws SQLException {
    return this.connection;
  }

  @Override
  public boolean supportsSavepoints() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsNamedParameters() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern,
          String typeNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern,
          String tableNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultSet getAttributes(String catalog, String schemaPattern,
          String typeNamePattern, String attributeNamePattern)
          throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability)
          throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getJDBCMajorVersion() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getJDBCMinorVersion() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getSQLStateType() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean locatorsUpdateCopy() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean supportsStatementPooling() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern)
          throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern,
          String functionNamePattern) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultSet getFunctionColumns(String catalog, String schemaPattern,
          String functionNamePattern, String columnNamePattern)
          throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ResultSet getPseudoColumns(String catalog, String schemaPattern,
          String tableNamePattern, String columnNamePattern)
          throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean generatedKeyAlwaysReturned() throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

}
