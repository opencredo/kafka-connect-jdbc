/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import io.confluent.connect.jdbc.sink.metadata.SinkRecordField;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.IdentifierRules;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link DatabaseDialect} for IBM DB2.
 */
public class SpannerDatabaseDialect extends GenericDatabaseDialect {

  /**
   * The provider for {@link SpannerDatabaseDialect}.
   */
  public static class Provider extends SubprotocolBasedProvider {
    public Provider() {
      super(SpannerDatabaseDialect.class.getSimpleName(), "cloudspanner");
    }

    @Override
    public DatabaseDialect create(AbstractConfig config) {
      return new SpannerDatabaseDialect(config);
    }
  }

  /**
   * Create a new dialect instance with the given connector configuration.
   *
   * @param config the connector configuration; may not be null
   */
  public SpannerDatabaseDialect(AbstractConfig config) {
    super(config, new IdentifierRules(".", "\"", "\""));
  }

  @Override
  public boolean tableExists(
      Connection connection,
      TableId tableId
  ) throws SQLException {
    log.info("Checking {} dialect for existence of table {}", this, tableId);
    Boolean autoCommit = connection.getAutoCommit();
    if (!autoCommit) {
      log.debug("Setting Autocommit to true when was {}", connection.getAutoCommit());
      connection.setAutoCommit(true);
    }
    try (
        ResultSet rs2 = connection.createStatement().executeQuery("SET TRANSACTION READ ONLY");
        ResultSet rs = connection.getMetaData().getTables(
                tableId.catalogName(),
                tableId.schemaName(),
                tableId.tableName(),
                new String[]{"TABLE"}
            );
        ResultSet rs3 = connection.createStatement().executeQuery("SET TRANSACTION READ WRITE");
    ) {
      final boolean exists = rs.next();
      log.info("Using {} dialect table {} {}", this, tableId, exists ? "present" : "absent");
      return exists;
    } finally {
      log.debug("Now Setting Autocommit to {} when was true", autoCommit);
      connection.setAutoCommit(autoCommit);
    }
  }

  @Override
  public void applyDdlStatements(
       Connection connection,
       List<String> statements
  ) throws SQLException {
    Boolean autoCommit = connection.getAutoCommit();
    if (!autoCommit) {
      log.debug("Setting Autocommit to true when was {}", connection.getAutoCommit());
      connection.setAutoCommit(true);
    }
    try (Statement statement = connection.createStatement()) {
      for (String ddlStatement : statements) {
        statement.executeUpdate(ddlStatement);
      }
    } finally {
      log.debug("Now Setting Autocommit to {} when was true", autoCommit);
      connection.setAutoCommit(autoCommit);
    }

  }

  @Override
  public Map<ColumnId, ColumnDefinition> describeColumns(
      Connection connection,
      String catalogPattern,
      String schemaPattern,
      String tablePattern,
      String columnPattern
  ) throws SQLException {
    log.info(
        "Querying {} dialect column metadata for catalog:{} schema:{} table:{}",
        this,
        catalogPattern,
        schemaPattern,
        tablePattern
    );

    // Get the primary keys of the table(s) ...
    final Set<ColumnId> pkColumns = primaryKeyColumns(
        connection,
        catalogPattern,
        schemaPattern,
        tablePattern
    );
    Map<ColumnId, ColumnDefinition> results = new HashMap<>();
    Boolean autoCommit = connection.getAutoCommit();
    if (!autoCommit) {
      log.debug("Setting Autocommit to true when was {}", connection.getAutoCommit());
      connection.setAutoCommit(true);
    }
    try (
        ResultSet rs2 = connection.createStatement().executeQuery("SET TRANSACTION READ ONLY");
        ResultSet rs = connection.getMetaData().getColumns(
                catalogPattern, schemaPattern, tablePattern, columnPattern);
        ResultSet rs3 = connection.createStatement().executeQuery("SET TRANSACTION READ WRITE");
    ) {
      final int rsColumnCount = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        describeColumnFromColumnMetaData(pkColumns, results, rs, rsColumnCount);
      }
      log.debug("Column definitions {}",results);
      return results;
    } finally {
      log.debug("Now Setting Autocommit to {} when was true", autoCommit);
      connection.setAutoCommit(autoCommit);
    }
  }

  private void describeColumnFromColumnMetaData(
       Set<ColumnId> pkColumns,
       Map<ColumnId,
       ColumnDefinition> results,
       ResultSet columnMetaData,
       int rsColumnCount
  ) throws SQLException {
    final String catalogName = columnMetaData.getString(1);
    final String schemaName = columnMetaData.getString(2);
    final String tableName = columnMetaData.getString(3);
    final TableId tableId = new TableId(catalogName, schemaName, tableName);
    final String columnName = columnMetaData.getString(4);
    final ColumnId columnId = new ColumnId(tableId, columnName, null);
    final int jdbcType = columnMetaData.getInt(5);
    final String typeName = columnMetaData.getString(6);
    final int precision = columnMetaData.getInt(7);
    int tempScale = 0;
    if (columnMetaData.getObject(9) != null) {
      tempScale = columnMetaData.getInt(9);
    }
    final int scale = tempScale;
    final String typeClassName = null;
    ColumnDefinition.Nullability nullability;
    final int nullableValue = columnMetaData.getInt(11);
    switch (nullableValue) {
      case DatabaseMetaData.columnNoNulls:
        nullability = ColumnDefinition.Nullability.NOT_NULL;
        break;
      case DatabaseMetaData.columnNullable:
        nullability = ColumnDefinition.Nullability.NULL;
        break;
      case DatabaseMetaData.columnNullableUnknown:
      default:
        nullability = ColumnDefinition.Nullability.UNKNOWN;
        break;
    }
    Boolean autoIncremented = null;
    if (rsColumnCount >= 23) {
      // Not all drivers include all columns ...
      String isAutoIncremented = columnMetaData.getString(23);
      if ("yes".equalsIgnoreCase(isAutoIncremented)) {
        autoIncremented = Boolean.TRUE;
      } else if ("no".equalsIgnoreCase(isAutoIncremented)) {
        autoIncremented = Boolean.FALSE;
      }
    }
    Boolean signed = null;
    Boolean caseSensitive = null;
    Boolean searchable = null;
    Boolean currency = null;
    Integer displaySize = null;
    boolean isPrimaryKey = pkColumns.contains(columnId);
    if (isPrimaryKey) {
      // Some DBMSes report pks as null
      nullability = ColumnDefinition.Nullability.NOT_NULL;
    }
    ColumnDefinition defn = columnDefinition(
            columnMetaData,
        columnId,
        jdbcType,
        typeName,
        typeClassName,
        nullability,
        ColumnDefinition.Mutability.UNKNOWN,
        precision,
        scale,
        signed,
        displaySize,
        autoIncremented,
        caseSensitive,
        searchable,
        currency,
        isPrimaryKey
    );
    log.warn("column def {} {}",catalogName, defn);
    results.put(columnId, defn);
  }

  @Override
  protected String getSqlType(SinkRecordField field) {
    if (field.schemaName() != null) {
      switch (field.schemaName()) {
        case Decimal.LOGICAL_NAME:
          return "STRING(MAX)";
        case Date.LOGICAL_NAME:
          return "DATE";
        case Time.LOGICAL_NAME:
          return "DATE";
        case Timestamp.LOGICAL_NAME:
          return "TIMESTAMP";
        default:
          // fall through to normal types
      }
    }
    return getPrimitiveSqlType(field);
  }

  private String getPrimitiveSqlType(SinkRecordField field) {
    switch (field.schemaType()) {
      case INT8:
        return "INT64";
      case INT16:
        return "INT64";
      case INT32:
        return "INT64";
      case INT64:
        return "INT64";
      case FLOAT32:
        return "FLOAT64";
      case FLOAT64:
        return "FLOAT64";
      case BOOLEAN:
        return "BOOL";
      case STRING:
        return "STRING(MAX)";
      case BYTES:
        return "BYTES(MAX)";
      default:
        return super.getSqlType(field);
    }
  }

  @Override
  protected Set<ColumnId> primaryKeyColumns(
          Connection connection,
          String catalogPattern,
          String schemaPattern,
          String tablePattern
  ) throws SQLException {

    // Get the primary keys of the table(s) ...
    final Set<ColumnId> pkColumns = new HashSet<>();
    log.debug("Getting primaryKeyColumns");
    Boolean autoCommit = connection.getAutoCommit();
    if (!autoCommit) {
      log.debug("Setting Autocommit to true when was {}", connection.getAutoCommit());
      connection.setAutoCommit(true);
    }
    try (
         ResultSet rs2 = connection.createStatement().executeQuery("SET TRANSACTION READ ONLY");
         ResultSet rs = connection.getMetaData().getPrimaryKeys(
            catalogPattern, schemaPattern, tablePattern);
         ResultSet rs3 = connection.createStatement().executeQuery("SET TRANSACTION READ WRITE");
    ) {
      while (rs.next()) {
        String catalogName = rs.getString(1);
        String schemaName = rs.getString(2);
        String tableName = rs.getString(3);
        TableId tableId = new TableId(catalogName, schemaName, tableName);
        final String colName = rs.getString(4);
        ColumnId columnId = new ColumnId(tableId, colName);
        pkColumns.add(columnId);
        log.debug("Got primaryKeyColumn {}",colName);
      }
    } finally {
      log.debug("Now Setting Autocommit to {} when was true", autoCommit);
      connection.setAutoCommit(autoCommit);
    }
    log.debug("Got primaryKeyColumn {}",pkColumns);
    return pkColumns;
  }


  @Override
  public String buildCreateTableStatement(
          TableId table,
          Collection<SinkRecordField> fields
  ) {
    ExpressionBuilder builder = expressionBuilder();

    final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
    builder.append("CREATE TABLE ");
    builder.append(table);
    builder.append(" (");
    writeColumnsSpec(builder, fields);

    builder.append(") ");
    if (!pkFieldNames.isEmpty()) {
      builder.append(System.lineSeparator());
      builder.append(" PRIMARY KEY(");
      builder.appendList()
              .delimitedBy(",")
              .transformedBy(ExpressionBuilder.quote())
              .of(pkFieldNames);
      builder.append(") ");
    } else {
      log.warn("For some reason I have no Primary Keys.");
    }
    return builder.toString();
  }

  @Override
  protected void writeColumnSpec(ExpressionBuilder builder, SinkRecordField f) {
    builder.appendColumnName(f.name());
    builder.append(" ");
    String sqlType = getSqlType(f);
    builder.append(sqlType);
    if ("TIMESTAMP".equals(sqlType)) {
      builder.append(" OPTIONS (allow_commit_timestamp=true) ");
    }
    if (!isColumnOptional(f)) {
      builder.append(" NOT NULL");
    }
  }
}
