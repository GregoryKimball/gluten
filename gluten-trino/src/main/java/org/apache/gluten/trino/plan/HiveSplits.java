/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gluten.trino.plan;

import io.substrait.proto.ReadRel;
import io.trino.metadata.Split;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Reads Hive connector handles and splits. The Hive classes live in the plugin's class loader, so
 * they are accessed reflectively by name rather than linked against.
 */
public final class HiveSplits {
  private static final String HIVE_SPLIT = "io.trino.plugin.hive.HiveSplit";
  private static final String HIVE_TABLE_HANDLE = "io.trino.plugin.hive.HiveTableHandle";
  private static final String HIVE_COLUMN_HANDLE = "io.trino.plugin.hive.HiveColumnHandle";

  private HiveSplits() {}

  public static void checkTable(ConnectorTableHandle table) {
    if (!table.getClass().getName().equals(HIVE_TABLE_HANDLE)) {
      throw new UnsupportedPlanException("table handle " + table.getClass().getName());
    }
    if (((Optional<?>) invoke(table, "getTablePartitioning")).isPresent()) {
      throw new UnsupportedPlanException("bucketed Hive table");
    }
    if (!((List<?>) invoke(table, "getPartitionColumns")).isEmpty()) {
      throw new UnsupportedPlanException("partitioned Hive table");
    }
  }

  /** Returns the file column name for a regular Hive column. */
  public static String columnName(ColumnHandle column) {
    if (!column.getClass().getName().equals(HIVE_COLUMN_HANDLE)) {
      throw new UnsupportedPlanException("column handle " + column.getClass().getName());
    }
    if (!invoke(column, "getColumnType").toString().equals("REGULAR")
        || !(Boolean) invoke(column, "isBaseColumn")) {
      throw new UnsupportedPlanException("Hive column " + column);
    }
    return (String) invoke(column, "getBaseColumnName");
  }

  public static ReadRel.LocalFiles.FileOrFiles toFile(Split split) {
    ConnectorSplit hiveSplit = split.getConnectorSplit();
    if (!hiveSplit.getClass().getName().equals(HIVE_SPLIT)) {
      throw new IllegalStateException("Expected a Hive split, got " + hiveSplit.getClass().getName());
    }
    String serde = (String) invoke(invoke(hiveSplit, "getSchema"), "serializationLibraryName");
    if (!serde.toLowerCase().contains("parquet")) {
      throw new IllegalStateException("Native scan supports Parquet only, got " + serde);
    }
    if (!((List<?>) invoke(hiveSplit, "getPartitionKeys")).isEmpty()
        || ((Optional<?>) invoke(hiveSplit, "getAcidInfo")).isPresent()
        || ((Optional<?>) invoke(hiveSplit, "getBucketConversion")).isPresent()
        || ((OptionalInt) invoke(hiveSplit, "getReadBucketNumber")).isPresent()) {
      throw new IllegalStateException("Native scan does not support partitioned, ACID or bucketed splits");
    }
    return ReadRel.LocalFiles.FileOrFiles.newBuilder()
        .setUriFile((String) invoke(hiveSplit, "getPath"))
        .setStart((Long) invoke(hiveSplit, "getStart"))
        .setLength((Long) invoke(hiveSplit, "getLength"))
        .setParquet(ReadRel.LocalFiles.FileOrFiles.ParquetReadOptions.getDefaultInstance())
        .setProperties(
            ReadRel.LocalFiles.FileOrFiles.fileProperties.newBuilder()
                .setFileSize((Long) invoke(hiveSplit, "getEstimatedFileSize"))
                .setModificationTime((Long) invoke(hiveSplit, "getFileModifiedTime")))
        .build();
  }

  private static Object invoke(Object target, String name) {
    try {
      Method method = target.getClass().getMethod(name);
      return method.invoke(target);
    } catch (ReflectiveOperationException e) {
      throw new UnsupportedPlanException(target.getClass().getName() + "." + name + ": " + e);
    }
  }
}
