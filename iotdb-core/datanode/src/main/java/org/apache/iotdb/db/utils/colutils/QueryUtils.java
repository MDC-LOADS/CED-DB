/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.utils.colutils;

import java.util.List;

/** SQL查询工具类，提供便捷的静态方法 */
public class QueryUtils {

  /** 检查IoTDB服务是否可用 */
  public static boolean isIoTDBAvailable() {
    return SQLQueryExecutor.isIoTDBServicesAvailable();
  }

  /** 检查IoTDB服务是否可用，带重试机制 */
  public static boolean isIoTDBAvailable(int maxRetries, long retryIntervalMs) {
    return SQLQueryExecutor.isIoTDBServicesAvailable(maxRetries, retryIntervalMs);
  }

  /** 检查IoTDB服务可用性，如果不可用则抛出异常 */
  public static void requireIoTDBAvailable() throws RuntimeException {
    if (!isIoTDBAvailable()) {
      throw new RuntimeException(
          "IoTDB services are not available. Please ensure ConfigNode and DataNode are running.");
    }
  }

  private static volatile SQLQueryExecutor executor;

  /** 获取单例查询执行器 */
  private static SQLQueryExecutor getExecutor() {
    if (executor == null) {
      synchronized (QueryUtils.class) {
        if (executor == null) {
          try {
            executor = new SQLQueryExecutor();
          } catch (RuntimeException e) {
            // Try using the testing method if normal initialization fails
            throw new RuntimeException(
                "Cannot initialize SQLQueryExecutor. "
                    + "This may indicate that IoTDB services are not running or properly initialized. "
                    + "In test environments, this is expected behavior.",
                e);
          }
        }
      }
    }
    return executor;
  }

  /**
   * 执行SQL查询，返回完整结果对象
   *
   * @param sql SQL查询语句
   * @return 查询结果
   * @throws SQLQueryExecutor.QueryExecutionException 查询执行异常
   */
  public static SQLQueryExecutor.QueryResult query(String sql)
      throws SQLQueryExecutor.QueryExecutionException {
    return getExecutor().executeQuery(sql);
  }

  /**
   * 执行SQL查询，返回数据行列表
   *
   * @param sql SQL查询语句
   * @return 数据行列表
   * @throws SQLQueryExecutor.QueryExecutionException 查询执行异常
   */
  public static List<List<Object>> queryRows(String sql)
      throws SQLQueryExecutor.QueryExecutionException {
    return getExecutor().executeQuery(sql).getRows();
  }

  /**
   * 执行SQL查询，返回第一行数据
   *
   * @param sql SQL查询语句
   * @return 第一行数据，如果没有数据则返回null
   * @throws SQLQueryExecutor.QueryExecutionException 查询执行异常
   */
  public static List<Object> queryFirstRow(String sql)
      throws SQLQueryExecutor.QueryExecutionException {
    SQLQueryExecutor.QueryResult result = getExecutor().executeQuery(sql);
    return result.getRows().isEmpty() ? null : result.getRows().get(0);
  }

  /**
   * 执行SQL查询，返回第一行第一列的值
   *
   * @param sql SQL查询语句
   * @return 第一行第一列的值，如果没有数据则返回null
   * @throws SQLQueryExecutor.QueryExecutionException 查询执行异常
   */
  public static Object queryScalar(String sql) throws SQLQueryExecutor.QueryExecutionException {
    List<Object> firstRow = queryFirstRow(sql);
    return (firstRow != null && !firstRow.isEmpty()) ? firstRow.get(0) : null;
  }

  /**
   * 执行SQL查询，返回行数
   *
   * @param sql SQL查询语句
   * @return 结果行数
   * @throws SQLQueryExecutor.QueryExecutionException 查询执行异常
   */
  public static int queryCount(String sql) throws SQLQueryExecutor.QueryExecutionException {
    return getExecutor().executeQuery(sql).getRowCount();
  }

  /**
   * 检查查询是否有结果
   *
   * @param sql SQL查询语句
   * @return 如果有结果返回true，否则返回false
   * @throws SQLQueryExecutor.QueryExecutionException 查询执行异常
   */
  public static boolean hasResults(String sql) throws SQLQueryExecutor.QueryExecutionException {
    return queryCount(sql) > 0;
  }

  /**
   * 执行SQL查询，以表格形式打印结果
   *
   * @param sql SQL查询语句
   * @param maxRows 最大显示行数，0表示显示全部
   * @throws SQLQueryExecutor.QueryExecutionException 查询执行异常
   */
  public static void printQueryResult(String sql, int maxRows)
      throws SQLQueryExecutor.QueryExecutionException {
    SQLQueryExecutor.QueryResult result = getExecutor().executeQuery(sql);

    System.out.println("SQL: " + sql);
    System.out.println("列名: " + result.getColumnNames());
    System.out.println("总行数: " + result.getRowCount());
    System.out.println();

    if (result.getRowCount() == 0) {
      System.out.println("无查询结果");
      return;
    }

    // 打印表头
    List<String> columnNames = result.getColumnNames();
    for (int i = 0; i < columnNames.size(); i++) {
      System.out.printf("%-15s", columnNames.get(i));
      if (i < columnNames.size() - 1) {
        System.out.print(" | ");
      }
    }
    System.out.println();

    // 打印分隔线
    for (int i = 0; i < columnNames.size(); i++) {
      System.out.print("---------------");
      if (i < columnNames.size() - 1) {
        System.out.print("-+-");
      }
    }
    System.out.println();

    // 打印数据行
    List<List<Object>> rows = result.getRows();
    int displayRows = (maxRows > 0) ? Math.min(maxRows, rows.size()) : rows.size();

    for (int rowIdx = 0; rowIdx < displayRows; rowIdx++) {
      List<Object> row = rows.get(rowIdx);
      for (int colIdx = 0; colIdx < row.size(); colIdx++) {
        Object value = row.get(colIdx);
        String valueStr = (value != null) ? value.toString() : "NULL";
        System.out.printf("%-15s", valueStr);
        if (colIdx < row.size() - 1) {
          System.out.print(" | ");
        }
      }
      System.out.println();
    }

    if (maxRows > 0 && rows.size() > maxRows) {
      System.out.println("... (省略了 " + (rows.size() - maxRows) + " 行)");
    }
    System.out.println();
  }

  /**
   * 执行SQL查询，以表格形式打印结果（默认显示前20行）
   *
   * @param sql SQL查询语句
   * @throws SQLQueryExecutor.QueryExecutionException 查询执行异常
   */
  public static void printQueryResult(String sql) throws SQLQueryExecutor.QueryExecutionException {
    printQueryResult(sql, 20);
  }
}
