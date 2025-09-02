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

//
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.SessionManager;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.execution.ExecutionResult;
import org.apache.iotdb.db.queryengine.plan.execution.IQueryExecution;
import org.apache.iotdb.db.queryengine.plan.parser.StatementGenerator;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** 通用SQL查询执行器，用于执行SQL语句并返回格式化的结果 */
public class SQLQueryExecutor {

  private final IPartitionFetcher partitionFetcher;
  private final ISchemaFetcher schemaFetcher;
  private final Coordinator coordinator;

  /**
   * Create a SQLQueryExecutor for testing purposes that handles initialization errors gracefully
   */
  public static SQLQueryExecutor createForTesting() {
    try {
      return new SQLQueryExecutor();
    } catch (RuntimeException e) {
      // In test environment, we might not have all IoTDB components initialized
      // Return a mock or stub implementation that can handle basic operations
      throw new UnsupportedOperationException(
          "SQLQueryExecutor cannot be initialized in this environment. "
              + "This is normal in unit test environments where IoTDB cluster is not running. "
              + "Original error: "
              + e.getMessage(),
          e);
    }
  }

  /** Check if the current environment has IoTDB services available */
  public static boolean isIoTDBServicesAvailable() {
    return isIoTDBServicesAvailable(1, 0); // 默认不重试
  }

  /** Check if the current environment has IoTDB services available with retry */
  public static boolean isIoTDBServicesAvailable(int maxRetries, long retryIntervalMs) {
    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        SQLQueryExecutor executor = new SQLQueryExecutor();
        // 进一步检查是否能执行基本查询
        executor.executeQuery("SHOW CLUSTER");
        return true;
      } catch (Exception e) {
        if (attempt == maxRetries) {
          // 记录具体的错误原因以便调试
          System.err.println(
              "IoTDB services check failed after " + maxRetries + " attempts: " + e.getMessage());
          if (e.getCause() != null) {
            System.err.println("Caused by: " + e.getCause().getMessage());
          }
        } else {
          System.err.println(
              "IoTDB services check attempt "
                  + attempt
                  + "/"
                  + maxRetries
                  + " failed, retrying...");
          try {
            Thread.sleep(retryIntervalMs);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return false;
          }
        }
      }
    }
    return false;
  }

  public SQLQueryExecutor() {
    try {
      // 在测试环境中，需要手动初始化ConfigNode信息
      initializeConfigNodeInfo();

      this.partitionFetcher = ClusterPartitionFetcher.getInstance();
      this.schemaFetcher = ClusterSchemaFetcher.getInstance();
      this.coordinator = Coordinator.getInstance();
    } catch (ExceptionInInitializerError | NoClassDefFoundError e) {
      throw new RuntimeException(
          "Failed to initialize SQLQueryExecutor. "
              + "This usually happens in test environment when IoTDB components are not properly initialized. "
              + "Please ensure DataNodeId is set or use createForTesting() method in test environments.",
          e);
    }
  }

  /**
   * 执行SQL查询并返回结果
   *
   * @param sql SQL查询语句
   * @param sessionId 会话ID，如果为null则使用默认会话
   * @return 查询结果
   * @throws QueryExecutionException 查询执行异常
   */
  public QueryResult executeQuery(String sql, Long sessionId) throws QueryExecutionException {
    try {
      // 解析SQL语句
      Statement statement = StatementGenerator.createStatement(sql, ZoneId.systemDefault());

      if (statement == null) {
        throw new QueryExecutionException("Failed to parse SQL statement: " + sql);
      }

      // 获取查询ID和会话信息
      SessionManager sessionManager = SessionManager.getInstance();
      Long queryId = sessionManager.requestQueryId();

      // In testing or standalone environment, there might be no current session
      IClientSession currentSession = sessionManager.getCurrSession();
      SessionInfo sessionInfo;
      if (currentSession == null) {
        // Create a default session info for testing/standalone usage
        sessionInfo = createDefaultSessionInfo();
      } else {
        sessionInfo = sessionManager.getSessionInfo(currentSession);
      }

      // 创建查询执行器
      ExecutionResult executionResult =
          coordinator.executeForTreeModel(
              statement,
              queryId,
              sessionInfo,
              sql,
              partitionFetcher,
              schemaFetcher,
              IoTDBDescriptor.getInstance().getConfig().getQueryTimeoutThreshold());

      if (executionResult.status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new QueryExecutionException(
            "Query execution failed: " + executionResult.status.getMessage());
      }

      IQueryExecution queryExecution = coordinator.getQueryExecution(queryId);
      if (queryExecution == null) {
        throw new QueryExecutionException("Query execution is null");
      }

      // 提取查询结果
      return extractQueryResult(queryExecution);

    } catch (Exception e) {
      // 提供更友好的错误信息
      String errorMessage = "Error executing SQL: " + sql;
      if (e.getMessage() != null) {
        if (e.getMessage().contains("getSchemaPartition")) {
          errorMessage +=
              "\n\nThis error usually occurs when IoTDB ConfigNode is not running or accessible.\n"
                  + "To fix this:\n"
                  + "1. Ensure IoTDB cluster is properly started\n"
                  + "2. Check ConfigNode connectivity\n"
                  + "3. Verify network configuration\n"
                  + "4. For testing purposes, use a running IoTDB instance";
        } else if (e.getMessage().contains("ClientManagerException")
            || e.getMessage().contains("Connection")) {
          errorMessage +=
              "\n\nConnection error: IoTDB services are not accessible.\n"
                  + "Please ensure IoTDB DataNode and ConfigNode are running.";
        }
      }
      throw new QueryExecutionException(errorMessage, e);
    }
  }

  /**
   * 简化版本的查询执行方法，使用默认会话
   *
   * @param sql SQL查询语句
   * @return 查询结果
   * @throws QueryExecutionException 查询执行异常
   */
  public QueryResult executeQuery(String sql) throws QueryExecutionException {
    return executeQuery(sql, null);
  }

  /** 从QueryExecution中提取结果数据 */
  private QueryResult extractQueryResult(IQueryExecution queryExecution) throws IoTDBException {
    QueryResult result = new QueryResult();
    DatasetHeader header = queryExecution.getDatasetHeader();

    if (header != null) {
      result.setColumnNames(header.getRespColumns());
      result.setDataTypes(header.getRespDataTypes());
    }

    List<List<Object>> rows = new ArrayList<>();
    List<Long> timestamps = new ArrayList<>();

    while (true) {
      Optional<TsBlock> optionalTsBlock = queryExecution.getBatchResult();
      if (!optionalTsBlock.isPresent() || optionalTsBlock.get().isEmpty()) {
        break;
      }

      TsBlock tsBlock = optionalTsBlock.get();
      int rowCount = tsBlock.getPositionCount();

      // 处理时间戳列
      for (int i = 0; i < rowCount; i++) {
        timestamps.add(tsBlock.getTimeByIndex(i));
      }

      // 处理数据列
      if (header != null && header.getRespColumns() != null) {
        Map<String, Integer> columnIndexMap = header.getColumnNameIndexMap();
        List<String> columnNames = header.getRespColumns();

        // 检查columnIndexMap是否为null
        if (columnIndexMap == null) {
          // 如果没有columnIndexMap，创建一个简单的映射
          columnIndexMap = new java.util.HashMap<String, Integer>();
          for (int i = 0; i < columnNames.size(); i++) {
            columnIndexMap.put(columnNames.get(i), i);
          }
        }

        for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
          List<Object> row = new ArrayList<>();

          for (String columnName : columnNames) {
            Integer columnIndex = columnIndexMap.get(columnName);
            if (columnIndex != null && columnIndex < tsBlock.getValueColumnCount()) {
              Column column = tsBlock.getColumn(columnIndex);

              if (column.isNull(rowIdx)) {
                row.add(null);
              } else {
                Object value =
                    column.getDataType().equals(TSDataType.TEXT)
                        ? column.getBinary(rowIdx).getStringValue(TSFileConfig.STRING_CHARSET)
                        : column.getObject(rowIdx);
                row.add(value);
              }
            } else {
              row.add(null);
            }
          }

          rows.add(row);
        }
      }
    }

    result.setTimestamps(timestamps);
    result.setRows(rows);
    result.setRowCount(rows.size());

    return result;
  }

  /** 初始化ConfigNode信息，在测试环境中可能需要 */
  private void initializeConfigNodeInfo() {
    try {
      ConfigNodeInfo configNodeInfo = ConfigNodeInfo.getInstance();
      // 尝试加载配置
      configNodeInfo.loadConfigNodeList();

      // 检查是否有ConfigNode信息
      if (configNodeInfo.getLatestConfigNodes().isEmpty()) {
        // 如果没有配置，尝试使用默认的ConfigNode地址
        java.util.List<TEndPoint> defaultConfigNodes = new java.util.ArrayList<TEndPoint>();
        defaultConfigNodes.add(new TEndPoint("127.0.0.1", 10710)); // 默认ConfigNode地址
        configNodeInfo.updateConfigNodeList(defaultConfigNodes);
      }
    } catch (Exception e) {
      // 在初始化失败时，记录但不阻止初始化
      System.err.println("Warning: Failed to initialize ConfigNode info: " + e.getMessage());
    }
  }

  /** 创建默认的SessionInfo用于测试或独立运行环境 */
  private SessionInfo createDefaultSessionInfo() {
    return new SessionInfo(
        0L, // 默认会话 ID
        "root", // 默认用户名
        ZoneId.systemDefault(), // 系统默认时区
        IoTDBConstant.ClientVersion.V_1_0 // 默认客户端版本
        );
  }

  /** 查询结果类 */
  public static class QueryResult {
    private List<String> columnNames = new ArrayList<>();
    private List<TSDataType> dataTypes = new ArrayList<>();
    private List<Long> timestamps = new ArrayList<>();
    private List<List<Object>> rows = new ArrayList<>();
    private int rowCount = 0;

    // Getters and Setters
    public List<String> getColumnNames() {
      return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
      this.columnNames = columnNames;
    }

    public List<TSDataType> getDataTypes() {
      return dataTypes;
    }

    public void setDataTypes(List<TSDataType> dataTypes) {
      this.dataTypes = dataTypes;
    }

    public List<Long> getTimestamps() {
      return timestamps;
    }

    public void setTimestamps(List<Long> timestamps) {
      this.timestamps = timestamps;
    }

    public List<List<Object>> getRows() {
      return rows;
    }

    public void setRows(List<List<Object>> rows) {
      this.rows = rows;
    }

    public int getRowCount() {
      return rowCount;
    }

    public void setRowCount(int rowCount) {
      this.rowCount = rowCount;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("QueryResult{\n");
      sb.append("  columnNames: ").append(columnNames).append("\n");
      sb.append("  dataTypes: ").append(dataTypes).append("\n");
      sb.append("  rowCount: ").append(rowCount).append("\n");
      sb.append("  rows: [\n");

      for (int i = 0; i < Math.min(rows.size(), 5); i++) {
        sb.append("    ").append(rows.get(i)).append("\n");
      }

      if (rows.size() > 5) {
        sb.append("    ... (").append(rows.size() - 5).append(" more rows)\n");
      }

      sb.append("  ]\n");
      sb.append("}");
      return sb.toString();
    }
  }

  /** 查询执行异常 */
  public static class QueryExecutionException extends Exception {
    public QueryExecutionException(String message) {
      super(message);
    }

    public QueryExecutionException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
