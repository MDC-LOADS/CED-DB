/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Per-query state manager for collaborative queries. Query instances are managed by
 * ColQuerySessions and looked up by query id instead of using a global singleton.
 */
public class QueryStateManager {

  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  private ColQueryStateMachine stateMachine;//协同查询状态机

  private final ConcurrentHashMap<String, ScanStates> scanStatesMap = new ConcurrentHashMap<>();//SeriesPath定位scan算子的状态

  private final ConcurrentHashMap<String, String> scanPathsMap =new ConcurrentHashMap<>();//PlanNodeId->SeriesPath对应

  private final ConcurrentHashMap<String, String> scanPlanNodeIdsMap =new ConcurrentHashMap<>();//SeriesPath->PlanNodeId一一对应

  private final ConcurrentHashMap<String, Boolean> scanExchangeMap = new ConcurrentHashMap<>();//seriesPath->ExchangeNode or seriesScan

  private volatile boolean hasLeftOuterJoin = false;//查询是否含有左外连接算子

  private volatile TsBlock leftOuterJoinCacheLeft;//保存左外连接算子内的中间状态

  private volatile TsBlock leftOuterJoinCacheRight;//保存左外连接算子内的中间状态


  //新增
  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
          MPPDataExchangeService.getInstance().getMPPDataExchangeManager();

  private volatile String rootIdentitySinkId = null;//查询的根结点Id

  private int cloudFragmentId;

  private volatile int edgeFragmentId;

  private volatile ISourceHandle sourceHandle = null;//接收数据句柄

  private volatile ISinkHandle sinkHandle = null;

  private static final String localhostIp = "127.0.0.1";

  private static final String remoteIp = "127.0.0.1";

  private static final String broadcastIp = "0.0.0.0";

  private static final int localhostRpcPort = 9091;

  private static final int remoteRpcPort = 9090;

  private static final int localPort = 10744;

  private static final int remotePort = 10740;

  private String colQueryId;//协同查询的id

  private static final String colPlanNodeId = "colPlanNodeId";

  private volatile boolean isSingleScan = false;

  // record SQL for heuristics / debugging
  private String sql;


  /** 构造函数 */
  public QueryStateManager(ColQueryStateMachine stateMachine) {
    this.stateMachine = stateMachine;
    this.colQueryId = "null";
    this.cloudFragmentId = 1000;
  }

  public QueryStateManager() {
    this.stateMachine = null;
    this.colQueryId = "null";
    this.cloudFragmentId = 1000;

  }

  public QueryStateManager(ColQueryStateMachine stateMachine, String queryId) {
    this.stateMachine = stateMachine;
    this.colQueryId = queryId;
    this.cloudFragmentId = 1000;
  }

  /** Inner class representing states for scan operators */
  public static class ScanStates {
    private volatile long scanTimestamp;
    private volatile long offset;
    private volatile boolean isCouldEqual;
    private volatile boolean isInnerJoin;
    private volatile boolean isFullOuterJoin;

    public ScanStates() {
      this.scanTimestamp = 0L;
      this.offset = 0L;
      this.isCouldEqual = false;
      this.isInnerJoin = false;
      this.isFullOuterJoin = false;
    }

    public ScanStates(
        long scanTimestamp,
        long offset,
        boolean isCouldEqual,
        boolean isInnerJoin,
        boolean isFullOuterJoin) {
      this.scanTimestamp = scanTimestamp;
      this.offset = offset;
      this.isCouldEqual = isCouldEqual;
      this.isInnerJoin = isInnerJoin;
      this.isFullOuterJoin = isFullOuterJoin;
    }

    public long getScanTimestamp() {
      return scanTimestamp;
    }

    public void setScanTimestamp(long scanTimestamp) {
      this.scanTimestamp = scanTimestamp;
    }

    public long getOffset() {
      return offset;
    }

    public void setOffset(long offset) {
      this.offset = offset;
    }

    public boolean isCouldEqual() {
      return isCouldEqual;
    }

    public void setCouldEqual(boolean couldEqual) {
      isCouldEqual = couldEqual;
    }

    public boolean isInnerJoin() {
      return isInnerJoin;
    }

    public void setInnerJoin(boolean innerJoin) {
      isInnerJoin = innerJoin;
    }

    public boolean isFullOuterJoin() {
      return isFullOuterJoin;
    }

    public void setFullOuterJoin(boolean fullOuterJoin) {
      isFullOuterJoin = fullOuterJoin;
    }

    @Override
    public String toString() {
      return "ScanStates{"
          + "scanTimestamp="
          + scanTimestamp
          + ", offset="
          + offset
          + ", isCouldEqual="
          + isCouldEqual
          + ", isInnerJoin="
          + isInnerJoin
          + ", isFullOuterJoin="
          + isFullOuterJoin
          + '}';
    }

    public ScanStates copy() {
      return new ScanStates(scanTimestamp, offset, isCouldEqual, isInnerJoin, isFullOuterJoin);
    }
  }

  // StateMachine operations
  public ColQueryStateMachine getStateMachine() {
    lock.readLock().lock();
    try {
      return stateMachine;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setStateMachine(ColQueryStateMachine stateMachine) {
    lock.writeLock().lock();
    try {
      this.stateMachine = stateMachine;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public String getQueryId() {
    lock.readLock().lock();
    try {
      return colQueryId;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void setQueryId(String queryId) {
    lock.writeLock().lock();
    try {
      this.colQueryId = queryId;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public String getSql() { return sql; }

  public void setSql(String sql) { this.sql = sql; }

  // Scan states operations
  public ScanStates getScanStates(String scanPath) {
    return scanStatesMap.get(scanPath);
  }

  public ScanStates getScanStatesCopy(String scanPath) {
    ScanStates states = scanStatesMap.get(scanPath);
    return states != null ? states.copy() : null;
  }

  public void setScanStates(String scanPath, ScanStates scanStates) {
    scanStatesMap.put(scanPath, scanStates);
  }

  public void updateScanTimestamp(String scanPath, long timestamp) {
    scanStatesMap.computeIfAbsent(scanPath, k -> new ScanStates()).setScanTimestamp(timestamp);
  }

  public void updateScanOffset(String scanPath, long offset) {
    scanStatesMap.computeIfAbsent(scanPath, k -> new ScanStates()).setOffset(offset);
  }

  public void updateScanCouldEqual(String scanPath, boolean couldEqual) {
    scanStatesMap.computeIfAbsent(scanPath, k -> new ScanStates()).setCouldEqual(couldEqual);
  }

  public void updateScanInnerJoin(String scanPath, boolean innerJoin) {
    scanStatesMap.computeIfAbsent(scanPath, k -> new ScanStates()).setInnerJoin(innerJoin);
  }

  public void updateScanFullOuterJoin(String scanPath, boolean fullOuterJoin) {
    scanStatesMap.computeIfAbsent(scanPath, k -> new ScanStates()).setFullOuterJoin(fullOuterJoin);
  }

  public void removeScanStates(String scanPath) {
    scanStatesMap.remove(scanPath);
  }

  public ConcurrentHashMap<String, ScanStates> getAllScanStates() {
    return new ConcurrentHashMap<>(scanStatesMap);
  }

  public void clearAllScanStates() {
    scanStatesMap.clear();
  }

  public String getSeriesPath(String seriesPlanNodeId) {
    return scanPathsMap.get(seriesPlanNodeId);
  }

  public String getPlanNodeId(String seriesPath) {
    return scanPlanNodeIdsMap.get(seriesPath);
  }

  public boolean isHasPlanNodeId(String seriesPath) {
    return scanPlanNodeIdsMap.get(seriesPath) != null;
  }

  public boolean isHasSeriesPath(String seriesPlanNodeId) {
//    System.out.println("isHasSeriesPath:"+scanPathsMap.get(seriesPlanNodeId));
    return scanPathsMap.get(seriesPlanNodeId) != null;
  }

  public void setSeriesPathAndPlanNodeId(String seriesPlanNodeId, String seriesPath) {
    if(scanPathsMap.contains(seriesPlanNodeId) || scanPlanNodeIdsMap.contains(seriesPath)) {
      return;
    }
    scanPathsMap.put(seriesPlanNodeId, seriesPath);
    scanPlanNodeIdsMap.put(seriesPath, seriesPlanNodeId);
  }

  public void updateScanTimestampByPlanNodeId(String planNodeId, long timestamp) {
    String scanPath = scanPathsMap.get(planNodeId);
    scanStatesMap.computeIfAbsent(scanPath, k -> new ScanStates()).setScanTimestamp(timestamp);
  }

  public void updateScanOffsetByPlanNodeId(String planNodeId, long offset) {
    String scanPath = scanPathsMap.get(planNodeId);
    scanStatesMap.computeIfAbsent(scanPath, k -> new ScanStates()).setOffset(offset);
  }

  public void updateScanCouldEqualByPlanNodeId(String planNodeId, boolean couldEqual) {
    String scanPath = scanPathsMap.get(planNodeId);
    scanStatesMap.computeIfAbsent(scanPath, k -> new ScanStates()).setCouldEqual(couldEqual);
  }

  public void updateScanInnerJoinByPlanNodeId(String planNodeId, boolean innerJoin) {
    String scanPath = scanPathsMap.get(planNodeId);
    scanStatesMap.computeIfAbsent(scanPath, k -> new ScanStates()).setInnerJoin(innerJoin);
  }

  public void updateScanFullOuterJoinByPlanNodeId(String planNodeId, boolean fullOuterJoin) {
    String scanPath = scanPathsMap.get(planNodeId);
    scanStatesMap.computeIfAbsent(scanPath, k -> new ScanStates()).setFullOuterJoin(fullOuterJoin);
  }

  public  void setScanPathExchange(String seriesPath,boolean isExchange) {
    scanExchangeMap.put(seriesPath,isExchange);
  }

  public void setScanPathExchangeByPlanNodeId(String planNodeId,boolean isExchange) {
    String scanPath = scanPathsMap.get(planNodeId);
    if(scanPath!=null) {
      scanExchangeMap.put(scanPath,isExchange);
    }else {
      System.out.println("scanPath is null");
    }
  }

  public boolean isScanPathExchange(String seriesPath) {
    if(scanExchangeMap.get(seriesPath) == null) {
      System.out.println("isScanPathExchange:"+seriesPath+"is null!");
      return false;
    }
    return scanExchangeMap.get(seriesPath);
  }

  public boolean isScanPathExchangeByPlanNodeId(String planNodeId) {
    String scanPath = scanPathsMap.get(planNodeId);
    if(scanPath!=null) {
//      System.out.println("bug位置，当前获取到的是:"+planNodeId);
      if(scanExchangeMap.get(scanPath)!=null) {
        return scanExchangeMap.get(scanPath);
      }
//      System.out.println("scan Exchange为空:"+scanPath+"is null!");
      return  false;
    }
//    System.out.println("scanPath is null");
    return false;
  }


  // LeftOuterJoin operations
  public boolean hasLeftOuterJoin() {
    return hasLeftOuterJoin;
  }

  public void setHasLeftOuterJoin(boolean hasLeftOuterJoin) {
    this.hasLeftOuterJoin = hasLeftOuterJoin;
  }

  public TsBlock getLeftOuterJoinCacheLeft() {
    return leftOuterJoinCacheLeft;
  }

  public TsBlock getLeftOuterJoinCacheRight() {
    return leftOuterJoinCacheRight;
  }

  public void setLeftOuterJoinCacheLeft(TsBlock leftOuterJoinCache) {
    this.leftOuterJoinCacheLeft = leftOuterJoinCache;
  }

  public void setLeftOuterJoinCacheRight(TsBlock leftOuterJoinCache) {
    this.leftOuterJoinCacheRight = leftOuterJoinCache;
  }

  public void clearLeftOuterJoinCache() {
    this.leftOuterJoinCacheLeft = null;
    this.leftOuterJoinCacheRight = null;
  }

  public String getLocalhostIp() {
    return localhostIp;
  }

  public String getRemoteIp() {
    return remoteIp;
  }

  public String getBroadcastIp(){
      return broadcastIp;
  }

  public int getLocalhostRpcPort() {
    return localhostRpcPort;
  }

  public int getRemoteRpcPort() {
    return remoteRpcPort;
  }

  public void setCloudFragmentId(int cloudFragmentId) {
    this.cloudFragmentId = cloudFragmentId;
  }

  public int getAndAddCloudFragmentId() {
    return ++cloudFragmentId;
  }

  public int getCloudFragmentId(){
      return cloudFragmentId;
  }

  public void setEdgeFragmentId(int edgeFragmentId) {
    this.edgeFragmentId = edgeFragmentId;
  }

  public void setRootIdentitySinkId(String rootIdentitySinkId) {
    this.rootIdentitySinkId = rootIdentitySinkId;
  }

  public void setRootIdentitySinkId(PlanNodeId rootIdentitySinkId) {
    this.rootIdentitySinkId = rootIdentitySinkId.getId();
  }

  public String getRootIdentitySinkId() {
    return rootIdentitySinkId;
  }

  public List<ScanStates> getAllScanStatesList() {
        return new ArrayList<>(scanStatesMap.values());
  }

  public List<String> getAllScanPathList() {
        return new ArrayList<>(scanPlanNodeIdsMap.keySet());
    }

  public List<String> getAllScanPlanNodeIdList() {
        return new ArrayList<>(scanPathsMap.keySet());
    }

  public void setSourceHandle(ISourceHandle sourceHandle) {
    this.sourceHandle = sourceHandle;
  }

  //创建并建立SourceHandle
  public void createAndSetSourceHandle(int cloudFragmentId) {
    TEndPoint remoteEndpoint = new TEndPoint(remoteIp, remotePort);
    TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(colQueryId,edgeFragmentId,"0");
    TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(colQueryId,cloudFragmentId,"0");
    long queryNum=1;
    FragmentInstanceContext colQueryInstanceContext = new FragmentInstanceContext(queryNum);
    this.sourceHandle = MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
              localFragmentInstanceId,
              colPlanNodeId,
              0,
              remoteEndpoint,
              remoteFragmentInstanceId,
              colQueryInstanceContext::failed);
  }

  public ISinkHandle getSinkHandle() {
      return sinkHandle;
  }

  public  void createAndSetSinkHandle(int edgeFragmentId){
      TEndPoint remoteEndpoint = new TEndPoint(remoteIp, remotePort);
      TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(colQueryId,cloudFragmentId,"0");
      TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(colQueryId,edgeFragmentId,"0");
      int channelNum = 1;
      AtomicInteger cnt = new AtomicInteger(channelNum);
      long query_num=1;
      FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);
      DownStreamChannelIndex downStreamChannelIndex = new DownStreamChannelIndex(0);
      String localPlanNodeId = "colCloudPlanNodeId";
      System.out.println("localPlanNodeId:"+localPlanNodeId);
      System.out.println("colPlanNodeId:"+colPlanNodeId);
      System.out.println("colQueryId:"+colQueryId);
      System.out.println("edgeFragmentId:"+edgeFragmentId);
      System.out.println("cloudFragmentId:"+cloudFragmentId);

      this.sinkHandle =
              MPP_DATA_EXCHANGE_MANAGER.createShuffleSinkHandle(
                      Collections.singletonList(
                              new DownStreamChannelLocation(
                                      remoteEndpoint,
                                      remoteFragmentInstanceId,
                                      colPlanNodeId)),
                      downStreamChannelIndex,
                      ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
                      localFragmentInstanceId,
                      localPlanNodeId,
                      instanceContext);

  }
  public ISourceHandle getSourceHandle() {
    return sourceHandle;
  }

  public void setSingleScan(boolean singleScan) {
    isSingleScan = singleScan;
  }

  public boolean isSingleScan() {
    return isSingleScan;
  }

  // Utility methods
  public void resetStates() {
    lock.writeLock().lock();
    try {
      clearAllScanStates();
      setHasLeftOuterJoin(false);
      clearLeftOuterJoinCache();
    } finally {
      lock.writeLock().unlock();
    }
  }


  @Override
  public String toString() {
    lock.readLock().lock();
    try {
      return "QueryStateManager{"
          + "stateMachine="
          + (stateMachine != null ? stateMachine.getState() : "null")
          + ", scanStatesCount="
          + scanStatesMap.size()
          + ", hasLeftOuterJoin="
          + hasLeftOuterJoin
          + ", leftOuterJoinCache="
          + (leftOuterJoinCacheLeft != null ? "present" : "null")
          + '}';
    } finally {
      lock.readLock().unlock();
    }
  }

  /** Get current query state summary for debugging */
  public String getStateSummary() {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryStateManager Summary:\n");

    // We keep the whole dump under read lock to keep a consistent snapshot
    lock.readLock().lock();
    try {
      // Basic query info
      sb.append("  QueryId: ")
              .append(colQueryId == null ? "<null>" : colQueryId)
              .append("\n");
      sb.append("  StateMachine: ")
              .append(stateMachine != null ? stateMachine.getState() : "<null>")
              .append("\n");

      // Scan states: print by PlanNodeId if available, otherwise list orphans by seriesPath
      sb.append("  Scan States (by PlanNodeId):\n");
      if (scanPathsMap.isEmpty() && scanStatesMap.isEmpty()) {
        sb.append("    <empty>\n");
      } else {
        // Primary view: PlanNodeId -> seriesPath -> states + exchange
        for (java.util.Map.Entry<String, String> e : scanPathsMap.entrySet()) {
          final String planNodeId = e.getKey();
          final String seriesPath = e.getValue();
          final ScanStates states = scanStatesMap.get(seriesPath);
          final Boolean isExchange = scanExchangeMap.get(seriesPath);
          sb.append("    PlanNodeId: ").append(planNodeId).append("\n");
          sb.append("      seriesPath: ").append(seriesPath == null ? "<null>" : seriesPath).append("\n");
          sb.append("      states: ").append(states == null ? "<null>" : states.toString()).append("\n");
          sb.append("      isExchange: ").append(isExchange == null ? "<null>" : isExchange.toString()).append("\n");
        }

        // Orphans: seriesPath present in states map but not mapped to any PlanNodeId
        for (java.util.Map.Entry<String, ScanStates> e : scanStatesMap.entrySet()) {
          final String seriesPath = e.getKey();
          if (!scanPlanNodeIdsMap.containsKey(seriesPath)) {
            final ScanStates states = e.getValue();
            final Boolean isExchange = scanExchangeMap.get(seriesPath);
            sb.append("    <orphan> seriesPath: ").append(seriesPath).append("\n");
            sb.append("      states: ").append(states == null ? "<null>" : states.toString()).append("\n");
            sb.append("      isExchange: ").append(isExchange == null ? "<null>" : isExchange.toString()).append("\n");
          }
        }
      }

      // Left outer join cache dump (assume value columns are double type as requested)
      sb.append("  HasLeftOuterJoin: ").append(hasLeftOuterJoin).append("\n");
      if (leftOuterJoinCacheLeft == null) {
        sb.append("  LeftOuterJoinCache: <null>\n");
      } else {
        try {
          sb.append("  LeftOuterJoinCache: present\n");
          final int rowCount = leftOuterJoinCacheLeft.getPositionCount();
          final org.apache.tsfile.block.column.Column[] valueColumns = leftOuterJoinCacheLeft.getValueColumns();
          final int colCount = valueColumns == null ? 0 : valueColumns.length;
          sb.append("    rows: ").append(rowCount).append(", valueColumns: ").append(colCount).append("\n");

          // time column
          long[] times = leftOuterJoinCacheLeft.getTimeColumn() == null ? null : leftOuterJoinCacheLeft.getTimeColumn().getTimes();
          if (times != null) {
            sb.append("    time:");
            for (int i = 0; i < rowCount; i++) {
              sb.append(i == 0 ? " [" : ", ").append(times[i]);
            }
            sb.append("]\n");
          } else {
            sb.append("    time: <null>\n");
          }

          // values (assume double)
          for (int c = 0; c < colCount; c++) {
            sb.append("    col").append(c).append(":");
            org.apache.tsfile.block.column.Column col = valueColumns[c];
            if (col == null) {
              sb.append(" <null>\n");
              continue;
            }
            sb.append(" [");
            for (int r = 0; r < rowCount; r++) {
              if (r > 0) sb.append(", ");
              // as requested, assume double type
              sb.append(col.getDouble(r));
            }
            sb.append("]\n");
          }
        } catch (Throwable t) {
          sb.append("  LeftOuterJoinCache: <error dumping cache> ").append(t.getMessage()).append("\n");
        }
      }
      if (leftOuterJoinCacheRight == null) {
        sb.append("  LeftOuterJoinCacheRight: <null>\n");
      } else {
        try {
          sb.append("  LeftOuterJoinCacheRight: present\n");
          final int rowCount = leftOuterJoinCacheRight.getPositionCount();
          final org.apache.tsfile.block.column.Column[] valueColumns = leftOuterJoinCacheRight.getValueColumns();
          final int colCount = valueColumns == null ? 0 : valueColumns.length;
          sb.append("    rows: ").append(rowCount).append(", valueColumns: ").append(colCount).append("\n");

          // time column
          long[] times = leftOuterJoinCacheRight.getTimeColumn() == null ? null : leftOuterJoinCacheRight.getTimeColumn().getTimes();
          if (times != null) {
            sb.append("    time:");
            for (int i = 0; i < rowCount; i++) {
              sb.append(i == 0 ? " [" : ", ").append(times[i]);
            }
            sb.append("]\n");
          } else {
            sb.append("    time: <null>\n");
          }

          // values (assume double)
          for (int c = 0; c < colCount; c++) {
            sb.append("    col").append(c).append(":");
            org.apache.tsfile.block.column.Column col = valueColumns[c];
            if (col == null) {
              sb.append(" <null>\n");
              continue;
            }
            sb.append(" [");
            for (int r = 0; r < rowCount; r++) {
              if (r > 0) sb.append(", ");
              // as requested, assume double type
              sb.append(col.getDouble(r));
            }
            sb.append("]\n");
          }
        } catch (Throwable t) {
          sb.append("  LeftOuterJoinCacheRight: <error dumping cache> ").append(t.getMessage()).append("\n");
        }
      }
    } finally {
      lock.readLock().unlock();
    }

    return sb.toString();
  }
}
