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

package org.apache.iotdb.db.queryengine.execution.operator.sink;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState;
import org.apache.iotdb.db.queryengine.execution.colquery.QueryStateManager;
import org.apache.iotdb.db.queryengine.execution.colquery.ScanInfoConverter;
import org.apache.iotdb.db.queryengine.execution.colquery.colservice.C2EColService;
import org.apache.iotdb.db.queryengine.execution.colquery.colservice.Column;
import org.apache.iotdb.db.queryengine.execution.colquery.colservice.ScanInfo;
import org.apache.iotdb.db.queryengine.execution.colquery.colservice.TimeColumn;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.iotdb.db.service.DataNode;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.common.rpc.thrift.TConsensusGroupType.DataRegion;

public class IdentitySinkOperator implements Operator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(IdentitySinkOperator.class)
          + RamUsageEstimator.shallowSizeOfInstance(DownStreamChannelIndex.class);

  private final OperatorContext operatorContext;
  private final List<Operator> children;

  private final DownStreamChannelIndex downStreamChannelIndex;

  private final ISinkHandle sinkHandle;

  private volatile ISinkHandle colSinkHandle;

  private boolean needToReturnNull = false;

  private boolean isFinished = false;

  public IdentitySinkOperator(
      OperatorContext operatorContext,
      List<Operator> children,
      DownStreamChannelIndex downStreamChannelIndex,
      ISinkHandle sinkHandle) {
    this.operatorContext = operatorContext;
    this.children = children;
    this.downStreamChannelIndex = downStreamChannelIndex;
    this.sinkHandle = sinkHandle;
  }

  @Override
  public boolean hasNext() throws Exception {
    if(QueryStateManager.isInitialized()){
//        .getInstances().get(0).getExecutorType().getRegionReplicaSet().getRegionId().getType()==DataRegion
        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        if(queryStateManager.getRootIdentitySinkId()!=null && queryStateManager.getRootIdentitySinkId().equals(operatorContext.getPlanNodeId().getId())) {
            if (queryStateManager.getStateMachine().getState() == ColQueryState.PRE_COL_QUERY) {
                this.colSinkHandle = queryStateManager.getSinkHandle();
                colSinkHandle.tryOpenChannel(0);
                queryStateManager.getStateMachine().transitionToColQuery();
            }
        }
    }
    int currentIndex = downStreamChannelIndex.getCurrentIndex();
    boolean currentChannelClosed = sinkHandle.isChannelClosed(currentIndex);
    if (!currentChannelClosed && children.get(currentIndex).hasNextWithTimer()) {
      return true;
    } else if (currentChannelClosed) {
      // we close the child directly. The child could be an ExchangeOperator which is the downstream
      // of an ISinkChannel of a pipeline driver.
      closeCurrentChild(currentIndex);
//                    System.out.println("不会是在这结束的吧。。。else-if");

    } else {
      // current child has no more data
        if(QueryStateManager.isInitialized()){
            QueryStateManager queryStateManager = QueryStateManager.getInstance();
            if(queryStateManager.getRootIdentitySinkId()!=null && queryStateManager.getRootIdentitySinkId().equals(operatorContext.getPlanNodeId().getId())){
                if(queryStateManager.getStateMachine().getState() == ColQueryState.COL_QUERY){
                    System.out.println("\n要结束啦！");
                    colSinkHandle.setNoMoreTsBlocksOfOneChannel(0);
                    System.out.println("\ncolSinkHandle closed");
                    queryStateManager.getStateMachine().transitionToPreClosed();
                    //调用关闭函数
                  if(queryStateManager.isSingleScan()){
                      String planNodeId = queryStateManager.getAllScanPlanNodeIdList().get(0);
                      QueryStateManager.ScanStates scanStates = queryStateManager.getAllScanStatesList().get(0);
                      long offset = scanStates.getOffset();
                      String seriesPath = queryStateManager.getSeriesPath(planNodeId);
                      callColQueryCloseWithSingleScan(planNodeId,offset,seriesPath,false);
                  }else {
                      List<QueryStateManager.ScanStates>  scanStates = queryStateManager.getAllScanStatesList();
                      List<String> seriesPaths = queryStateManager.getAllScanPathList();
                      List<String> planNodeIds = queryStateManager.getAllScanPlanNodeIdList();
                      int i=0;
                      Map<String, ScanInfo> scanInfoMap = new HashMap<>();
                      for(QueryStateManager.ScanStates scanState:scanStates)
                      {
                          ScanInfo scanInfo = ScanInfoConverter.convertToScanInfo(scanState,seriesPaths.get(i));
                          scanInfoMap.put(planNodeIds.get(i),scanInfo);
                          i++;
                      }
                      if(queryStateManager.hasLeftOuterJoin()){
                          TsBlock cache = queryStateManager.getLeftOuterJoinCache();
                          ScanInfoConverter.TsBlockColumns valueColumns=ScanInfoConverter.convertTsBlockToColumns(cache);
                          callColQueryCloseWithLeftOuterJoin(scanInfoMap,valueColumns.getTimeColumn(),valueColumns.getValueColumns(),queryStateManager.getIsRightCache());
                      }else {
                          callColQueryClose(scanInfoMap);
                      }
                  }
                    queryStateManager.getStateMachine().transitionToClosed();
                }
            }
        }
      closeCurrentChild(currentIndex);
      sinkHandle.setNoMoreTsBlocksOfOneChannel(downStreamChannelIndex.getCurrentIndex());

    }

    // increment the index
    currentIndex++;
    if (currentIndex >= children.size()) {
      isFinished = true;
      return false;
    }
    downStreamChannelIndex.setCurrentIndex(currentIndex);
    // if we reach here, it means that isBlocked() is called on a different child
    // we need to ensure that this child is not blocked. We set this field to true here so that we
    // can begin another loop in Driver.
    needToReturnNull = true;
    // tryOpenChannel first
    sinkHandle.tryOpenChannel(currentIndex);
    return true;
  }

  private void closeCurrentChild(int index) throws Exception {
    children.get(index).close();
    children.set(index, null);
  }

  @Override
  public TsBlock next() throws Exception {
    if(QueryStateManager.isInitialized()){
        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        System.out.println("\n怀疑是IdentitySink的问题"+queryStateManager.getRootIdentitySinkId()+"\n");
        if(queryStateManager.getRootIdentitySinkId()!=null){
            System.out.println();
        }
        if(queryStateManager.getRootIdentitySinkId()!=null &&
                queryStateManager.getRootIdentitySinkId().equals(operatorContext.getPlanNodeId().getId())){
            System.out.println("\n找到了，状态机状态为："+queryStateManager.getStateMachine().getState()+"\n");
            if(queryStateManager.getStateMachine().getState()== ColQueryState.COL_QUERY){
                if (needToReturnNull) {
                    needToReturnNull = false;
//                    System.out.println("不会是在这结束的吧。。。");
                    return null;
                }
                TsBlock res = children.get(downStreamChannelIndex.getCurrentIndex()).nextWithTimer();
                //TODO:开始发送数据
                System.out.println("\n要开始发送啦！");
                if(res!=null && res.getPositionCount()!=0 && !colSinkHandle.isAborted()){
                    try {
                        Thread.sleep(2);
                        //          System.out.println("waiting");
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    colSinkHandle.send(res);//发送数据
                    System.out.println(showTsBlock(res));
                    System.out.println("\nseries scan send");
                }
                return res;
            }
        }
    }

    if (needToReturnNull) {
      needToReturnNull = false;
      return null;
    }
    return children.get(downStreamChannelIndex.getCurrentIndex()).nextWithTimer();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return children.get(downStreamChannelIndex.getCurrentIndex()).isBlocked();
  }

  @Override
  public boolean isFinished() throws Exception {
    return isFinished;
  }

  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public void close() throws Exception {
    for (int i = downStreamChannelIndex.getCurrentIndex(), n = children.size(); i < n; i++) {
      Operator currentChild = children.get(i);
      if (currentChild != null) {
        currentChild.close();
      }
    }
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;
    for (Operator child : children) {
      maxPeekMemory = Math.max(maxPeekMemory, child.calculateMaxPeekMemoryWithCounter());
    }
    return maxPeekMemory;
  }

  @Override
  public long calculateMaxReturnSize() {
    long maxReturnSize = 0;
    for (Operator child : children) {
      maxReturnSize = Math.max(maxReturnSize, child.calculateMaxReturnSize());
    }
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return 0L;
  }

  @TestOnly
  public List<Operator> getChildren() {
    return children;
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + children.stream()
            .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
            .sum()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sinkHandle);
  }

  public void callColQueryClose(Map<String, ScanInfo> scanInfoMap) throws TException{
      try (TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9090))) {
          TProtocol protocol = new TBinaryProtocol(transport);
          C2EColService.Client client = new C2EColService.Client(protocol);
          transport.open();
          // 调用服务方法

          client.ColQueryClose(scanInfoMap);
//            System.out.println("ansData:"+SourceId+" sent successfully.");
      } catch (TException x) {
          x.printStackTrace();
      }
  }

  public void callColQueryCloseWithLeftOuterJoin(Map<String, ScanInfo> scanInfoMap, TimeColumn timeColumn, List<Column> valueColumns,boolean isRightCache) throws TException{
      try (TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9090))) {
          TProtocol protocol = new TBinaryProtocol(transport);
          C2EColService.Client client = new C2EColService.Client(protocol);
          transport.open();
          // 调用服务方法

          client.ColQueryCloseWithLeftOuterJoin(scanInfoMap,timeColumn,valueColumns,isRightCache);
//            System.out.println("ansData:"+SourceId+" sent successfully.");
      } catch (TException x) {
          x.printStackTrace();
      }
  }

  public void callColQueryCloseWithSingleScan(String planNodeId, long offset, String seriesPath, boolean isCloudEqual) throws TException{
      try (TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9090))) {
          TProtocol protocol = new TBinaryProtocol(transport);
          C2EColService.Client client = new C2EColService.Client(protocol);
          transport.open();
          // 调用服务方法

          client.ColQueryCloseWithSingleScan(planNodeId,offset,seriesPath,isCloudEqual);
//            System.out.println("ansData:"+SourceId+" sent successfully.");
      } catch (TException x) {
          x.printStackTrace();
      }
  }

    private String showTsBlock(TsBlock tsBlock) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n！！！准备发送当前的TsBlock为:\n");
        // We keep the whole dump under read lock to keep a consistent snapshot
//        lock.readLock().lock();
        try {
            sb.append("  Identity Sink TsBlock: present\n");
            final int rowCount = tsBlock.getPositionCount();
            final org.apache.tsfile.block.column.Column[] valueColumns = tsBlock.getValueColumns();
            final int colCount = valueColumns == null ? 0 : valueColumns.length;
            sb.append("    rows: ").append(rowCount).append(", valueColumns: ").append(colCount).append("\n");

            // time column
            long[] times = tsBlock.getTimeColumn() == null ? null : tsBlock.getTimeColumn().getTimes();
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
        return sb.toString();
    }

    static class WaitForClose implements Runnable {
        @Override
        public void run() {
            QueryStateManager qsm = QueryStateManager.getInstance();
            while(!qsm.getSinkHandle().getChannel(0).isFinished()){
                try {
                    Thread.sleep(10);
                    System.out.println("waiting 123");
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            qsm.getSinkHandle().close();
        }
    }
}
