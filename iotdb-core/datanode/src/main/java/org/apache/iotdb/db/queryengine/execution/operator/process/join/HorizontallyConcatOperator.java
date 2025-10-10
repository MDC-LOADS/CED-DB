/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process.join;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQuerySessions;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState;
import org.apache.iotdb.db.queryengine.execution.colquery.QueryStateManager;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.AbstractConsumeAllOperator;

import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanUtil;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This operator is used to horizontally concatenate TsBlocks with the same timestamp column.
 *
 * <p>For example, TsBlock A is: [1, 1.0; 2, 2.0], TsBlock B is: [1, true; 2, false]
 *
 * <p>HorizontallyConcat(A,B) is: [1, 1.0, true; 2, 2.0, false]
 */
public class HorizontallyConcatOperator extends AbstractConsumeAllOperator {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(HorizontallyConcatOperator.class);

  /** Start index for each input TsBlocks and size of it is equal to inputTsBlocks. */
  private final int[] inputIndex;

  private final TsBlockBuilder tsBlockBuilder;

  private boolean finished;

  private final List<String> childScanPaths;

  public HorizontallyConcatOperator(
      OperatorContext operatorContext, List<Operator> children, List<TSDataType> dataTypes) {
    super(operatorContext, children);
    checkArgument(
        !children.isEmpty(), "child size of VerticallyConcatOperator should be larger than 0");
    this.inputIndex = new int[this.inputOperatorsCount];
    this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
    this.childScanPaths = new ArrayList<>();
    for (int i = 0; i < inputOperatorsCount; i++) {
      String scanPath = extractSeriesPathFromChild(children.get(i), i);
      if(scanPath!=null){
        this.childScanPaths.add(scanPath);
      }
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (!prepareInput()) {
      return null;
    }
    tsBlockBuilder.reset();
    // Indicates how many rows can be built in this calculate
    int maxRowCanBuild = Integer.MAX_VALUE;
    for (int i = 0; i < inputOperatorsCount; i++) {
      maxRowCanBuild =
          Math.min(maxRowCanBuild, inputTsBlocks[i].getPositionCount() - inputIndex[i]);
    }

    TimeColumn firstTimeColumn = inputTsBlocks[0].getTimeColumn();
    TimeColumnBuilder timeColumnBuilder = tsBlockBuilder.getTimeColumnBuilder();
    ColumnBuilder[] valueColumnBuilders = tsBlockBuilder.getValueColumnBuilders();

    // Build TimeColumn according to the first inputTsBlock
    int currTsBlockIndex = inputIndex[0];
    for (int row = 0; row < maxRowCanBuild; row++) {
      timeColumnBuilder.writeLong(firstTimeColumn.getLong(currTsBlockIndex + row));
      tsBlockBuilder.declarePosition();
    }

    // Build ValueColumns according to inputTsBlocks
    int valueBuilderIndex = 0; // Indicate which valueColumnBuilder should use
    for (int i = 0; i < inputOperatorsCount; i++) {
      currTsBlockIndex = inputIndex[i];
      for (Column column : inputTsBlocks[i].getValueColumns()) {
        for (int row = 0; row < maxRowCanBuild; row++) {
          if (column.isNull(currTsBlockIndex + row)) {
            valueColumnBuilders[valueBuilderIndex].appendNull();
          } else {
            valueColumnBuilders[valueBuilderIndex].write(column, currTsBlockIndex + row);
          }
        }
        valueBuilderIndex++;
      }
      inputIndex[i] += maxRowCanBuild;
    }
    TsBlock res = tsBlockBuilder.build();
    long endTime = res.getEndTime();
    updateScanStates(endTime);
    return res;
  }

  @Override
  public boolean hasNext() throws Exception {
    QueryStateManager queryStateManager = getSession();
    if(queryStateManager != null){
      if(queryStateManager.getStateMachine().getState()== ColQueryState.PRE_CLOSED
              && !queryStateManager.getOperatorClearManager().isCleared("HCJoin")){
        //清空全部中间状态
        Arrays.fill(inputIndex, 0);
        inputTsBlocks = new TsBlock[inputOperatorsCount];
        retainedTsBlock = null;
        startOffset=0;
        for (int i = 0; i < inputOperatorsCount; i++) {
          canCallNext[i] = false;
        }
        currentChildIndex = 0;
        hasEmptyChildInput = false;
        tsBlockBuilder.reset();
        queryStateManager.getOperatorClearManager().clearOperator(getColQueryId(),"HCJoin");
      }
    }
    if (finished) {
      return false;
    }
    return !isEmpty(readyChildIndex)
        || (children.get(readyChildIndex) != null
            && children.get(readyChildIndex).hasNextWithTimer());
  }

  @Override
  public boolean isFinished() throws Exception {
    if (finished) {
      return true;
    }
    finished =
        isEmpty(readyChildIndex)
            && (children.get(readyChildIndex) == null
                || !children.get(readyChildIndex).hasNextWithTimer());
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = 0;
    long childrenMaxPeekMemory = 0;
    for (Operator child : children) {
      childrenMaxPeekMemory =
          Math.max(
              childrenMaxPeekMemory, maxPeekMemory + child.calculateMaxPeekMemoryWithCounter());
      maxPeekMemory +=
          (child.calculateMaxReturnSize() + child.calculateRetainedSizeAfterCallingNext());
    }

    maxPeekMemory += calculateMaxReturnSize();
    return Math.max(maxPeekMemory, childrenMaxPeekMemory);
  }

  @Override
  public long calculateMaxReturnSize() {
    return children.stream().mapToLong(Operator::calculateMaxReturnSize).sum();
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    long currentRetainedSize = 0;
    long minChildReturnSize = Long.MAX_VALUE;
    for (Operator child : children) {
      long maxReturnSize = child.calculateMaxReturnSize();
      currentRetainedSize += (maxReturnSize + child.calculateRetainedSizeAfterCallingNext());
      minChildReturnSize = Math.min(minChildReturnSize, maxReturnSize);
    }
    // max cached TsBlock
    return currentRetainedSize - minChildReturnSize;
  }

  /**
   * If the tsBlock of tsBlockIndex is null or has no more data in the tsBlock, return true; else
   * return false.
   */
  @Override
  protected boolean isEmpty(int tsBlockIndex) {
    return inputTsBlocks[tsBlockIndex] == null
        || inputTsBlocks[tsBlockIndex].getPositionCount() == inputIndex[tsBlockIndex];
  }

  @Override
  protected TsBlock getNextTsBlock(int childIndex) throws Exception {
    inputIndex[childIndex] = 0;
    return children.get(childIndex).nextWithTimer();
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE
        + children.stream()
            .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
            .sum()
        + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
        + RamUsageEstimator.sizeOf(inputIndex)
        + RamUsageEstimator.sizeOf(canCallNext)
        + tsBlockBuilder.getRetainedSizeInBytes();
  }

  private void updateScanStates(long endTime) {

    if (getSession() == null) {
      return;
    }
    QueryStateManager stateManager = getSession();
    for(int i = 0; i < inputOperatorsCount; i++){
      if (i >= childScanPaths.size()) {
        continue;
      }

      String scanPath = childScanPaths.get(i);
      if (scanPath == null || scanPath.isEmpty()) {
        continue;
      }

      // Get or create scan states for this path
      QueryStateManager.ScanStates scanStates = stateManager.getScanStates(scanPath);
      if (scanStates == null) {
        scanStates = new QueryStateManager.ScanStates();
        stateManager.setScanStates(scanPath, scanStates);
      }
      scanStates.setOffset(endTime+stateManager.getTimeRangeSize());
      scanStates.setCouldEqual(true);
    }
    stateManager.setHorizontal(true);
//    System.out.println(stateManager.getStateSummary());
  }

  private String extractSeriesPathFromChild(Operator childOperator, int childIndex) {
    // Check if child operator is an ExchangeOperator that contains SourceId -> SeriesScanUtil
    if (childOperator instanceof ExchangeOperator) {
      ExchangeOperator sourceOperator = (ExchangeOperator) childOperator;
      // Access the sourceOperator field using reflection to get sourceId
      try {
        java.lang.reflect.Field sourceIdField = ExchangeOperator.class.getDeclaredField("sourceId");
        sourceIdField.setAccessible(true);
        Object sourceId = sourceIdField.get(sourceOperator);
        if (sourceId != null) {
          String planNodeId = sourceId.toString();
          QueryStateManager stateManager = getSession();
          if(stateManager.getSeriesPath(planNodeId) != null) {
            stateManager.updateScanInnerJoin(planNodeId, true);
            return stateManager.getSeriesPath(planNodeId);
          }
        }
      } catch (Exception e) {
        // Log the exception and handle accordingly
      }
    } else if (childOperator instanceof AbstractDataSourceOperator) {
      AbstractDataSourceOperator dataSourceOperator = (AbstractDataSourceOperator) childOperator;
      // Access the seriesScanUtil field using reflection to get seriesPath
      try {
        java.lang.reflect.Field seriesScanUtilField = AbstractDataSourceOperator.class.getDeclaredField("seriesScanUtil");
        seriesScanUtilField.setAccessible(true);
        SeriesScanUtil seriesScanUtil = (SeriesScanUtil) seriesScanUtilField.get(dataSourceOperator);
        if (seriesScanUtil != null) {
          // Access the seriesPath field from SeriesScanUtil
          java.lang.reflect.Field seriesPathField = SeriesScanUtil.class.getDeclaredField("seriesPath");
          seriesPathField.setAccessible(true);
          Object seriesPathObj = seriesPathField.get(seriesScanUtil);
          if (seriesPathObj != null) {
            return seriesPathObj.toString();
          }
        }
      } catch (Exception e) {
        // Log the exception and fall back to default naming
        // Consider adding proper logging here if needed
      }
    }
    // Fall back to default path naming if SeriesScanUtil is not found or extraction fails
    return "child_" + childIndex + "_" + operatorContext.getPlanNodeId();
  }

  public QueryStateManager getQueryStateManager() {
    return getSession();
  }

  private String getColQueryId() {
    String edgeQueryId = getOperatorContext().getInstanceContext().getId().getQueryId().getId();
    int dataNodeId = org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    return edgeQueryId + "-" + dataNodeId;
  }

  private QueryStateManager getSession() {
    return ColQuerySessions.getByEdgeQueryId(getColQueryId());
  }
}
