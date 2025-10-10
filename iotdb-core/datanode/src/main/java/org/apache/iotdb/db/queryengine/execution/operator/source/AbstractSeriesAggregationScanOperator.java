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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.aggregation.Aggregator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQuerySessions;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState;
import org.apache.iotdb.db.queryengine.execution.colquery.QueryStateManager;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;

import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.calculateAggregationFromRawData;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.isAllAggregatorsHasFinalResult;

public abstract class AbstractSeriesAggregationScanOperator extends AbstractDataSourceOperator {

  protected final boolean ascending;
  protected final boolean isGroupByQuery;

  protected int subSensorSize;

  protected TsBlock inputTsBlock;

  protected final ITimeRangeIterator timeRangeIterator;
  // Current interval of aggregation window [curStartTime, curEndTime)
  protected TimeRange curTimeRange;

  // We still think aggregator in SeriesAggregateScanOperator is a inputRaw step.
  // But in facing of statistics, it will invoke another method processStatistics()
  protected final List<Aggregator> aggregators;

  protected boolean finished = false;

  protected final boolean outputEndTime;

  private final long cachedRawDataSize;

  /** Time slice for one next call in total, shared by the inner methods of the next() method */
  private long leftRuntimeOfOneNextCall;

  /** Some special data types(like BLOB) cannot use statistics. */
  private final boolean canUseStatistics;

  private final long timeRangeSize;

  private long resumeOffsetTimestamp = Long.MIN_VALUE;
  private boolean resumeOffsetInclusive;
  private boolean hasResumeOffset;

  @SuppressWarnings("squid:S107")
  protected AbstractSeriesAggregationScanOperator(
      PlanNodeId sourceId,
      OperatorContext context,
      SeriesScanUtil seriesScanUtil,
      int subSensorSize,
      List<Aggregator> aggregators,
      ITimeRangeIterator timeRangeIterator,
      boolean ascending,
      boolean outputEndTime,
      GroupByTimeParameter groupByTimeParameter,
      long maxReturnSize,
      boolean canUseStatistics) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.ascending = ascending;
    this.isGroupByQuery = groupByTimeParameter != null;
    this.seriesScanUtil = seriesScanUtil;
    this.subSensorSize = subSensorSize;
    this.aggregators = aggregators;
    this.timeRangeIterator = timeRangeIterator;
    this.timeRangeSize = timeRangeIterator.getFirstTimeRange().getMax()-timeRangeIterator.getFirstTimeRange().getMin();
    this.cachedRawDataSize =
        (1L + subSensorSize) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxReturnSize = maxReturnSize;
    this.outputEndTime = outputEndTime;
    this.canUseStatistics = canUseStatistics;
    this.hasResumeOffset = false;
    QueryStateManager stateManager = ColQuerySessions.getByEdgeQueryId(
            operatorContext.getInstanceContext().getId().getQueryId().getId() + "-" +
                    org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getConfig().getDataNodeId());
    if(stateManager != null){
      stateManager.setSeriesPathAndPlanNodeId(this.sourceId.getId(),this.seriesScanUtil.seriesPath.toString());
      stateManager.setScanPathExchangeByPlanNodeId(operatorContext.getPlanNodeId().getId(),
              operatorContext.getDriverContext().getOperatorContexts().size() == 1);
      stateManager.setScanStates(this.seriesScanUtil.seriesPath.toString(),new QueryStateManager.ScanStates());

    }
  }

  @Override
  public long calculateMaxPeekMemory() {
    return cachedRawDataSize + maxReturnSize;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return isGroupByQuery ? cachedRawDataSize : 0;
  }

  @Override
  public boolean hasNext() throws Exception {
    QueryStateManager queryStateManager = getSession();
    if(queryStateManager != null){
      if (queryStateManager.getStateMachine().getState()== ColQueryState.COL_QUERY){
        try {
          queryStateManager.getStateMachine().getStateChange(ColQueryState.COL_QUERY).get();
        }catch (InterruptedException ie){
          Thread.currentThread().interrupt();
        }catch (java.util.concurrent.ExecutionException ee){
        }
      }
      if(queryStateManager.getStateMachine().getState()== ColQueryState.PRE_CLOSED){
//        System.out.println("准备进入清空");
        if(!queryStateManager.getOperatorClearManager().isCleared(sourceId.getId())){
//          System.out.println("完成清空");
          retainedTsBlock = null;
          startOffset = 0;
          inputTsBlock = null;
          //清空管道
          ISourceHandle sourceHandle = queryStateManager.getScanSourceHandle(sourceId.getId());
          if(sourceHandle instanceof LocalSourceHandle && !queryStateManager.isSingleScan()) {
//            System.out.println("清除了Source管道，plan node id为："+sourceId.getId());
            ((LocalSourceHandle) sourceHandle).getSharedTsBlockQueue().fastClear();
          }

          //创建新的series scan util 用于恢复查询
          PartialPath seriesPath = this.seriesScanUtil.seriesPath;
          Ordering scanOrder=this.seriesScanUtil.scanOrder;
          SeriesScanOptions oldScanOptions = this.seriesScanUtil.scanOptions;
          QueryDataSource dataSource =this.seriesScanUtil.dataSource;
          Filter newOffsetFilter;
          QueryStateManager.ScanStates scanStates = queryStateManager.getScanStates(seriesPath.getFullPath());
//          System.out.println("设置新查询的filter的offet为："+scanStates.getOffset());
          curTimeRange = new TimeRange(scanStates.getOffset(),scanStates.getOffset()+timeRangeSize);
          if(scanStates.isCouldEqual()){
            newOffsetFilter = TimeFilterApi.gtEq(scanStates.getOffset());
          }else {
            newOffsetFilter = TimeFilterApi.gt(scanStates.getOffset());
          }
          Filter existingFilter = oldScanOptions.getGlobalTimeFilter();
          Filter combinedFilter = null;
          if (existingFilter != null) {
            combinedFilter = FilterFactory.and(existingFilter, newOffsetFilter);
//          System.out.println("组合现有过滤器和新timestamp过滤器");
          } else {
            combinedFilter = newOffsetFilter;
//          System.out.println("使用新timestamp过滤器作为globalTimeFilter");
          }
          // 创建新的SeriesScanOptions
          SeriesScanOptions.Builder builder = new SeriesScanOptions.Builder();
          SeriesScanOptions newScanOptions = builder
                  .withGlobalTimeFilter(combinedFilter)
                  .withPushDownFilter(oldScanOptions.getPushDownFilter())
                  .build();
//          if(oldScanOptions.pushDownLimit!=0){
//            System.out.println("pushDownLimit不为0:"+oldScanOptions.pushDownLimit);
//          }
          builder.withAllSensors(oldScanOptions.getAllSensors());
          newScanOptions = builder.build();
          FragmentInstanceContext context = this.seriesScanUtil.context;
          this.seriesScanUtil = new SeriesScanUtil(seriesPath, scanOrder, newScanOptions, context,dataSource);
          inputTsBlock = null;
          curTimeRange = null;
          hasResumeOffset = false;
          if (!aggregators.isEmpty()) {
            aggregators.forEach(Aggregator::reset);
          }
          if (resultTsBlockBuilder != null) {
            resultTsBlockBuilder.reset();
          }
          updateResumeOffset(scanStates);
          queryStateManager.getOperatorClearManager().clearOperator(getColQueryId(),operatorContext.getPlanNodeId().getId());
        }
      }
    }
    return curTimeRange != null || timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() throws Exception {

//    try{
//      Thread.sleep(20);
//      System.out.println("stop scan 2s");
//    }catch (InterruptedException e){
//      e.printStackTrace();
//    }

    // start stopwatch, reset leftRuntimeOfOneNextCall
    long start = System.nanoTime();
    leftRuntimeOfOneNextCall = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long maxRuntime = leftRuntimeOfOneNextCall;

    while (System.nanoTime() - start < maxRuntime
        && (curTimeRange != null || timeRangeIterator.hasNextTimeRange())
        && !resultTsBlockBuilder.isFull()) {
      if (curTimeRange == null) {
        // move to the next time window
        curTimeRange = timeRangeIterator.nextTimeRange();
        // clear previous aggregation result
        for (Aggregator aggregator : aggregators) {
          aggregator.reset();
        }
      }

      if (shouldSkipCurrentTimeRange()) {
        curTimeRange = null;
        continue;
      } else if (hasResumeOffset) {
        hasResumeOffset = false;
      }

      // calculate aggregation result on current time window
      // Keep curTimeRange if the calculation of this timeRange is not done
      if (calculateAggregationResultForCurrentTimeRange()) {
        curTimeRange = null;
      }
    }

    if (resultTsBlockBuilder.getPositionCount() > 0) {
      TsBlock resultTsBlock = resultTsBlockBuilder.build();
      resultTsBlockBuilder.reset();
      setScanTimestamp(resultTsBlock);
      return resultTsBlock;
    } else {
      return null;
    }
  }

  private void setScanTimestamp(TsBlock res) {
    QueryStateManager queryStateManager = getSession();
    if(queryStateManager != null){
//      System.out.println("待设置偏移量 Ascan，id为"+sourceId.getId());
      if(queryStateManager.isHasSeriesPath(sourceId.getId())
              && !queryStateManager.isScanPathExchangeByPlanNodeId(sourceId.getId())) {
        long currentEndTime = res.getEndTime();
        queryStateManager.updateScanTimestampByPlanNodeId(sourceId.getId(),currentEndTime+timeRangeSize);
        queryStateManager.updateScanCouldEqualByPlanNodeId(sourceId.getId(),true);
//        System.out.println("设置了偏移量 Ascan，id为"+sourceId.getId());
      }
      if(queryStateManager.getTimeRangeSize()==0){
        queryStateManager.setTimeRangeSize(timeRangeSize);
      }
    }
  }

  private void updateResumeOffset(QueryStateManager.ScanStates scanStates) {
    if (scanStates == null) {
      hasResumeOffset = false;
      resumeOffsetTimestamp = Long.MIN_VALUE;
      resumeOffsetInclusive = false;
      return;
    }
    resumeOffsetTimestamp = scanStates.getOffset();
    resumeOffsetInclusive = scanStates.isCouldEqual();
    hasResumeOffset = true;
  }

  @Override
  public boolean isFinished() throws Exception {
    if (!finished) {
      finished = !hasNextWithTimer();
    }
    return finished;
  }

  private boolean shouldSkipCurrentTimeRange() {
    if (!hasResumeOffset) {
      return false;
    }
    long outputTime = timeRangeIterator.currentOutputTime();
    return resumeOffsetInclusive ? outputTime < resumeOffsetTimestamp : outputTime <= resumeOffsetTimestamp;
  }

  @SuppressWarnings("squid:S112")
  /** Return true if we have the result of this timeRange. */
  protected boolean calculateAggregationResultForCurrentTimeRange() {
    try {
      if (calcFromCachedData()) {
        updateResultTsBlock();
        return true;
      }

      if (readAndCalcFromPage()) {
        updateResultTsBlock();
        return true;
      }

      // only when all the page data has been consumed, we need to read the chunk data
      if (!seriesScanUtil.hasNextPage() && readAndCalcFromChunk()) {
        updateResultTsBlock();
        return true;
      }

      // only when all the page and chunk data has been consumed, we need to read the file data
      if (!seriesScanUtil.hasNextPage()
          && !seriesScanUtil.hasNextChunk()
          && readAndCalcFromFile()) {
        updateResultTsBlock();
        return true;
      }

      // If the TimeRange is (Long.MIN_VALUE, Long.MAX_VALUE), for Aggregators like countAggregator,
      // we have to consume all the data before we finish the aggregation calculation.
      if (seriesScanUtil.hasNextPage()
          || seriesScanUtil.hasNextChunk()
          || seriesScanUtil.hasNextFile()) {
        return false;
      }
      updateResultTsBlock();
      return true;
    } catch (IOException e) {
      throw new RuntimeException("Error while scanning the file", e);
    }
  }

  protected void updateResultTsBlock() {
    if (!outputEndTime) {
      appendAggregationResult(
          resultTsBlockBuilder, aggregators, timeRangeIterator.currentOutputTime());
    } else {
      appendAggregationResult(
          resultTsBlockBuilder,
          aggregators,
          timeRangeIterator.currentOutputTime(),
          curTimeRange.getMax());
    }
  }

  protected boolean calcFromCachedData() {
    return calcFromRawData(inputTsBlock);
  }

  private boolean calcFromRawData(TsBlock tsBlock) {
    Pair<Boolean, TsBlock> calcResult =
        calculateAggregationFromRawData(tsBlock, aggregators, curTimeRange, ascending);
    inputTsBlock = calcResult.getRight();
    return calcResult.getLeft();
  }

  protected void calcFromStatistics(Statistics timeStatistics, Statistics[] valueStatistics) {
    for (Aggregator aggregator : aggregators) {
      if (aggregator.hasFinalResult()) {
        continue;
      }
      aggregator.processStatistics(timeStatistics, valueStatistics);
    }
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromFile() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextFile()) {
      if (canUseStatistics && seriesScanUtil.canUseCurrentFileStatistics()) {
        Statistics fileTimeStatistics = seriesScanUtil.currentFileTimeStatistics();
        if (fileTimeStatistics.getStartTime() > curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentFile();
            continue;
          }
        }
        // calc from fileMetaData
        if (curTimeRange.contains(
            fileTimeStatistics.getStartTime(), fileTimeStatistics.getEndTime())) {
          Statistics[] statisticsList = new Statistics[subSensorSize];
          for (int i = 0; i < subSensorSize; i++) {
            statisticsList[i] = seriesScanUtil.currentFileStatistics(i);
          }
          calcFromStatistics(fileTimeStatistics, statisticsList);
          seriesScanUtil.skipCurrentFile();
          if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
            return true;
          } else {
            continue;
          }
        }
      }

      // read chunk
      if (readAndCalcFromChunk()) {
        return true;
      }
    }

    return false;
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromChunk() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextChunk()) {
      if (canUseStatistics && seriesScanUtil.canUseCurrentChunkStatistics()) {
        Statistics chunkTimeStatistics = seriesScanUtil.currentChunkTimeStatistics();
        if (chunkTimeStatistics.getStartTime() > curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentChunk();
            continue;
          }
        }
        // calc from chunkMetaData
        if (curTimeRange.contains(
            chunkTimeStatistics.getStartTime(), chunkTimeStatistics.getEndTime())) {
          // calc from chunkMetaData
          Statistics[] statisticsList = new Statistics[subSensorSize];
          for (int i = 0; i < subSensorSize; i++) {
            statisticsList[i] = seriesScanUtil.currentChunkStatistics(i);
          }
          calcFromStatistics(chunkTimeStatistics, statisticsList);
          seriesScanUtil.skipCurrentChunk();
          if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
            return true;
          } else {
            continue;
          }
        }
      }

      // read page
      if (readAndCalcFromPage()) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromPage() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    try {
      while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextPage()) {
        if (canUseStatistics && seriesScanUtil.canUseCurrentPageStatistics()) {
          Statistics pageTimeStatistics = seriesScanUtil.currentPageTimeStatistics();
          // There is no more eligible points in current time range
          if (pageTimeStatistics.getStartTime() > curTimeRange.getMax()) {
            if (ascending) {
              return true;
            } else {
              seriesScanUtil.skipCurrentPage();
              continue;
            }
          }
          // can use pageHeader
          if (curTimeRange.contains(
              pageTimeStatistics.getStartTime(), pageTimeStatistics.getEndTime())) {
            Statistics[] statisticsList = new Statistics[subSensorSize];
            for (int i = 0; i < subSensorSize; i++) {
              statisticsList[i] = seriesScanUtil.currentPageStatistics(i);
            }
            calcFromStatistics(pageTimeStatistics, statisticsList);
            seriesScanUtil.skipCurrentPage();
            if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
              return true;
            } else {
              continue;
            }
          }
        }

        // calc from page data
        TsBlock tsBlock = seriesScanUtil.nextPage();
        if (tsBlock == null || tsBlock.isEmpty()) {
          continue;
        }

        // calc from raw data
        if (calcFromRawData(tsBlock)) {
          return true;
        }
      }
      return false;
    } finally {
      leftRuntimeOfOneNextCall -= (System.nanoTime() - start);
    }
  }

  @Override
  protected List<TSDataType> getResultDataTypes() {
    List<TSDataType> dataTypes = new ArrayList<>();
    if (outputEndTime) {
      dataTypes.add(TSDataType.INT64);
    }
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    return dataTypes;
  }

  private String getColQueryId() {
    String edgeQueryId = operatorContext.getInstanceContext().getId().getQueryId().getId();
    int dataNodeId = org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    return edgeQueryId + "-" + dataNodeId;
  }

  private QueryStateManager getSession() {
    return ColQuerySessions.getByEdgeQueryId(getColQueryId());
  }
}
