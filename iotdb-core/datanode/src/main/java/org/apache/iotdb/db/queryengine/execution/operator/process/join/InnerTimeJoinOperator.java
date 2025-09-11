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

package org.apache.iotdb.db.queryengine.execution.operator.process.join;

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState;
import org.apache.iotdb.db.queryengine.execution.colquery.QueryStateManager;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQuerySessions;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.successfulAsList;

public class InnerTimeJoinOperator implements ProcessOperator {

    private static final long INSTANCE_SIZE =
            RamUsageEstimator.shallowSizeOfInstance(InnerTimeJoinOperator.class);

    private final OperatorContext operatorContext;

    private final long maxReturnSize =
            TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

    /** Start index for each input TsBlocks and size of it is equal to inputTsBlocks. */
    private final int[] inputIndex;

    private final List<Operator> children;
    private final int inputOperatorsCount;

    /** TsBlock from child operator. Only one cache now. */
    private final TsBlock[] inputTsBlocks;

    private final boolean[] canCallNext;

    private final TsBlockBuilder resultBuilder;

    private final TimeComparator comparator;

    private final Map<InputLocation, Integer> outputColumnMap;

    /** Index of the child that is currently fetching input */
    private int currentChildIndex = 0;

    /** Indicate whether we found an empty child input in one loop */
    private boolean hasEmptyChildInput = false;

    /** Mapping from child operator index to scan path */
    private final List<String> childScanPaths;

    public InnerTimeJoinOperator(
            OperatorContext operatorContext,
            List<Operator> children,
            List<TSDataType> dataTypes,
            TimeComparator comparator,
            Map<InputLocation, Integer> outputColumnMap) {
        this(operatorContext, children, dataTypes, comparator, outputColumnMap, new ArrayList<>());
    }

    public InnerTimeJoinOperator(
            OperatorContext operatorContext,
            List<Operator> children,
            List<TSDataType> dataTypes,
            TimeComparator comparator,
            Map<InputLocation, Integer> outputColumnMap,
            List<String> childScanPaths) {
        this.operatorContext = operatorContext;
        this.children = children;
        this.inputOperatorsCount = children.size();
        this.inputTsBlocks = new TsBlock[inputOperatorsCount];
        this.canCallNext = new boolean[inputOperatorsCount];
        checkArgument(
                children.size() > 1, "child size of InnerTimeJoinOperator should be larger than 1");
        this.inputIndex = new int[this.inputOperatorsCount];
        this.resultBuilder = new TsBlockBuilder(dataTypes);
        this.comparator = comparator;
        this.outputColumnMap = outputColumnMap;
        this.childScanPaths = childScanPaths != null ? childScanPaths : new ArrayList<>();

        // Initialize default scan paths if not provided
        if (this.childScanPaths.isEmpty()) {
            for (int i = 0; i < inputOperatorsCount; i++) {
                String scanPath = extractSeriesPathFromChild(children.get(i), i);
                this.childScanPaths.add(scanPath);
            }
        }
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
        boolean hasReadyChild = false;
        List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
        for (int i = 0; i < inputOperatorsCount; i++) {
            if (!isEmpty(i)) {
                continue;
            }
            ListenableFuture<?> blocked = children.get(i).isBlocked();
            if (blocked.isDone()) {
                hasReadyChild = true;
                canCallNext[i] = true;
            } else {
                listenableFutures.add(blocked);
            }
        }
        return (hasReadyChild || listenableFutures.isEmpty())
                ? NOT_BLOCKED
                : successfulAsList(listenableFutures);
    }

    @Override
    public TsBlock next() throws Exception {
        // start stopwatch
        long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
        long start = System.nanoTime();
        if (!prepareInput(start, maxRuntime)) {
            return null;
        }

        // still have time
        if (System.nanoTime() - start < maxRuntime) {
            // End time for returned TsBlock this time, it's the min/max end time among all the children
            // TsBlocks order by asc/desc
            long currentEndTime = 0;
            boolean init = false;

            // Get TsBlock for each input, put their time stamp into TimeSelector and then use the min
            // Time
            // among all the input TsBlock as the current output TsBlock's endTime.
            for (int i = 0; i < inputOperatorsCount; i++) {
                // Update the currentEndTime if the TsBlock is not empty
                currentEndTime =
                        init
                                ? comparator.getCurrentEndTime(currentEndTime, inputTsBlocks[i].getEndTime())
                                : inputTsBlocks[i].getEndTime();
                init = true;
            }

            // collect time that each child has
            int[][] selectedRowIndexArray = buildTimeColumn(currentEndTime);

            // build value columns for each child
            if (selectedRowIndexArray[0].length > 0) {
                for (int i = 0; i < inputOperatorsCount; i++) {
                    buildValueColumns(i, selectedRowIndexArray[i]);
                }
            }
        }

        // set corresponding inputTsBlock to null if its index already reach its size, friendly for gc
        cleanUpInputTsBlock();

        TsBlock res = resultBuilder.build();
        resultBuilder.reset();

        // Update scan states after processing
        updateScanStates();

        return res;
    }

    // return selected row index for each child's tsblock
    private int[][] buildTimeColumn(long currentEndTime) {
        TimeColumnBuilder timeBuilder = resultBuilder.getTimeColumnBuilder();
        List<List<Integer>> selectedRowIndexArray = new ArrayList<>(inputOperatorsCount);
        for (int i = 0; i < inputOperatorsCount; i++) {
            selectedRowIndexArray.add(new ArrayList<>());
        }

        int column0Size = inputTsBlocks[0].getPositionCount();

        while (inputIndex[0] < column0Size
                && comparator.canContinueInclusive(
                inputTsBlocks[0].getTimeByIndex(inputIndex[0]), currentEndTime)) {
            long time = inputTsBlocks[0].getTimeByIndex(inputIndex[0]);
            inputIndex[0]++;
            boolean allHave = true;
            for (int i = 1; i < inputOperatorsCount; i++) {
                int size = inputTsBlocks[i].getPositionCount();
                updateInputIndex(i, time);

                if (inputIndex[i] == size || inputTsBlocks[i].getTimeByIndex(inputIndex[i]) != time) {
                    allHave = false;
                    break;
                } else {
                    inputIndex[i]++;
                }
            }
            if (allHave) {
                timeBuilder.writeLong(time);
                resultBuilder.declarePosition();
                appendOneSelectedRow(selectedRowIndexArray);
            }
        }

        // update inputIndex for each child to the last index larger than currentEndTime
        for (int i = 0; i < inputOperatorsCount; i++) {
            updateInputIndexUntilLargerThan(i, currentEndTime);
        }

        return transformListToIntArray(selectedRowIndexArray);
    }

    private void appendOneSelectedRow(List<List<Integer>> selectedRowIndexArray) {
        for (int i = 0; i < inputOperatorsCount; i++) {
            selectedRowIndexArray.get(i).add(inputIndex[i] - 1);
        }
    }

    private void updateInputIndex(int i, long currentEndTime) {
        int size = inputTsBlocks[i].getPositionCount();
        while (inputIndex[i] < size
                && comparator.lessThan(inputTsBlocks[i].getTimeByIndex(inputIndex[i]), currentEndTime)) {
            inputIndex[i]++;
        }
    }

    private void updateInputIndexUntilLargerThan(int i, long currentEndTime) {
        int size = inputTsBlocks[i].getPositionCount();
        while (inputIndex[i] < size
                && comparator.canContinueInclusive(
                inputTsBlocks[i].getTimeByIndex(inputIndex[i]), currentEndTime)) {
            inputIndex[i]++;
        }
    }

    private void cleanUpInputTsBlock() {
        for (int i = 0; i < inputOperatorsCount; i++) {
            if (inputTsBlocks[i].getPositionCount() == inputIndex[i]) {
                inputTsBlocks[i] = null;
                inputIndex[i] = 0;
            }
        }
    }

    private int[][] transformListToIntArray(List<List<Integer>> lists) {
        if (lists.size() <= 1) {
            throw new IllegalStateException(
                    "Child size of InnerTimeJoinOperator should be larger than 1.");
        }
        int[][] res = new int[lists.size()][lists.get(0).size()];
        for (int i = 0; i < res.length; i++) {
            List<Integer> list = lists.get(i);
            int[] array = res[i];
            if (list.size() != array.length) {
                throw new IllegalStateException("All child should have same time column result!");
            }
            for (int j = 0; j < array.length; j++) {
                array[j] = list.get(j);
            }
        }
        return res;
    }

    private void buildValueColumns(int childIndex, int[] selectedRowIndex) {
        TsBlock tsBlock = inputTsBlocks[childIndex];
        for (int i = 0, size = inputTsBlocks[childIndex].getValueColumnCount(); i < size; i++) {
            ColumnBuilder columnBuilder =
                    resultBuilder.getColumnBuilder(outputColumnMap.get(new InputLocation(childIndex, i)));
            Column column = tsBlock.getColumn(i);
            if (column.mayHaveNull()) {
                for (int rowIndex : selectedRowIndex) {
                    if (column.isNull(rowIndex)) {
                        columnBuilder.appendNull();
                    } else {
                        columnBuilder.write(column, rowIndex);
                    }
                }
            } else {
                for (int rowIndex : selectedRowIndex) {
                    columnBuilder.write(column, rowIndex);
                }
            }
        }
    }

    /**
     * Try to cache one result of each child.
     *
     * @return true if results of all children are ready. Return false if some children is blocked or
     *     return null.
     * @throws Exception errors happened while getting tsblock from children
     */
    private boolean prepareInput(long start, long maxRuntime) throws Exception {
        while (System.nanoTime() - start < maxRuntime && currentChildIndex < inputOperatorsCount) {
            if (!isEmpty(currentChildIndex)) {
                currentChildIndex++;
                continue;
            }
            if (canCallNext[currentChildIndex]) {
                if (children.get(currentChildIndex).hasNextWithTimer()) {
                    inputIndex[currentChildIndex] = 0;
                    inputTsBlocks[currentChildIndex] = children.get(currentChildIndex).nextWithTimer();
                    canCallNext[currentChildIndex] = false;
                    // child operator has next but return an empty TsBlock which means that it may not
                    // finish calculation in given time slice.
                    // In such case, TimeJoinOperator can't go on calculating, so we just return null.
                    // We can also use the while loop here to continuously call the hasNext() and next()
                    // methods of the child operator until its hasNext() returns false or the next() gets
                    // the data that is not empty, but this will cause the execution time of the while loop
                    // to be uncontrollable and may exceed all allocated time slice
                    if (isEmpty(currentChildIndex)) {
                        hasEmptyChildInput = true;
                    }
                } else {
                    return false;
                }
            } else {
                hasEmptyChildInput = true;
            }
            currentChildIndex++;
        }

        if (currentChildIndex == inputOperatorsCount) {
            // start a new loop
            currentChildIndex = 0;
            if (!hasEmptyChildInput) {
                // all children are ready now
                return true;
            } else {
                // In a new loop, previously empty child input could be non-empty now, and we can skip the
                // children that have generated input
                hasEmptyChildInput = false;
            }
        }
        return false;
    }

    @Override
    public boolean hasNext() throws Exception {
        QueryStateManager queryStateManager = getSession();
        if(queryStateManager != null && queryStateManager.getStateMachine().getState()== ColQueryState.PRE_CLOSED
                && !queryStateManager.getOperatorClearManager().isCleared("InnerJoin")) {
            //清空全部中间状态
            Arrays.fill(inputIndex, 0);
            Arrays.fill(inputTsBlocks, null);
            Arrays.fill(canCallNext,false);
            resultBuilder.reset();
            currentChildIndex = 0;
            hasEmptyChildInput = false;
            queryStateManager.getOperatorClearManager().clearOperator(getColQueryId(),"InnerJoin");
        }

        // return false if any child is consumed up.
        for (int i = 0; i < inputOperatorsCount; i++) {
            if (isEmpty(i) && canCallNext[i] && !children.get(i).hasNextWithTimer()) {
                return false;
            }
        }
        // return true if all children still hava data
        return true;
    }

    @Override
    public void close() throws Exception {
        for (int i = 0; i < inputOperatorsCount; i++) {
            if (children.get(i) != null) {
                children.get(i).close();
            }
        }
    }

    @Override
    public boolean isFinished() throws Exception {
        // return true if any child is finished.
        for (int i = 0; i < inputOperatorsCount; i++) {
            if (isEmpty(i) && children.get(i).isFinished()) {
                return true;
            }
        }
        // return false if all children still hava data
        return false;
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
        return maxReturnSize;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
        long currentRetainedSize = 0;
        long minChildReturnSize = Long.MAX_VALUE;
        for (Operator child : children) {
            long tmpMaxReturnSize = child.calculateMaxReturnSize();
            currentRetainedSize += (tmpMaxReturnSize + child.calculateRetainedSizeAfterCallingNext());
            minChildReturnSize = Math.min(minChildReturnSize, tmpMaxReturnSize);
        }
        // max cached TsBlock
        return currentRetainedSize - minChildReturnSize;
    }

    @Override
    public long ramBytesUsed() {
        return INSTANCE_SIZE
                + children.stream()
                .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
                .sum()
                + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
                + RamUsageEstimator.sizeOf(canCallNext)
                + RamUsageEstimator.sizeOf(inputIndex)
                + resultBuilder.getRetainedSizeInBytes();
    }

    /**
     * If the tsBlock of columnIndex is null or has no more data in the tsBlock, return true; else
     * return false.
     */
    protected boolean isEmpty(int columnIndex) {
        return inputTsBlocks[columnIndex] == null
                || inputTsBlocks[columnIndex].getPositionCount() == inputIndex[columnIndex];
    }

    /**
     * Update scan states in QueryStateManager singleton after each next() call.
     *
     * <p>For each child operator: 1. If inputTsBlocks[i] is empty, set offset to current
     * scanTimestamp and isCouldEqual to false 2. If inputTsBlocks[i] is not empty, set offset to
     * timestamp at inputIndex[i] and isCouldEqual to true
     */
    private void updateScanStates() {
        QueryStateManager stateManager = getSession();
        if (stateManager == null) return;

        for (int i = 0; i < inputOperatorsCount; i++) {
            // Skip if no corresponding scan path
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

            if (inputTsBlocks[i] == null || inputTsBlocks[i].getPositionCount() == inputIndex[i]) {
                // Case 1: inputTsBlocks[i] is empty
                // Set offset to current scanTimestamp and isCouldEqual to false
                stateManager.updateScanOffset(scanPath, scanStates.getScanTimestamp());
                stateManager.updateScanCouldEqual(scanPath, false);
            } else {
                // Case 2: inputTsBlocks[i] is not empty
                // Set offset to timestamp at inputIndex[i] and isCouldEqual to true
                long currentTimestamp;
                if (inputIndex[i] < inputTsBlocks[i].getPositionCount()) {
                    // Current processing position
                    currentTimestamp = inputTsBlocks[i].getTimeByIndex(inputIndex[i]);
                } else if (inputIndex[i] > 0) {
                    // If already processed all data, use the last processed timestamp
                    currentTimestamp = inputTsBlocks[i].getTimeByIndex(inputIndex[i] - 1);
                } else {
                    // Fallback to the first timestamp if available
                    currentTimestamp =
                            inputTsBlocks[i].getPositionCount() > 0
                                    ? inputTsBlocks[i].getTimeByIndex(0)
                                    : scanStates.getScanTimestamp();
                }

                stateManager.updateScanOffset(scanPath, currentTimestamp);
                stateManager.updateScanCouldEqual(scanPath, true);
            }

            // Update inner join status
            stateManager.updateScanInnerJoin(scanPath, true);
        }
    }


    /**
     * Get the child scan paths for this operator.
     *
     * @return list of scan paths corresponding to child operators
     */
    public List<String> getChildScanPaths() {
        return new ArrayList<>(childScanPaths);
    }

    /**
     * Extract series path from child operator if it is ExchangeOperator. If sourceId is
     * found, use its sourceId -> seriesPath; otherwise create a default path.
     *
     * @param childOperator the child operator to extract series path from
     * @param childIndex the index of the child operator
     * @return the extracted or generated series path
     */
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

    private String getColQueryId() {
        String edgeQueryId = operatorContext.getInstanceContext().getId().getQueryId().getId();
        int dataNodeId = org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
        return edgeQueryId + "-" + dataNodeId;
    }

    private QueryStateManager getSession() {
        return ColQuerySessions.getByEdgeQueryId(getColQueryId());
    }
}
