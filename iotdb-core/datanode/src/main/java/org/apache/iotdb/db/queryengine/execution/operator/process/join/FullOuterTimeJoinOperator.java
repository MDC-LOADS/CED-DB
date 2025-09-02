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

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState;
import org.apache.iotdb.db.queryengine.execution.colquery.QueryStateManager;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.AbstractConsumeAllOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.ColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.source.AbstractDataSourceOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.ExchangeOperator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanUtil;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.successfulAsList;

public class FullOuterTimeJoinOperator extends AbstractConsumeAllOperator {

    private static final long INSTANCE_SIZE =
            RamUsageEstimator.shallowSizeOfInstance(FullOuterTimeJoinOperator.class)
                    + RamUsageEstimator.shallowSizeOfInstance(TimeSelector.class);

    /** Start index for each input TsBlocks and size of it is equal to inputTsBlocks. */
    private final int[] inputIndex;

    /** Used to record current index for input TsBlocks after merging. */
    private final int[] shadowInputIndex;

    /**
     * Represent whether there are more tsBlocks from ith child operator. If all elements in {@code
     * noMoreTsBlocks[]} are true and {@code inputTsBlocks[]} are consumed completely, this operator
     * is finished.
     */
    private final boolean[] noMoreTsBlocks;

    private final TimeSelector timeSelector;

    private final int outputColumnCount;

    /**
     * This field indicates each data type for output columns(not including time column) of
     * TimeJoinOperator its size should be equal to outputColumnCount.
     */
    private final List<TSDataType> dataTypes;

    private final List<ColumnMerger> mergers;

    private final TsBlockBuilder tsBlockBuilder;

    private boolean finished;

    private final TimeComparator comparator;

    /** Mapping from child operator index to scan path */
    private final List<String> childScanPaths;


    public FullOuterTimeJoinOperator(
            OperatorContext operatorContext,
            List<Operator> children,
            Ordering mergeOrder,
            List<TSDataType> dataTypes,
            List<ColumnMerger> mergers,
            TimeComparator comparator) {
        this(operatorContext, children, mergeOrder, dataTypes, mergers, comparator, new ArrayList<>());
    }

    public FullOuterTimeJoinOperator(
            OperatorContext operatorContext,
            List<Operator> children,
            Ordering mergeOrder,
            List<TSDataType> dataTypes,
            List<ColumnMerger> mergers,
            TimeComparator comparator,
            List<String> childScanPaths) {
        super(operatorContext, children);
        checkArgument(!children.isEmpty(), "child size of TimeJoinOperator should be larger than 0");
        this.inputIndex = new int[this.inputOperatorsCount];
        this.shadowInputIndex = new int[this.inputOperatorsCount];
        this.noMoreTsBlocks = new boolean[this.inputOperatorsCount];
        this.timeSelector = new TimeSelector(this.inputOperatorsCount << 1, Ordering.ASC == mergeOrder);
        this.outputColumnCount = dataTypes.size();
        this.dataTypes = dataTypes;
        this.tsBlockBuilder = new TsBlockBuilder(dataTypes);
        this.mergers = mergers;
        this.comparator = comparator;
        this.childScanPaths = childScanPaths != null ? childScanPaths : new ArrayList<>();
        this.maxReturnSize =
                Math.min(
                        maxReturnSize,
                        (1L + outputColumnCount)
                                * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());

        // Initialize default scan paths if not provided and QueryStateManager singleton is available
        if (this.childScanPaths.isEmpty() && QueryStateManager.isInitialized()) {
            for (int i = 0; i < inputOperatorsCount; i++) {
                String scanPath = extractSeriesPathFromChild(children.get(i), i);
                this.childScanPaths.add(scanPath);
            }
        }
    }

    @Override
    public ListenableFuture<?> isBlocked() {
        boolean hasReadyChild = false;
        List<ListenableFuture<?>> listenableFutures = new ArrayList<>();
        for (int i = 0; i < inputOperatorsCount; i++) {
            if (noMoreTsBlocks[i] || !isEmpty(i) || children.get(i) == null) {
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
        if (retainedTsBlock != null) {
            return getResultFromRetainedTsBlock();
        }
        tsBlockBuilder.reset();
        if (!prepareInput()) {
            return null;
        }

        // End time for returned TsBlock this time, it's the min/max end time among all the children
        // TsBlocks order by asc/desc
        long currentEndTime = 0;
        boolean init = false;

        // Get TsBlock for each input, put their time stamp into TimeSelector and then use the min Time
        // among all the input TsBlock as the current output TsBlock's endTime.
        for (int i = 0; i < inputOperatorsCount; i++) {
            if (!noMoreTsBlocks[i]) {
                // Update the currentEndTime if the TsBlock is not empty
                currentEndTime =
                        init
                                ? comparator.getCurrentEndTime(currentEndTime, inputTsBlocks[i].getEndTime())
                                : inputTsBlocks[i].getEndTime();
                init = true;
            }
        }

        if (timeSelector.isEmpty()) {
            // Return empty TsBlock
            TsBlockBuilder emptyTsBlockBuilder = new TsBlockBuilder(0, dataTypes);
            return emptyTsBlockBuilder.build();
        }

        TimeColumnBuilder timeBuilder = tsBlockBuilder.getTimeColumnBuilder();
        long currentTime;
        do {
            currentTime = timeSelector.pollFirst();
            timeBuilder.writeLong(currentTime);

            appendOneRow(currentTime);
            tsBlockBuilder.declarePosition();

            prepareForTimeHeap();

        } while (comparator.lessThan(currentTime, currentEndTime) && !timeSelector.isEmpty());

        resultTsBlock = tsBlockBuilder.build();


        // Update scan states after processing
        updateScanStates();

        return checkTsBlockSizeAndGetResult();
    }

    private void appendOneRow(long currentTime) {
        for (int i = 0; i < outputColumnCount; i++) {
            ColumnMerger merger = mergers.get(i);
            merger.mergeColumn(
                    inputTsBlocks,
                    inputIndex,
                    shadowInputIndex,
                    currentTime,
                    tsBlockBuilder.getColumnBuilder(i));
        }
    }

    private void prepareForTimeHeap() {
        for (int i = 0; i < inputOperatorsCount; i++) {
            if (inputIndex[i] != shadowInputIndex[i]) {
                inputIndex[i] = shadowInputIndex[i];
                if (!isEmpty(i)) {
                    updateTimeSelector(i);
                }
            }
        }
    }

    @Override
    public boolean hasNext() throws Exception {
        if(QueryStateManager.isInitialized()){
            QueryStateManager queryStateManager = QueryStateManager.getInstance();
            if(queryStateManager.getStateMachine().getState()== ColQueryState.PRE_CLOSED){
                //清空全部中间状态
                Arrays.fill(inputIndex, 0);
                Arrays.fill(shadowInputIndex, 0);
                Arrays.fill(noMoreTsBlocks, false);
                inputTsBlocks = new TsBlock[inputOperatorsCount];
                retainedTsBlock = null;
                for (int i = 0; i < inputOperatorsCount; i++) {
                    canCallNext[i] = false;
                }
                currentChildIndex = 0;
                hasEmptyChildInput = false;
                timeSelector.clear();
                queryStateManager.getOperatorClearManager().clearOperator("FullOuterJoin");
            }
        }
        if (finished) {
            return false;
        }
        if (retainedTsBlock != null) {
            return true;
        }
        for (int i = 0; i < inputOperatorsCount; i++) {
            if (!isEmpty(i)) {
                return true;
            } else if (!noMoreTsBlocks[i]) {
                if (!canCallNext[i] || children.get(i).hasNextWithTimer()) {
                    return true;
                } else {
                    noMoreTsBlocks[i] = true;
                    inputTsBlocks[i] = null;
                    children.get(i).close();
                    children.set(i, null);
                }
            }
        }
        return false;
    }

    @Override
    public boolean isFinished() throws Exception {
        if (finished) {
            return true;
        }
        if (retainedTsBlock != null) {
            return false;
        }

        finished = true;
        for (int i = 0; i < inputOperatorsCount; i++) {
            // has more tsBlock output from children[i] or has cached tsBlock in inputTsBlocks[i]
            if (!noMoreTsBlocks[i] || !isEmpty(i)) {
                finished = false;
                break;
            }
        }
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
        return maxReturnSize;
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

    private void updateTimeSelector(int index) {
        timeSelector.add(inputTsBlocks[index].getTimeByIndex(inputIndex[index]));
    }

    // region helper function used in prepareInput

    /**
     * @param currentChildIndex the index of the child
     * @return true if we can skip the currentChild in prepareInput
     */
    @Override
    protected boolean canSkipCurrentChild(int currentChildIndex) {
        return noMoreTsBlocks[currentChildIndex]
                || !isEmpty(currentChildIndex)
                || children.get(currentChildIndex) == null;
    }

    /**
     * @param currentInputIndex index of the input TsBlock
     */
    @Override
    protected void processCurrentInputTsBlock(int currentInputIndex) {
        updateTimeSelector(currentInputIndex);
    }

    /**
     * @param currentChildIndex the index of the child
     * @throws Exception Potential Exception thrown by Operator.close()
     */
    @Override
    protected void handleFinishedChild(int currentChildIndex) throws Exception {
        noMoreTsBlocks[currentChildIndex] = true;
        inputTsBlocks[currentChildIndex] = null;
        children.get(currentChildIndex).close();
        children.set(currentChildIndex, null);
    }

    // endregion

    @TestOnly
    public List<Operator> getChildren() {
        return children;
    }

    @Override
    protected TsBlock getNextTsBlock(int childIndex) throws Exception {
        inputIndex[childIndex] = 0;
        return children.get(childIndex).nextWithTimer();
    }

    /**
     * If the tsBlock of columnIndex is null or has no more data in the tsBlock, return true; else
     * return false.
     */
    @Override
    protected boolean isEmpty(int columnIndex) {
        return inputTsBlocks[columnIndex] == null
                || inputTsBlocks[columnIndex].getPositionCount() == inputIndex[columnIndex];
    }

    @Override
    public long ramBytesUsed() {
        return INSTANCE_SIZE
                + children.stream()
                .mapToLong(MemoryEstimationHelper::getEstimatedSizeOfAccountableObject)
                .sum()
                + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
                + RamUsageEstimator.sizeOf(canCallNext)
                + RamUsageEstimator.sizeOf(noMoreTsBlocks)
                + RamUsageEstimator.sizeOf(inputIndex)
                + RamUsageEstimator.sizeOf(shadowInputIndex)
                + tsBlockBuilder.getRetainedSizeInBytes();
    }

    /**
     * Update scan states in QueryStateManager singleton after each next() call.
     *
     * <p>具体功能如下：
     * 1. 当子算子对应的 inputTsBlocks[] 为空时，QueryStateManager 中的 ScanStates 中的 scanOffset 设置为它的 ScanTimestamp，isCouldEqual 设置为 false；
     * 2. 子算子对应 inputTsBlock[] 为空，retainedTsBlock 不为空，scanOffset 设置为 retainedTsBlock 中的最小时间戳；
     * 3. 子算子对应 inputTsBlocks[] 不为空，retainedTsBlock 为空，scanOffset 设置为 inputTsBLocks[inputIndex[]]；
     * 4. 子算子对应 inputTsBlocks[] 不为空，retainedTsBlock 也不为空，offset 设置为 retainedTsBlock 中的最小时间戳；
     */
    private void updateScanStates() {
        // Check if QueryStateManager singleton is initialized
        if (!QueryStateManager.isInitialized()) {
            return;
        }

        QueryStateManager stateManager = QueryStateManager.getInstance();

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
                //Case 2:当子算子对应的 inputTsBlocks[] 为空,retainedTsBlock不为空时
                if(retainedTsBlock !=null && retainedTsBlock.getPositionCount() > 0){
                    long offsetTime;
                    offsetTime = retainedTsBlock.getTimeByIndex(0);
                    stateManager.updateScanOffset(scanPath, offsetTime);
                    stateManager.updateScanCouldEqual(scanPath, true);
                }else {
                    // Case 1: 当子算子对应的 inputTsBlocks[] 为空,retainedTsBlock为空时
                    // scanOffset 设置为它的 ScanTimestamp，isCouldEqual 设置为 false
                    stateManager.updateScanOffset(scanPath, scanStates.getScanTimestamp());
                    stateManager.updateScanCouldEqual(scanPath, false);
                }
            } else {
                long offsetTime;
                // inputTsBlocks[] 不为空的情况
                // Case 4: retainedTsBlock 也不为空，offset 设置为 retainedTsBlock 中的最小时间戳
                if (retainedTsBlock != null && retainedTsBlock.getPositionCount() > 0) {
                    offsetTime = retainedTsBlock.getTimeByIndex(0); // 最小时间戳（第一个）
                }
                // Case 3: retainedTsBlock 为空，scanOffset 设置为 returnedMaxTime
                else {
                    offsetTime = inputTsBlocks[i].getTimeByIndex(inputIndex[i]);
                }

                stateManager.updateScanOffset(scanPath, offsetTime);
                stateManager.updateScanCouldEqual(scanPath, true);
            }

            // Update full outer join status
            stateManager.updateScanFullOuterJoin(scanPath, true);
        }
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
                    QueryStateManager stateManager = QueryStateManager.getInstance();
                    if(stateManager.getSeriesPath(planNodeId) != null) {
                        return stateManager.getSeriesPath(planNodeId);
                    }
                }
            } catch (Exception e) {
                // Log the exception and handle accordingly
            }
        }
        // Fall back to default path naming if SeriesScanUtil is not found or extraction fails
        return "child_" + childIndex + "_" + operatorContext.getPlanNodeId();
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
     * Check if QueryStateManager singleton is available for state tracking.
     *
     * @return true if QueryStateManager singleton is initialized, false otherwise
     */
    public boolean isStateTrackingAvailable() {
        return QueryStateManager.isInitialized();
    }

    /**
     * Get the QueryStateManager singleton instance if available.
     *
     * @return the QueryStateManager singleton instance, or null if not initialized
     */
    public QueryStateManager getQueryStateManager() {
        return QueryStateManager.isInitialized() ? QueryStateManager.getInstance() : null;
    }
}
