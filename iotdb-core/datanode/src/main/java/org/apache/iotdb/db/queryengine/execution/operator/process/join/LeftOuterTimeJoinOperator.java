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
import org.apache.iotdb.db.queryengine.execution.colquery.ColQuerySessions;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState;
import org.apache.iotdb.db.queryengine.execution.colquery.QueryStateManager;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.execution.operator.process.ProcessOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.TimeComparator;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.read.common.block.column.TimeColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.Futures.successfulAsList;

public class LeftOuterTimeJoinOperator implements ProcessOperator {

    private static final long INSTANCE_SIZE =
            RamUsageEstimator.shallowSizeOfInstance(LeftOuterTimeJoinOperator.class);

    private final OperatorContext operatorContext;

    private final int outputColumnCount;

    private final TimeComparator comparator;

    private final TsBlockBuilder resultBuilder;

    private final Operator left;
    private final int leftColumnCount;

    private TsBlock leftTsBlock;

    // start index of leftTsBlock
    private int leftIndex;

    private final Operator right;

    private TsBlock rightTsBlock;

    // start index of rightTsBlock
    private int rightIndex;

    private boolean rightFinished = false;

    private final long maxReturnSize =
            TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

    public LeftOuterTimeJoinOperator(
            OperatorContext operatorContext,
            Operator leftChild,
            int leftColumnCount,
            Operator rightChild,
            List<TSDataType> dataTypes,
            TimeComparator comparator) {

        this.operatorContext = operatorContext;
        this.resultBuilder = new TsBlockBuilder(dataTypes);
        this.outputColumnCount = dataTypes.size();
        this.comparator = comparator;
        this.left = leftChild;
        this.leftColumnCount = leftColumnCount;
        this.right = rightChild;
        QueryStateManager stateManager = getSession();
        if(stateManager != null) {
            stateManager.setHasLeftOuterJoin(true);
        }
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked() {
        ListenableFuture<?> leftBlocked = left.isBlocked();
        ListenableFuture<?> rightBlocked = right.isBlocked();
        if (leftBlocked.isDone()) {
            return rightBlocked;
        } else if (rightBlocked.isDone()) {
            return leftBlocked;
        } else {
            return successfulAsList(leftBlocked, rightBlocked);
        }
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
            long currentEndTime =
                    rightFinished
                            ? leftTsBlock.getEndTime()
                            : comparator.getCurrentEndTime(leftTsBlock.getEndTime(), rightTsBlock.getEndTime());

            long time = leftTsBlock.getTimeByIndex(leftIndex);

            // all the rightTsBlock is less than leftTsBlock, just skip it
            if (!rightFinished && comparator.largerThan(time, rightTsBlock.getEndTime())) {
                // clean rightTsBlock
                rightTsBlock = null;
                rightIndex = 0;
            } else if (rightFinished
                    || comparator.lessThan(
                    leftTsBlock.getEndTime(), rightTsBlock.getTimeByIndex(rightIndex))) {
                // all the rightTsBlock is larger than leftTsBlock, fill null for right child
                appendAllLeftTableAndFillNullForRightTable();
            } else {
                // left and right are overlapped, do the left outer join row by row
                int leftRowSize = leftTsBlock.getPositionCount();
                TimeColumnBuilder timeColumnBuilder = resultBuilder.getTimeColumnBuilder();

                while (comparator.canContinueInclusive(time, currentEndTime)
                        && !resultBuilder.isFull()
                        && appendRightTableRow(time)) {
                    timeColumnBuilder.writeLong(time);
                    resultBuilder.declarePosition();
                    // deal with leftTsBlock
                    appendLeftTableRow();

                    if (leftIndex < leftRowSize) {
                        // update next row's time
                        time = leftTsBlock.getTimeByIndex(leftIndex);
                    } else { // all the leftTsBlock is consumed up
                        // clean leftTsBlock
                        leftTsBlock = null;
                        leftIndex = 0;
                        break;
                    }
                }
            }
        }
        TsBlock res = resultBuilder.build();
        resultBuilder.reset();

        // Update left outer join cache after processing
        updateLeftOuterJoinCache();
        if(leftTsBlock != null) {
            System.out.println("leftTsBlock: " + showTsBlock(leftTsBlock));
            System.out.println("leftIndex: " + leftIndex);
        }else {
            System.out.println("leftTsBlock: null");
        }

        if(rightTsBlock != null) {
            System.out.println("rightTsBlock: " + showTsBlock(rightTsBlock));
            System.out.println("rightIndex: " + rightIndex);
        }else {
            System.out.println("rightTsBlock: null");
        }
        return res;
    }

    private boolean prepareInput(long start, long maxRuntime) throws Exception {
        if ((leftTsBlock == null || leftTsBlock.getPositionCount() == leftIndex)
                && left.hasNextWithTimer()) {
            leftTsBlock = left.nextWithTimer();
            leftIndex = 0;
        }
        // still have time and right child still have remaining data
        if ((System.nanoTime() - start < maxRuntime)
                && (!rightFinished
                && (rightTsBlock == null || rightTsBlock.getPositionCount() == rightIndex))) {
            if (right.hasNextWithTimer()) {
                rightTsBlock = right.nextWithTimer();
                rightIndex = 0;
            } else {
                rightFinished = true;
            }
        }
        return tsBlockIsNotEmpty(leftTsBlock, leftIndex)
                && (rightFinished || tsBlockIsNotEmpty(rightTsBlock, rightIndex));
    }

    private boolean tsBlockIsNotEmpty(TsBlock tsBlock, int index) {
        return tsBlock != null && index < tsBlock.getPositionCount();
    }

    private String getColQueryId() {
        String edgeQueryId = operatorContext.getInstanceContext().getId().getQueryId().getId();
        int dataNodeId = org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
        return edgeQueryId + "-" + dataNodeId;
    }

    private QueryStateManager getSession() {
        return ColQuerySessions.getByEdgeQueryId(getColQueryId());
    }

    private void appendLeftTableRow() {
        for (int i = 0; i < leftColumnCount; i++) {
            Column leftColumn = leftTsBlock.getColumn(i);
            ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
            if (leftColumn.isNull(leftIndex)) {
                columnBuilder.appendNull();
            } else {
                columnBuilder.write(leftColumn, leftIndex);
            }
        }
        leftIndex++;
    }

    /**
     * deal with rightTsBlock
     *
     * @param time left table's current time
     * @return true if we can append this row into result, that means there exists time in
     *     rightTsBlock larger than or equals to current time false if we cannot decide whether there
     *     exist corresponding time in right table until rightTsBlock is consumed up
     */
    private boolean appendRightTableRow(long time) {
        int rowCount = rightTsBlock.getPositionCount();

        while (rightIndex < rowCount
                && comparator.lessThan(rightTsBlock.getTimeByIndex(rightIndex), time)) {
            rightIndex++;
        }

        if (rightIndex == rowCount) {
            // clean up rightTsBlock
            rightTsBlock = null;
            rightIndex = 0;
            return false;
        }

        if (rightTsBlock.getTimeByIndex(rightIndex) == time) {
            // right table has this time, append right table's corresponding row
            for (int i = leftColumnCount; i < outputColumnCount; i++) {
                Column rightColumn = rightTsBlock.getColumn(i - leftColumnCount);
                ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
                if (rightColumn.isNull(rightIndex)) {
                    columnBuilder.appendNull();
                } else {
                    columnBuilder.write(rightColumn, rightIndex);
                }
            }
            // update right Index
            rightIndex++;
        } else {
            // right table doesn't have this time, just append null for right table
            for (int i = leftColumnCount; i < outputColumnCount; i++) {
                resultBuilder.getColumnBuilder(i).appendNull();
            }
        }
        return true;
    }

    private void appendAllLeftTableAndFillNullForRightTable() {
        int rowSize = leftTsBlock.getPositionCount();
        // append time column
        TimeColumnBuilder timeColumnBuilder = resultBuilder.getTimeColumnBuilder();
        TimeColumn leftTimeColumn = leftTsBlock.getTimeColumn();
        for (int i = leftIndex; i < rowSize; i++) {
            timeColumnBuilder.writeLong(leftTimeColumn.getLong(i));
        }

        resultBuilder.declarePositions(rowSize - leftIndex);

        // append value column of left table
        appendValueColumnForLeftTable(rowSize);

        // append null for each column of right table
        appendNullForRightTable(rowSize);

        // clean leftTsBlock
        leftTsBlock = null;
        leftIndex = 0;
    }

    private void appendValueColumnForLeftTable(int rowSize) {
        for (int i = 0; i < leftColumnCount; i++) {
            ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
            Column valueColumn = leftTsBlock.getColumn(i);

            if (valueColumn.mayHaveNull()) {
                for (int rowIndex = leftIndex; rowIndex < rowSize; rowIndex++) {
                    if (valueColumn.isNull(rowIndex)) {
                        columnBuilder.appendNull();
                    } else {
                        columnBuilder.write(valueColumn, rowIndex);
                    }
                }
            } else {
                // no null in current column, no need to do isNull judgement for each row in for-loop
                for (int rowIndex = leftIndex; rowIndex < rowSize; rowIndex++) {
                    columnBuilder.write(valueColumn, rowIndex);
                }
            }
        }
    }

    private void appendNullForRightTable(int rowSize) {
        int nullCount = rowSize - leftIndex;
        for (int i = leftColumnCount; i < outputColumnCount; i++) {
            ColumnBuilder columnBuilder = resultBuilder.getColumnBuilder(i);
            columnBuilder.appendNull(nullCount);
        }
    }

    @Override
    public boolean hasNext() throws Exception {
        QueryStateManager queryStateManager = getSession();
        if(queryStateManager != null){
            if(queryStateManager.getStateMachine().getState()== ColQueryState.PRE_CLOSED
                    && !queryStateManager.getOperatorClearManager().isCleared("LeftOuterJoin")){
                //清空全部中间状态
                resultBuilder.reset();
                leftTsBlock = queryStateManager.getLeftOuterJoinCacheLeft();
                leftIndex = 0;
                rightTsBlock = queryStateManager.getLeftOuterJoinCacheRight();
                rightIndex = 0;
                rightFinished = false;

                queryStateManager.getOperatorClearManager().clearOperator(getColQueryId(),"LeftOuterJoin");
            }
        }
        return tsBlockIsNotEmpty(leftTsBlock, leftIndex) || left.hasNextWithTimer();
    }

    @Override
    public void close() throws Exception {
        if (left != null) {
            left.close();
        }
        if (right != null) {
            right.close();
        }
    }

    @Override
    public boolean isFinished() throws Exception {
        return !tsBlockIsNotEmpty(leftTsBlock, leftIndex) && left.isFinished();
    }

    @Override
    public long calculateMaxPeekMemory() {
        return Math.max(
                Math.max(
                        left.calculateMaxPeekMemoryWithCounter(), right.calculateMaxPeekMemoryWithCounter()),
                calculateRetainedSizeAfterCallingNext() + calculateMaxReturnSize());
    }

    @Override
    public long calculateMaxReturnSize() {
        return maxReturnSize;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
        // leftTsBlock + leftChild.RetainedSizeAfterCallingNext + rightTsBlock +
        // rightChild.RetainedSizeAfterCallingNext
        return left.calculateMaxReturnSize()
                + left.calculateRetainedSizeAfterCallingNext()
                + right.calculateMaxReturnSize()
                + right.calculateRetainedSizeAfterCallingNext();
    }

    @Override
    public long ramBytesUsed() {
        return INSTANCE_SIZE
                + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
                + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(left)
                + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(right)
                + resultBuilder.getRetainedSizeInBytes();
    }

    /**
     * Update left outer join cache in QueryStateManager after each next() call.
     *
     * <p>This method implements the specified caching logic:
     * 1. If leftTsBlock is empty, cache data from rightTsBlock where timestamps >= rightIndex timestamp
     * 2. If rightTsBlock is empty, cache data from leftTsBlock where timestamps >= leftIndex timestamp
     * 3. Set hasLeftOuterJoin = true when caching occurs
     */
    private void updateLeftOuterJoinCache() {
        QueryStateManager stateManager = getSession();
        if (stateManager == null) return;

        // Case 1: leftTsBlock is empty, cache data from rightTsBlock
        if ((leftTsBlock == null || leftIndex == leftTsBlock.getPositionCount())
                && rightTsBlock != null && rightIndex < rightTsBlock.getPositionCount()) {

            long thresholdTime = rightTsBlock.getTimeByIndex(rightIndex);
            TsBlock cacheBlock = extractDataFromThreshold(rightTsBlock, rightIndex, thresholdTime);

            if (cacheBlock != null && cacheBlock.getPositionCount() > 0) {
//                System.out.println("不应从这走"+showTsBlock(cacheBlock));
                stateManager.setLeftOuterJoinCacheRight(cacheBlock);
                stateManager.setLeftOuterJoinCacheLeft(null);
                stateManager.setHasLeftOuterJoin(true);
            }
        }
        // Case 2: rightTsBlock is empty or finished, cache data from leftTsBlock
        else if ((rightFinished || rightTsBlock == null || rightIndex == rightTsBlock.getPositionCount())
                && leftTsBlock != null && leftIndex < leftTsBlock.getPositionCount()) {

            long thresholdTime = leftTsBlock.getTimeByIndex(leftIndex);
            TsBlock cacheBlock = extractDataFromThreshold(leftTsBlock, leftIndex, thresholdTime);

            if (cacheBlock != null && cacheBlock.getPositionCount() > 0) {
//                System.out.println("从这里走"+showTsBlock(cacheBlock));
                stateManager.setLeftOuterJoinCacheLeft(cacheBlock);
                stateManager.setLeftOuterJoinCacheRight(null);
                stateManager.setHasLeftOuterJoin(true);
            }
        }
        else if (rightTsBlock!=null && rightIndex==0 && leftTsBlock!=null && leftIndex==0) {
            long thresholdTimeRight = rightTsBlock.getTimeByIndex(rightIndex);
            long thresholdTimeLeft = leftTsBlock.getTimeByIndex(leftIndex);
            if(thresholdTimeRight == thresholdTimeLeft){
                stateManager.setLeftOuterJoinCacheLeft(leftTsBlock);
                stateManager.setLeftOuterJoinCacheRight(rightTsBlock);
                stateManager.setHasLeftOuterJoin(true);
            }
        }
        else {
            stateManager.setLeftOuterJoinCacheLeft(null);
            stateManager.setLeftOuterJoinCacheRight(null);
        }
    }

    /**
     * Extract data from a TsBlock starting from the given index where timestamps are >= threshold.
     *
     * @param tsBlock the source TsBlock to extract data from
     * @param startIndex the starting index to begin extraction
     * @param thresholdTime the minimum timestamp threshold for extraction
     * @return a new TsBlock containing the extracted data, or null if no data meets criteria
     */
    private TsBlock extractDataFromThreshold(TsBlock tsBlock, int startIndex, long thresholdTime) {
        if (tsBlock == null || startIndex >= tsBlock.getPositionCount()) {
            return null;
        }

        // Count how many rows meet the threshold criteria
        int validRowCount = 0;
        for (int i = startIndex; i < tsBlock.getPositionCount(); i++) {
            if (tsBlock.getTimeByIndex(i) >= thresholdTime) {
                validRowCount++;
            }
        }

        if (validRowCount == 0) {
            return null;
        }

        // Create a new TsBlockBuilder with the same data types as the source
        List<TSDataType> dataTypes = new ArrayList<>();
        for (int i = 0; i < tsBlock.getValueColumnCount(); i++) {
            dataTypes.add(tsBlock.getColumn(i).getDataType());
        }
        TsBlockBuilder cacheBuilder = new TsBlockBuilder(dataTypes);

        TimeColumnBuilder timeColumnBuilder = cacheBuilder.getTimeColumnBuilder();

        // Copy rows that meet the threshold criteria
        for (int i = startIndex; i < tsBlock.getPositionCount(); i++) {
            long timestamp = tsBlock.getTimeByIndex(i);
            if (timestamp >= thresholdTime) {
                // Copy timestamp
                timeColumnBuilder.writeLong(timestamp);

                // Copy value columns
                for (int j = 0; j < tsBlock.getValueColumnCount(); j++) {
                    Column sourceColumn = tsBlock.getColumn(j);
                    ColumnBuilder targetColumnBuilder = cacheBuilder.getColumnBuilder(j);

                    if (sourceColumn.isNull(i)) {
                        targetColumnBuilder.appendNull();
                    } else {
                        targetColumnBuilder.write(sourceColumn, i);
                    }
                }

                cacheBuilder.declarePosition();
            }
        }

        return cacheBuilder.build();
    }
    private String showTsBlock(TsBlock tsBlock) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n！！！:\n");
        // We keep the whole dump under read lock to keep a consistent snapshot
//        lock.readLock().lock();
        try {
            sb.append("  输出块\n");
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
}
