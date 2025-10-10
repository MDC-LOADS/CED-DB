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

import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState;
import org.apache.iotdb.db.queryengine.execution.colquery.QueryStateManager;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQuerySessions;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

public class ExchangeOperator implements SourceOperator {

    private static final long INSTANCE_SIZE =
            RamUsageEstimator.shallowSizeOfInstance(ExchangeOperator.class);

    private final OperatorContext operatorContext;

    private final ISourceHandle sourceHandle;

    private final PlanNodeId sourceId;

    private ListenableFuture<?> isBlocked = NOT_BLOCKED;

    private long maxReturnSize =
            TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();

    private SettableFuture<Void> blockedDependencyDriver = null;

    public ExchangeOperator(
            OperatorContext operatorContext, ISourceHandle sourceHandle, PlanNodeId sourceId) {
        this.operatorContext = operatorContext;
        this.sourceHandle = sourceHandle;
        this.sourceId = sourceId;
    }

    /**
     * For ExchangeOperator in pipeline, the maxReturnSize is equal to the maxReturnSize of the child
     * operator.
     *
     * @param maxReturnSize max return size of child operator
     */
    public ExchangeOperator(
            OperatorContext operatorContext,
            ISourceHandle sourceHandle,
            PlanNodeId sourceId,
            long maxReturnSize) {
        this.operatorContext = operatorContext;
        this.sourceHandle = sourceHandle;
        this.sourceId = sourceId;
        this.maxReturnSize = maxReturnSize;
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    @Override
    public TsBlock next() throws Exception {
        QueryStateManager queryStateManager = getSession();
        if(queryStateManager != null){
            if(queryStateManager.isHasSeriesPath(sourceId.getId())
                    && !queryStateManager.isSingleScan()
                    && queryStateManager.isScanPathExchangeByPlanNodeId(sourceId.getId())
                    && queryStateManager.getStateMachine().getState() ==ColQueryState.PRE_CLOSED){
                while(!queryStateManager.getOperatorClearManager().isCleared(sourceId.getId())){
                    try {
                        Thread.sleep(10);
                    }catch(InterruptedException e){
                        Thread.currentThread().interrupt();
                    }
                }
                if(sourceHandle instanceof LocalSourceHandle) {
                    ((LocalSourceHandle) sourceHandle).getSharedTsBlockQueue().waitUntilNotEmpty();
                }
            }
        }
//        if(sourceHandle instanceof LocalSourceHandle) {
//            ((LocalSourceHandle) sourceHandle).getSharedTsBlockQueue().waitUntilNotEmpty();
//        }
        TsBlock res = sourceHandle.receive();
        if(res!=null){
            QueryStateManager queryStateManager2 = getSession();
            if (queryStateManager2 != null) {
                if(queryStateManager2.isHasSeriesPath(sourceId.getId())
                    && !queryStateManager2.isSingleScan()
                    && queryStateManager2.isScanPathExchangeByPlanNodeId(sourceId.getId())) {
                    long currentEndTime = res.getEndTime();
                    queryStateManager2.updateScanTimestampByPlanNodeId(sourceId.getId(),currentEndTime);
//                    System.out.println("设置时间戳"+currentEndTime+"此时的plan id为："+sourceId.getId());
                    if(!queryStateManager2.hasScanSourceHandle(sourceId.getId())) {
                        queryStateManager2.addScanSourceHandle(sourceId.getId(),sourceHandle);
                    }
                    System.out.println(showTsBlock(res));
                }
            }
        }
        return res;
    }

    @Override
    public boolean hasNext() throws Exception {
        return !sourceHandle.isFinished();
    }

    @Override
    public boolean isFinished() throws Exception {
        return sourceHandle.isFinished();
    }

    @Override
    public long calculateMaxPeekMemory() {
        return maxReturnSize;
    }

    @Override
    public long calculateMaxReturnSize() {
        return maxReturnSize;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
        return 0L;
    }

    @Override
    public PlanNodeId getSourceId() {
        return sourceId;
    }

    public ISourceHandle getSourceHandle() {
        return sourceHandle;
    }

    private String getColQueryId() {
        String edgeQueryId = operatorContext.getInstanceContext().getId().getQueryId().getId();
        int dataNodeId = org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
        return edgeQueryId + "-" + dataNodeId;
    }

    private QueryStateManager getSession() {
        return ColQuerySessions.getByEdgeQueryId(getColQueryId());
    }

    @Override
    public ListenableFuture<?> isBlocked() {
        // Avoid registering a new callback in the source handle when one is already pending
        if (isBlocked.isDone()) {
            isBlocked = sourceHandle.isBlocked();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public void close() throws Exception {
        sourceHandle.close();
        if (blockedDependencyDriver != null) {
            blockedDependencyDriver.set(null);
        }
    }

    public SettableFuture<Void> getBlockedDependencyDriver() {
        if (blockedDependencyDriver == null) {
            blockedDependencyDriver = SettableFuture.create();
        }
        return blockedDependencyDriver;
    }

    @Override
    public long ramBytesUsed() {
        return INSTANCE_SIZE
                + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceId)
                + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(operatorContext)
                + MemoryEstimationHelper.getEstimatedSizeOfAccountableObject(sourceHandle);
    }

    private String showTsBlock(TsBlock tsBlock) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n！！！当前Exchange的TsBlock为:\n");
        // We keep the whole dump under read lock to keep a consistent snapshot
//        lock.readLock().lock();
        sb.append("plan node id :").append(sourceId.getId()).append("\n");
        try {
            sb.append("TsBlock: present\n");
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
