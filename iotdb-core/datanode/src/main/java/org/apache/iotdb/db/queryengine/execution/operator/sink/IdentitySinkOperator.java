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
import org.apache.iotdb.db.queryengine.execution.colquery.ResourceMonitor;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.utils.RamUsageEstimator;

import java.util.List;

public class IdentitySinkOperator implements Operator {

    private static final long INSTANCE_SIZE =
            RamUsageEstimator.shallowSizeOfInstance(IdentitySinkOperator.class)
                    + RamUsageEstimator.shallowSizeOfInstance(DownStreamChannelIndex.class);

    private final OperatorContext operatorContext;
    private final List<Operator> children;

    private final DownStreamChannelIndex downStreamChannelIndex;

    private final ISinkHandle sinkHandle;

    private boolean needToReturnNull = false;

    private boolean isFinished = false;

    ListenableFuture<?> blocked;

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
            QueryStateManager queryStateManager = QueryStateManager.getInstance();
            if(queryStateManager.getRootIdentitySinkId()!=null
                    && queryStateManager.getStateMachine().getState()== ColQueryState.PRE_COL_QUERY
                    && queryStateManager.getRootIdentitySinkId().equals(operatorContext.getPlanNodeId().getId())){
                queryStateManager.setCanSendOffset(true);
                while(queryStateManager.getStateMachine().getState() != ColQueryState.COL_QUERY){
                    try {
                        Thread.sleep(10);
                        System.out.println("等待COL_QUERY中："+queryStateManager.getStateMachine().getState());
                    }catch (InterruptedException e){
                        e.printStackTrace();
                    }
                }
            }
        }
        if(QueryStateManager.isInitialized()){
            QueryStateManager queryStateManager = QueryStateManager.getInstance();
            if (queryStateManager.getStateMachine().getState() == ColQueryState.COL_QUERY
                    && queryStateManager.getRootIdentitySinkId().equals(operatorContext.getPlanNodeId().getId())) {
                ISourceHandle sourceHandle = queryStateManager.getSourceHandle();
                if(!sourceHandle.isFinished()){
                    return true;//如果已经打开通道开始传输数据了，返回还有数据
                }else {
                    while(queryStateManager.getStateMachine().getState() != ColQueryState.PRE_CLOSED){
                        try{
                            Thread.sleep(10);
                        }catch (InterruptedException e){
                            e.printStackTrace();
                        }
                    }
                }
                //TODO:进入重启阶段

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
        } else {
            // current child has no more data
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
            if(queryStateManager.getRootIdentitySinkId()!=null && queryStateManager.getRootIdentitySinkId().equals(operatorContext.getPlanNodeId().getId())){
                System.out.println(queryStateManager.getStateSummary());
                if (queryStateManager.getStateMachine().getState() == ColQueryState.COL_QUERY) {
                    ISourceHandle colSourceHandle=queryStateManager.getSourceHandle();
                    TsBlock tsBlock_rev = null;
                    if(colSourceHandle!=null){
                        blocked = colSourceHandle.isBlocked();
                        if (!blocked.isDone()) {
                            blocked.get(); // 或加超时 blocked.get(5, TimeUnit.SECONDS)
                        }
                        if(!colSourceHandle.isFinished()){
                            tsBlock_rev = colSourceHandle.receive();
//                            System.out.println("接收到的TsBlock："+showTsBlock(tsBlock_rev));
                        }
                    }
                    return tsBlock_rev;
                }
            }
        }


        if (needToReturnNull) {
            needToReturnNull = false;
            return null;
        }
        System.out.println("\nSink "+this.operatorContext.getPlanNodeId()+"children are: ");
        for (int i = 0; i < children.size(); i++) {
            System.out.println("\n"+i+": "+children.get(i).toString());
        }
        TsBlock res = children.get(downStreamChannelIndex.getCurrentIndex()).nextWithTimer();
//        if(QueryStateManager.isInitialized()){
//            QueryStateManager queryStateManager = QueryStateManager.getInstance();
//            if(queryStateManager.getRootIdentitySinkId()!=null
//                    && queryStateManager.getStateMachine().getState()== ColQueryState.PRE_COL_QUERY
//                    && queryStateManager.getRootIdentitySinkId().equals(operatorContext.getPlanNodeId().getId())){
//                queryStateManager.getStateMachine().transitionToColQuery();
//                System.out.println("\n转变为协同查询");
////                notifyAll();
//            }
//        }
        if(QueryStateManager.isInitialized()){
            QueryStateManager queryStateManager = QueryStateManager.getInstance();
            if(queryStateManager.getRootIdentitySinkId()!=null && queryStateManager.getRootIdentitySinkId().equals(operatorContext.getPlanNodeId().getId())) {
//                System.out.println("\n- - - - - - - - - -\nTsBlock comes");
                System.out.println(showTsBlock(res));
            }
        }
        return res;
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

    private String showTsBlock(TsBlock tsBlock) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n！！！当前Identity的TsBlock为:\n");
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
}
