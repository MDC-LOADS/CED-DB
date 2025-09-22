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
import org.apache.iotdb.db.queryengine.execution.QueryState;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQueryState;
import org.apache.iotdb.db.queryengine.execution.colquery.QueryStateManager;
import org.apache.iotdb.db.queryengine.execution.colquery.ColQuerySessions;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.TimeFilterApi;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class AbstractSeriesScanOperator extends AbstractDataSourceOperator {

    private boolean finished = false;

    @Override
    public TsBlock next() throws Exception {

        try{
            Thread.sleep(10);
            System.out.println("stop scan 2s");
        }catch (InterruptedException e){
            e.printStackTrace();
        }

        if (retainedTsBlock != null) {
            TsBlock res = getResultFromRetainedTsBlock();
            setScanTimestamp(res);
            System.out.println("最终scan返回的TsBlock-retained"+showTsBlock(res));
            return res;
        }
        // we don't get any data in current batch time slice, just return null
        if (resultTsBlockBuilder.isEmpty()) {
            return null;
        }
        resultTsBlock = resultTsBlockBuilder.build();
        resultTsBlockBuilder.reset();
//        System.out.println(showTsBlock(resultTsBlock));
        TsBlock res = checkTsBlockSizeAndGetResult();
        setScanTimestamp(res);
        System.out.println("最终scan返回的TsBlock"+showTsBlock(res));
        return res;
    }

    private void setScanTimestamp(TsBlock res) {
        QueryStateManager queryStateManager = getSession();
        if(queryStateManager != null){
            System.out.println("待设置偏移量 scan，id为"+sourceId.getId());
            if(queryStateManager.isHasSeriesPath(sourceId.getId())
                    && !queryStateManager.isScanPathExchangeByPlanNodeId(sourceId.getId())) {
                long currentEndTime = res.getEndTime();
                queryStateManager.updateScanTimestampByPlanNodeId(sourceId.getId(),currentEndTime);
                System.out.println("设置了偏移量 scan，id为"+sourceId.getId());
            }
        }
    }

    @Override
    public boolean isFinished() throws Exception {
        return finished;
    }

    @SuppressWarnings("squid:S112")
    @Override
    public boolean hasNext() throws Exception {
        QueryStateManager queryStateManager = getSession();
        if(queryStateManager != null){
            if (queryStateManager.getStateMachine().getState()==ColQueryState.COL_QUERY){
                try {
                    queryStateManager.getStateMachine().getStateChange(ColQueryState.COL_QUERY).get();
                }catch (InterruptedException ie){
                    Thread.currentThread().interrupt();
                }catch (java.util.concurrent.ExecutionException ee){
                }
            }
            if(queryStateManager.getStateMachine().getState()== ColQueryState.PRE_CLOSED){
                System.out.println("准备进入清空");
                if(!queryStateManager.getOperatorClearManager().isCleared(sourceId.getId())){
                    System.out.println("完成清空");
                    retainedTsBlock = null;
                    startOffset = 0;
                    //清空管道
                ISourceHandle sourceHandle = queryStateManager.getScanSourceHandle(sourceId.getId());
                if(sourceHandle instanceof LocalSourceHandle  && !queryStateManager.isSingleScan()) {
                    System.out.println("清除了Source管道，plan node id为："+sourceId.getId());
                    ((LocalSourceHandle) sourceHandle).getSharedTsBlockQueue().fastClear();
                }

                    //创建新的series scan util 用于恢复查询
                    PartialPath seriesPath = this.seriesScanUtil.seriesPath;
                    Ordering scanOrder=this.seriesScanUtil.scanOrder;
                    SeriesScanOptions oldScanOptions = this.seriesScanUtil.scanOptions;
                    QueryDataSource dataSource =this.seriesScanUtil.dataSource;
                    Filter newOffsetFilter;
                    QueryStateManager.ScanStates scanStates = queryStateManager.getScanStates(seriesPath.getFullPath());
                    System.out.println("设置新查询的filter的offet为："+scanStates.getOffset());
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
                    if(oldScanOptions.pushDownLimit!=0){
                        System.out.println("pushDownLimit不为0:"+oldScanOptions.pushDownLimit);
                    }
                    builder.withAllSensors(oldScanOptions.getAllSensors());
                    newScanOptions = builder.build();
                    FragmentInstanceContext context = this.seriesScanUtil.context;
                    this.seriesScanUtil = new SeriesScanUtil(seriesPath, scanOrder, newScanOptions, context,dataSource);
                    queryStateManager.getOperatorClearManager().clearOperator(getColQueryId(),operatorContext.getPlanNodeId().getId());
                }
            }
        }
        if (retainedTsBlock != null) {
            return true;
        }
        try {

            // start stopwatch
            long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
            long start = System.nanoTime();

            boolean noMoreData = false;

            // here use do-while to promise doing this at least once
            do {
                /*
                 * 1. consume page data firstly
                 * 2. consume chunk data secondly
                 * 3. consume next file finally
                 */
                if (!readPageData() && !readChunkData() && !readFileData()) {
                    noMoreData = true;
                    break;
                }

            } while (System.nanoTime() - start < maxRuntime
                    && !resultTsBlockBuilder.isFull()
                    && retainedTsBlock == null);

            finished = (resultTsBlockBuilder.isEmpty() && retainedTsBlock == null && noMoreData);

            return !finished;
        } catch (IOException e) {
            throw new RuntimeException("Error happened while scanning the file", e);
        }
    }

    private String getColQueryId() {
        String edgeQueryId = operatorContext.getInstanceContext().getId().getQueryId().getId();
        int dataNodeId = org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
        return edgeQueryId + "-" + dataNodeId;
    }

    private QueryStateManager getSession() {
        return ColQuerySessions.getByEdgeQueryId(getColQueryId());
    }

    private boolean readFileData() throws IOException {
        while (seriesScanUtil.hasNextFile()) {
            if (readChunkData()) {
                return true;
            }
        }
        return false;
    }

    private boolean readChunkData() throws IOException {
        while (seriesScanUtil.hasNextChunk()) {
            if (readPageData()) {
                return true;
            }
        }
        return false;
    }

    private boolean readPageData() throws IOException {
        if (seriesScanUtil.hasNextPage()) {
            TsBlock tsBlock = seriesScanUtil.nextPage();
            if (!isEmpty(tsBlock)) {
                appendToBuilder(tsBlock);
            }
            return true;
        }
        return false;
    }

    private boolean isEmpty(TsBlock tsBlock) {
        return tsBlock == null || tsBlock.isEmpty();
    }

    private void appendToBuilder(TsBlock tsBlock) {
        int size = tsBlock.getPositionCount();
        if (resultTsBlockBuilder.isEmpty() && size >= resultTsBlockBuilder.getMaxTsBlockLineNumber()) {
            retainedTsBlock = tsBlock;
            return;
        }
        buildResult(tsBlock);
    }

    protected abstract void buildResult(TsBlock tsBlock);

    @Override
    protected List<TSDataType> getResultDataTypes() {
        return seriesScanUtil.getTsDataTypeList();
    }

    @Override
    public long calculateMaxReturnSize() {
        return maxReturnSize;
    }

    @Override
    public long calculateRetainedSizeAfterCallingNext() {
        return calculateMaxPeekMemoryWithCounter() - calculateMaxReturnSize();
    }

    private String showTsBlock(TsBlock tsBlock) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n！！！当前Scan的TsBlock为:\n");
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
