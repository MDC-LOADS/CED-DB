package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.colquery.colservice.*;
import org.apache.iotdb.db.utils.colutils.SQLQueryExecutor;
import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ServiceImpl implements E2CColService.Iface{

    private String sql = null;

    @Override
    public void ColQueryStart(String sql, String queryId) throws TException {
        this.sql = sql;
        QueryStateManager stateManager = QueryStateManager.initialize();//初始化管理状态
        ExecutorService executor = Executors.newSingleThreadExecutor();
        ColQueryStateMachine colQueryStateMachine =
                new ColQueryStateMachine(queryId, executor);
        stateManager.setStateMachine(colQueryStateMachine);
        stateManager.setQueryId(new QueryId(queryId));
        colQueryStateMachine.transitionToStart();//状态机切换为START
        Thread queryExecution = new Thread(new ExecuteIdentityQuery());
        queryExecution.start();
    }

    @Override
    public void AnsMessageWithLeftOuterJoin(int edgeFragmentId, Map<String, ScanInfo> scanInfoMap, TimeColumn timeColumnLeft, List<Column> valueColumnsLeft, TimeColumn timeColumnRight, List<Column> valueColumnsRight) throws TException {
        //将数据转换后塞到全局变量中，状态变更pre_col_query
        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        queryStateManager.setEdgeFragmentId(edgeFragmentId);
        queryStateManager.createAndSetSinkHandle(edgeFragmentId);
        scanInfoMap.forEach((key, value) -> {
            queryStateManager.setSeriesPathAndPlanNodeId(key,value.getSeriesPath());
            QueryStateManager.ScanStates scanStates = ScanInfoConverter.convertToScanStates(value);
            scanStates.setScanTimestamp(value.offset);
            queryStateManager.setScanStates(value.getSeriesPath(),scanStates);
        });
        List<TSDataType> inferredTypesLeft = valueColumnsLeft.stream()
                .map(column -> {
                    ColumnData columnData = column.getData();
                    if (columnData == null) {
                        return TSDataType.UNKNOWN;
                    }
                    // 根据 ColumnData 的实际字段推断类型
                    if (columnData.isSetIntValues()) {
                        return TSDataType.INT32;
                    } else if (columnData.isSetLongValues()) {
                        return TSDataType.INT64;
                    } else if (columnData.isSetDoubleValues()) {
                        return TSDataType.DOUBLE;
                    } else if (columnData.isSetStringValues()) {
                        return TSDataType.TEXT;
                    } else if (columnData.isSetBoolValues()) {
                        return TSDataType.BOOLEAN;
                    }
                    return TSDataType.UNKNOWN;
                })
                .collect(Collectors.toList());
        List<TSDataType> inferredTypesRight = valueColumnsRight.stream()
                .map(column -> {
                    ColumnData columnData = column.getData();
                    if (columnData == null) {
                        return TSDataType.UNKNOWN;
                    }
                    // 根据 ColumnData 的实际字段推断类型
                    if (columnData.isSetIntValues()) {
                        return TSDataType.INT32;
                    } else if (columnData.isSetLongValues()) {
                        return TSDataType.INT64;
                    } else if (columnData.isSetDoubleValues()) {
                        return TSDataType.DOUBLE;
                    } else if (columnData.isSetStringValues()) {
                        return TSDataType.TEXT;
                    } else if (columnData.isSetBoolValues()) {
                        return TSDataType.BOOLEAN;
                    }
                    return TSDataType.UNKNOWN;
                })
                .collect(Collectors.toList());
        TsBlock cacheLeft = ScanInfoConverter.convertColumnsToTsBlock(timeColumnLeft,valueColumnsLeft,inferredTypesLeft);
        TsBlock cacheRight = ScanInfoConverter.convertColumnsToTsBlock(timeColumnRight,valueColumnsRight,inferredTypesRight);
        queryStateManager.setHasLeftOuterJoin(true);
        queryStateManager.setLeftOuterJoinCacheLeft(cacheLeft);
        queryStateManager.setLeftOuterJoinCacheRight(cacheRight);
        System.out.println(queryStateManager.getStateSummary());
        System.out.println("接收的cache为："+showTsBlock(cacheLeft)+showTsBlock(cacheRight));
        queryStateManager.getStateMachine().transitionToPreColQuery();
    }

    @Override
    public void AnsMessage(int edgeFragmentId, Map<String, ScanInfo> scanInfoMap) throws TException {
        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        queryStateManager.setEdgeFragmentId(edgeFragmentId);
        queryStateManager.createAndSetSinkHandle(edgeFragmentId);
        scanInfoMap.forEach((key, value) -> {
            queryStateManager.setSeriesPathAndPlanNodeId(key,value.getSeriesPath());
            QueryStateManager.ScanStates scanStates = ScanInfoConverter.convertToScanStates(value);
            scanStates.setScanTimestamp(value.offset);
            queryStateManager.setScanStates(value.getSeriesPath(),scanStates);
        });
        System.out.println("接收到的索引为："+queryStateManager.getStateSummary());
        queryStateManager.getStateMachine().transitionToPreColQuery();
//        notifyAll();
    }

    @Override
    public void AnsMessageWithSingleScan(int edgeFragmentId, String planNodeId, long offset, String seriesPath, boolean isCloudEqual) throws TException {
        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        queryStateManager.setEdgeFragmentId(edgeFragmentId);
        queryStateManager.createAndSetSinkHandle(edgeFragmentId);
        queryStateManager.setSeriesPathAndPlanNodeId(planNodeId,seriesPath);
        queryStateManager.setScanStates(seriesPath,new QueryStateManager.ScanStates(0,offset,isCloudEqual,false,false));
        queryStateManager.setSingleScan(true);
        queryStateManager.getStateMachine().transitionToPreColQuery();
    }

    @Override
    public void PreColQueryClose() throws TException {
        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        if(queryStateManager.getStateMachine().getState() == ColQueryState.COL_QUERY){
            queryStateManager.getStateMachine().transitionToPreClosed();
        }
    }
    class ExecuteIdentityQuery implements Runnable {
        @Override
        public void run() {
            // 首先检查IoTDB服务是否可用，带重试机制
            System.out.println("Checking IoTDB services availability (with retry)...");
            // 在DataNode启动过程中，ConfigNode可能需要一些时间才能完全准备好
            boolean servicesAvailable = SQLQueryExecutor.isIoTDBServicesAvailable(5, 20); // 5次重试，间隔2秒
            System.out.println("✓ IoTDB services are available! Running examples...");
            SQLQueryExecutor executor = new SQLQueryExecutor();
            if(sql!=null && servicesAvailable){
                try {
                    SQLQueryExecutor.QueryResult result = executor.executeQuery(sql);
                    System.out.println("✓ 查询成功!");
                    System.out.println("SQL: " + sql);
                    System.out.println("列名: " + result.getColumnNames());
                    System.out.println("数据类型: " + result.getDataTypes());
                    System.out.println("行数: " + result.getRowCount());

                    if (result.getRowCount() > 0) {
                        System.out.println("数据:");
                        for (int i = 0; i < result.getRowCount(); i++) {
                            System.out.println("  行 " + (i + 1) + ": " + result.getRows().get(i));
                        }
                    } else {
                        System.out.println("没有查询到数据");
                    }

                } catch (SQLQueryExecutor.QueryExecutionException e) {
                    System.err.println("✗ 查询执行失败: " + e.getMessage());
                }
            }
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
}
