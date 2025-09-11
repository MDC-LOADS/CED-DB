package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.iotdb.db.queryengine.execution.colquery.colservice.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ServiceImpl implements C2EColService.Iface{


    @Override
    public void ACKMessage(String colQueryId, int cloudFragmentId) throws TException {
        QueryStateManager queryStateManager = ColQuerySessions.getByEdgeQueryId(colQueryId);
        if (queryStateManager == null) {
            return;
        }
        // 切换至同步索引
        queryStateManager.getStateMachine().transitionToPreColQuery();

        // 快照获取当前各扫描状态
        queryStateManager.getLock().readLock().lock();
        List<QueryStateManager.ScanStates> scanStates;
        List<String> seriesPaths;
        List<String> planNodeIds;
        try {
            scanStates = queryStateManager.getAllScanStatesList();
            seriesPaths = queryStateManager.getAllScanPathList();
            planNodeIds = queryStateManager.getAllScanPlanNodeIdList();
        } finally {
            queryStateManager.getLock().readLock().unlock();
        }

        // 建立边→云数据通道
        queryStateManager.setCloudFragmentId(cloudFragmentId);
        int edgeFragmentId = queryStateManager.getAndAddEdgeFragmentId();
        queryStateManager.createAndSetSourceHandle();

        // 等待根 IdentitySink 上报 offset 允许发送
        try {
            queryStateManager.getCanSendOffsetFuture().get();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        } catch (java.util.concurrent.ExecutionException e) {
            // ignore; let flow continue or add logging if needed
        }
        queryStateManager.setCanSendOffset(false);

        // 回传索引/缓存到云
        if (queryStateManager.isSingleScan()) {
            long offset = scanStates.get(0).getScanTimestamp();
            String seriesPath = queryStateManager.getSeriesPath(planNodeIds.get(0));
            callAnsMessageWithSingleScan(colQueryId, edgeFragmentId, planNodeIds.get(0), offset, seriesPath, false);
            queryStateManager.getStateMachine().transitionToColQuery();
        } else {
            Map<String, ScanInfo> scanInfoMap = new HashMap<>();
            for (int i = 0; i < scanStates.size(); i++) {
                ScanInfo scanInfo = ScanInfoConverter.convertToScanInfo(scanStates.get(i), seriesPaths.get(i));
                scanInfoMap.put(planNodeIds.get(i), scanInfo);
            }
            if (queryStateManager.hasLeftOuterJoin()) {
                TsBlock cacheLeft = queryStateManager.getLeftOuterJoinCacheLeft();
                TsBlock cacheRight = queryStateManager.getLeftOuterJoinCacheRight();
                ScanInfoConverter.TsBlockColumns valueColumnsLeft = ScanInfoConverter.convertTsBlockToColumns(cacheLeft);
                ScanInfoConverter.TsBlockColumns valueColumnsRight = ScanInfoConverter.convertTsBlockToColumns(cacheRight);
                if (valueColumnsRight == null && valueColumnsLeft != null) {
                    callAnsMessageWithLeftOuterJoin(
                            colQueryId, edgeFragmentId, scanInfoMap,
                            valueColumnsLeft.getTimeColumn(), valueColumnsLeft.getValueColumns(),
                            new TimeColumn(), new ArrayList<>());
                } else if (valueColumnsLeft == null && valueColumnsRight != null) {
                    callAnsMessageWithLeftOuterJoin(
                            colQueryId, edgeFragmentId, scanInfoMap,
                            new TimeColumn(), new ArrayList<>(),
                            valueColumnsRight.getTimeColumn(), valueColumnsRight.getValueColumns());
                } else if (valueColumnsLeft == null) {
                    callAnsMessageWithLeftOuterJoin(
                            colQueryId, edgeFragmentId, scanInfoMap,
                            new TimeColumn(), new ArrayList<>(),
                            new TimeColumn(), new ArrayList<>());
                } else {
                    callAnsMessageWithLeftOuterJoin(
                            colQueryId, edgeFragmentId, scanInfoMap,
                            valueColumnsLeft.getTimeColumn(), valueColumnsLeft.getValueColumns(),
                            valueColumnsRight.getTimeColumn(), valueColumnsRight.getValueColumns());
                }
                queryStateManager.getStateMachine().transitionToColQuery();
            } else {
                callAnsMessage(colQueryId, edgeFragmentId, scanInfoMap);
                queryStateManager.getStateMachine().transitionToColQuery();
            }
        }

    }


    @Override
    public void ColQueryCloseWithLeftOuterJoin(String colQueryId, Map<String, ScanInfo> scanInfoMap, TimeColumn timeColumnLeft, List<Column> valueColumnsLeft, TimeColumn timeColumnRight, List<Column> valueColumnsRight) throws TException {
        QueryStateManager queryStateManager = ColQuerySessions.getByEdgeQueryId(colQueryId);
        if (queryStateManager == null) return;
        while(!queryStateManager.getSourceHandle().isFinished()){
            try{
                Thread.sleep(10);
                System.out.println("等待关闭SInk channel");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //更新全部算子状态
        scanInfoMap.forEach((key, value) -> {
            queryStateManager.setSeriesPathAndPlanNodeId(key,value.getSeriesPath());
            QueryStateManager.ScanStates scanStates = ScanInfoConverter.convertToScanStates(value);
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
        //TODO:清除全部中间状态，恢复查询
        List<String> planNodeIds = queryStateManager.getAllScanPlanNodeIdList();
        boolean hasFullOuterJoin =false;
        boolean hasInnerJoin =false;
        for(Map.Entry<String, ScanInfo> entry : scanInfoMap.entrySet()){
            ScanInfo value = entry.getValue();
            if(value.isFullOuterJoin){
                hasFullOuterJoin = true;
            }
            if (value.isInnerJoin) {
                hasInnerJoin = true;
            }
        }
        if(hasFullOuterJoin){
            planNodeIds.add("FullOuterJoin");
        }
        if (hasInnerJoin){
            planNodeIds.add("InnerJoin");
        }
        planNodeIds.add("LeftOuterJoin");
        queryStateManager.setOperatorClearManager(planNodeIds);
        queryStateManager.getStateMachine().transitionToPreClosed();
    }

    @Override
    public void ColQueryClose(String colQueryId, Map<String, ScanInfo> scanInfoMap) throws TException {
        QueryStateManager queryStateManager = ColQuerySessions.getByEdgeQueryId(colQueryId);
        if (queryStateManager == null) return;
        while(!queryStateManager.getSourceHandle().isFinished()){
            try{
                Thread.sleep(10);
                System.out.println("等待关闭SInk channel");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //更新全部算子状态
//        QueryStateManager.getLock().readLock().lock();
        scanInfoMap.forEach((key, value) -> {
            queryStateManager.setSeriesPathAndPlanNodeId(key,value.getSeriesPath());
            QueryStateManager.ScanStates scanStates = ScanInfoConverter.convertToScanStates(value);
            scanStates.setScanTimestamp(value.offset);
            queryStateManager.setScanStates(value.getSeriesPath(),scanStates);
        });
//        QueryStateManager.getLock().readLock().unlock();
        //test
        System.out.println("\nColQueryClose success:");
        for (String planNodeId : queryStateManager.getAllScanPlanNodeIdList()) {
            System.out.println("planNodeId:"+planNodeId);
        }
        for (String seriesPath: queryStateManager.getAllScanPathList()) {
            System.out.println("seriesPath:"+seriesPath);
            QueryStateManager.ScanStates scanStates =queryStateManager.getScanStates(seriesPath);
            System.out.println("scanStates:"+scanStates);
        }
        System.out.println("设置新的offset以及数据信息：");
        System.out.println("快速查看"+queryStateManager.getStateSummary());
        //test end

        //TODO:清除全部中间状态，恢复查询
        List<String> planNodeIds = queryStateManager.getAllScanPlanNodeIdList();
        boolean hasFullOuterJoin =false;
        boolean hasInnerJoin =false;
        for(Map.Entry<String, ScanInfo> entry : scanInfoMap.entrySet()){
            ScanInfo value = entry.getValue();
            if(value.isFullOuterJoin){
                hasFullOuterJoin = true;
            }
            if (value.isInnerJoin) {
                hasInnerJoin = true;
            }
        }
        if(hasFullOuterJoin){
            planNodeIds.add("FullOuterJoin");
        }
        if (hasInnerJoin){
            planNodeIds.add("InnerJoin");
        }
        queryStateManager.setOperatorClearManager(planNodeIds);
        queryStateManager.getStateMachine().transitionToPreClosed();

    }

    @Override
    public void ColQueryCloseWithSingleScan(String colQueryId, String planNodeId, long offset, String seriesPath, boolean isCloudEqual) throws TException {
        QueryStateManager queryStateManager = ColQuerySessions.getByEdgeQueryId(colQueryId);
        if (queryStateManager == null) return;
        while(!queryStateManager.getSourceHandle().isFinished()){
            try{
                Thread.sleep(10);
                System.out.println("等待关闭SInk channel");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        //更新全部算子状态
//        queryStateManager.setSeriesPathAndPlanNodeId(planNodeId,seriesPath);
//        queryStateManager.setScanStates(seriesPath,new QueryStateManager.ScanStates(0,offset,isCloudEqual,false,false));
        System.out.println("获取到的offset为："+offset);
        queryStateManager.updateScanOffsetByPlanNodeId(planNodeId,offset);
        queryStateManager.updateScanCouldEqualByPlanNodeId(planNodeId,isCloudEqual);
        System.out.println("设置新的offset以及数据信息：");
        System.out.println("快速查看"+queryStateManager.getStateSummary());
        queryStateManager.setSingleScan(true);
        //TODO:清除全部中间状态，恢复查询
        List<String> planNodeIds = queryStateManager.getAllScanPlanNodeIdList();
        queryStateManager.setOperatorClearManager(planNodeIds);
        queryStateManager.getStateMachine().transitionToPreClosed();
    }

    public void callAnsMessageWithLeftOuterJoin(String colQueryId, int edgeFragmentId, Map<String, ScanInfo> scanInfoMap, TimeColumn timeColumnLeft, List<Column> valueColumnsLeft,TimeColumn timeColumnRight, List<Column> valueColumnsRight) throws TException {
        try (TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9091))) {
            TProtocol protocol = new TBinaryProtocol(transport);
            E2CColService.Client client = new E2CColService.Client(protocol);
            transport.open();
            // 调用服务方法
            client.AnsMessageWithLeftOuterJoin(colQueryId, edgeFragmentId, scanInfoMap, timeColumnLeft, valueColumnsLeft, timeColumnRight, valueColumnsRight);
//            System.out.println("ansData:"+SourceId+" sent successfully.");
        } catch (TException x) {
            x.printStackTrace();
        }
    }
    public void callAnsMessage(String colQueryId, int edgeFragmentId, Map<String, ScanInfo> scanInfoMap) throws TException {
        try (TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9091))) {
            TProtocol protocol = new TBinaryProtocol(transport);
            E2CColService.Client client = new E2CColService.Client(protocol);
            transport.open();
            // 调用服务方法
            client.AnsMessage(colQueryId, edgeFragmentId, scanInfoMap);
//            System.out.println("ansData:"+SourceId+" sent successfully.");
        } catch (TException x) {
            x.printStackTrace();
        }
    }
    public void callAnsMessageWithSingleScan(String colQueryId, int edgeFragmentId, String planNodeId, long offset, String seriesPath, boolean isCloudEqual) throws TException {
        try (TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9091))) {
            TProtocol protocol = new TBinaryProtocol(transport);
            E2CColService.Client client = new E2CColService.Client(protocol);
            transport.open();
            // 调用服务方法
            client.AnsMessageWithSingleScan(colQueryId, edgeFragmentId, planNodeId, offset, seriesPath, isCloudEqual);
//            System.out.println("ansData:"+SourceId+" sent successfully.");
        } catch (TException x) {
            x.printStackTrace();
        }
    }
}
