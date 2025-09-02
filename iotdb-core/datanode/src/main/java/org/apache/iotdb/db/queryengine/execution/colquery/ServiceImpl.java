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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ServiceImpl implements C2EColService.Iface{


    @Override
    public void ACKMessage(int cloudFragmentId) throws TException {
        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        queryStateManager.getStateMachine().transitionToPreColQuery();//切换至同步索引
        queryStateManager.setCloudFragmentId(cloudFragmentId);
        int edgeFragmentId = queryStateManager.getAndAddEdgeFragmentId();
        queryStateManager.createAndSetSourceHandle();
        try {
            while (queryStateManager.getStateMachine().getState()!= ColQueryState.COL_QUERY){
                wait();
            }
        }catch (Exception e){
            System.out.println("\n等待IdentitySink失败");
        }
        if(queryStateManager.isSingleScan()){
            String planNodeId = queryStateManager.getAllScanPlanNodeIdList().get(0);
            QueryStateManager.ScanStates scanStates = queryStateManager.getAllScanStatesList().get(0);
            long offset = scanStates.getOffset();
            String seriesPath = queryStateManager.getSeriesPath(planNodeId);
            callAnsMessageWithSingleScan(edgeFragmentId,planNodeId,offset,seriesPath,false);
        }else {
            List<QueryStateManager.ScanStates>  scanStates = queryStateManager.getAllScanStatesList();
            List<String> seriesPaths = queryStateManager.getAllScanPathList();
            List<String> planNodeIds = queryStateManager.getAllScanPlanNodeIdList();
            int i=0;
            Map<String, ScanInfo> scanInfoMap = new HashMap<>();
            for(QueryStateManager.ScanStates scanState:scanStates)
            {
                ScanInfo scanInfo = ScanInfoConverter.convertToScanInfo(scanState,seriesPaths.get(i));
                scanInfoMap.put(planNodeIds.get(i),scanInfo);
                i++;
            }
            if(queryStateManager.hasLeftOuterJoin()){
                TsBlock cache = queryStateManager.getLeftOuterJoinCache();
                ScanInfoConverter.TsBlockColumns valueColumns=ScanInfoConverter.convertTsBlockToColumns(cache);
                callAnsMessageWithLeftOuterJoin(edgeFragmentId,scanInfoMap,valueColumns.getTimeColumn(),valueColumns.getValueColumns(),queryStateManager.getIsRightCache());
            }else{
                callAnsMessage(edgeFragmentId,scanInfoMap);
            }

        }
    }

    @Override
    public void ColQueryCloseWithLeftOuterJoin(Map<String, ScanInfo> scanInfoMap, TimeColumn timeColumn, List<Column> valueColumns, boolean isRightCache) throws TException {
        QueryStateManager queryStateManager=QueryStateManager.getInstance();
        //更新全部算子状态
        scanInfoMap.forEach((key, value) -> {
            queryStateManager.setSeriesPathAndPlanNodeId(value.getSeriesPath(),key);
            QueryStateManager.ScanStates scanStates = ScanInfoConverter.convertToScanStates(value);
            queryStateManager.setScanStates(value.getSeriesPath(),scanStates);
        });
        List<TSDataType> inferredTypes = valueColumns.stream()
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
        TsBlock cache = ScanInfoConverter.convertColumnsToTsBlock(timeColumn,valueColumns,inferredTypes);
        queryStateManager.setHasLeftOuterJoin(true);
        queryStateManager.setLeftOuterJoinCache(cache);
        queryStateManager.setIsRightCache(isRightCache);
        queryStateManager.getStateMachine().transitionToPreClosed();
        notifyAll();
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
    }

    @Override
    public void ColQueryClose(Map<String, ScanInfo> scanInfoMap) throws TException {
        QueryStateManager queryStateManager=QueryStateManager.getInstance();
        //更新全部算子状态
        scanInfoMap.forEach((key, value) -> {
            queryStateManager.setSeriesPathAndPlanNodeId(value.getSeriesPath(),key);
            QueryStateManager.ScanStates scanStates = ScanInfoConverter.convertToScanStates(value);
            queryStateManager.setScanStates(value.getSeriesPath(),scanStates);
        });
        queryStateManager.getStateMachine().transitionToPreClosed();
        notifyAll();

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
    }

    @Override
    public void ColQueryCloseWithSingleScan(String planNodeId, long offset, String seriesPath, boolean isCloudEqual) throws TException {
        QueryStateManager queryStateManager=QueryStateManager.getInstance();
        //更新全部算子状态
        queryStateManager.setSeriesPathAndPlanNodeId(planNodeId,seriesPath);
        queryStateManager.setScanStates(seriesPath,new QueryStateManager.ScanStates(0,offset,isCloudEqual,false,false));
        queryStateManager.setSingleScan(true);
        queryStateManager.getStateMachine().transitionToPreClosed();
        notifyAll();

        //TODO:清除全部中间状态，恢复查询
        List<String> planNodeIds = queryStateManager.getAllScanPlanNodeIdList();
        queryStateManager.setOperatorClearManager(planNodeIds);

    }

    public void callAnsMessageWithLeftOuterJoin(int edgeFragmentId, Map<String, ScanInfo> scanInfoMap, TimeColumn timeColumn, List<Column> valueColumns, boolean isRightCache) throws TException {
        try (TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9091))) {
            TProtocol protocol = new TBinaryProtocol(transport);
            E2CColService.Client client = new E2CColService.Client(protocol);
            transport.open();
            // 调用服务方法
            client.AnsMessageWithLeftOuterJoin(edgeFragmentId, scanInfoMap, timeColumn, valueColumns, isRightCache);
//            System.out.println("ansData:"+SourceId+" sent successfully.");
        } catch (TException x) {
            x.printStackTrace();
        }
    }
    public void callAnsMessage(int edgeFragmentId, Map<String, ScanInfo> scanInfoMap) throws TException {
        try (TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9091))) {
            TProtocol protocol = new TBinaryProtocol(transport);
            E2CColService.Client client = new E2CColService.Client(protocol);
            transport.open();
            // 调用服务方法
            client.AnsMessage(edgeFragmentId, scanInfoMap);
//            System.out.println("ansData:"+SourceId+" sent successfully.");
        } catch (TException x) {
            x.printStackTrace();
        }
    }
    public void callAnsMessageWithSingleScan(int edgeFragmentId, String planNodeId, long offset, String seriesPath, boolean isCloudEqual) throws TException {
        try (TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9091))) {
            TProtocol protocol = new TBinaryProtocol(transport);
            E2CColService.Client client = new E2CColService.Client(protocol);
            transport.open();
            // 调用服务方法
            client.AnsMessageWithSingleScan(edgeFragmentId, planNodeId, offset, seriesPath, isCloudEqual);
//            System.out.println("ansData:"+SourceId+" sent successfully.");
        } catch (TException x) {
            x.printStackTrace();
        }
    }
}
