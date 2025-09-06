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
    public void ACKMessage(int cloudFragmentId) throws TException {

        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        queryStateManager.getStateMachine().transitionToPreColQuery();//切换至同步索引
        queryStateManager.setCloudFragmentId(cloudFragmentId);
        int edgeFragmentId = queryStateManager.getAndAddEdgeFragmentId();
        queryStateManager.createAndSetSourceHandle();
        try {
            while (!queryStateManager.isCanSendOffset()){
                System.out.println("\n等待IdentitySink中");
                Thread.sleep(10);
            }
            System.out.println("\n等待IdentitySink完成");
        }catch (Exception e){
            System.out.println("\n等待IdentitySink失败");
        }
        queryStateManager.setCanSendOffset(false);
        if(queryStateManager.isSingleScan()){
            String planNodeId = queryStateManager.getAllScanPlanNodeIdList().get(0);
            QueryStateManager.ScanStates scanStates = queryStateManager.getAllScanStatesList().get(0);
            long offset = scanStates.getOffset();
            String seriesPath = queryStateManager.getSeriesPath(planNodeId);
            callAnsMessageWithSingleScan(edgeFragmentId,planNodeId,offset,seriesPath,false);
            queryStateManager.getStateMachine().transitionToColQuery();
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
                queryStateManager.getStateMachine().transitionToColQuery();

            }else{

                callAnsMessage(edgeFragmentId,scanInfoMap);
                queryStateManager.getStateMachine().transitionToColQuery();
            }

        }

        //for test
//        AckMessageTestForAnsMessage(cloudFragmentId);
//        AckMessageTestForAnsMessageWithSingleScan(cloudFragmentId);
//        AckMessageTestForAnsMessageWithLeftJoin(cloudFragmentId);
    }

    public void AckMessageTestForAnsMessage(int cloudFragmentId) throws TException {
        //for test
        Map<String,ScanInfo> map = new HashMap<>();
        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        queryStateManager.setCloudFragmentId(cloudFragmentId);
        queryStateManager.getAndAddEdgeFragmentId();
        queryStateManager.createAndSetSourceHandle();
        queryStateManager.getStateMachine().transitionToPreColQuery();
//        map.put("4", new ScanInfo(1756819812635L,"root.ln.wf01.wt02.t2",true,false,true));
//        map.put("5", new ScanInfo(1756819812635L,"root.ln.wf01.wt02.t1",true,false,true));
        map.put("7",new ScanInfo(1756819812635L,"root.ln.wf01.wt02.t4",true,false,true));
        map.put("8",new ScanInfo(1756819812635L,"root.ln.wf01.wt02.t3",true,false,true));
        map.put("9",new ScanInfo(1756819812635L,"root.ln.wf01.wt02.t5",true,false,true));
        map.put("10",new ScanInfo(1756819812635L,"root.ln.wf01.wt02.t2",true,false,true));
        map.put("11",new ScanInfo(1756819812635L,"root.ln.wf01.wt02.t1",true,false,true));

        callAnsMessage(queryStateManager.getEdgeFragmentId(), map);
        System.out.println("\nACKMessage success");
        queryStateManager.getStateMachine().transitionToColQuery();
    }

    public void AckMessageTestForAnsMessageWithLeftJoin(int cloudFragmentId) throws TException {
        //for test
        Map<String,ScanInfo> map = new HashMap<>();
        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        queryStateManager.setCloudFragmentId(cloudFragmentId);
        queryStateManager.getAndAddEdgeFragmentId();
        queryStateManager.createAndSetSourceHandle();
        queryStateManager.getStateMachine().transitionToPreColQuery();
        map.put("13", new ScanInfo(1756819812635L,"root.ln.wf01.wt02.t2",true,true,false));
        map.put("14", new ScanInfo(1756819812635L,"root.ln.wf01.wt02.t1",true,true,false));
        map.put("15", new ScanInfo(1756819812635L,"root.ln.wf01.wt02.t4",true,false,true));
        map.put("16", new ScanInfo(1756819812635L,"root.ln.wf01.wt02.t3",true,false,true));
        TimeColumn timeColumn = new TimeColumn();
        List<Column> valueColumns = new ArrayList<>();
        callAnsMessageWithLeftOuterJoin(queryStateManager.getEdgeFragmentId(), map,timeColumn,valueColumns,false);
        System.out.println("\nACKMessage success");
        queryStateManager.getStateMachine().transitionToColQuery();
    }

    public void AckMessageTestForAnsMessageWithSingleScan(int cloudFragmentId) throws TException {
        //for test
        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        queryStateManager.setCloudFragmentId(cloudFragmentId);
        queryStateManager.getAndAddEdgeFragmentId();
        queryStateManager.createAndSetSourceHandle();
        queryStateManager.getStateMachine().transitionToPreColQuery();
        callAnsMessageWithSingleScan(queryStateManager.getEdgeFragmentId(), "1",1756819812635L,"root.ln.wf01.wt02.t1",false);
        System.out.println("\nACKMessage with Single Scan success");
        queryStateManager.getStateMachine().transitionToColQuery();
    }

    @Override
    public void ColQueryCloseWithLeftOuterJoin(Map<String, ScanInfo> scanInfoMap, TimeColumn timeColumn, List<Column> valueColumns, boolean isRightCache) throws TException {
        QueryStateManager queryStateManager=QueryStateManager.getInstance();
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
        if(queryStateManager.getLeftOuterJoinCache()!=null){
            TsBlock cache1 = queryStateManager.getLeftOuterJoinCache();
            System.out.println("leftOuterJoinCache-is right?:"+queryStateManager.getIsRightCache());
            System.out.println("\nleftOuterJoinCache:");
            long[] times =cache1.getTimeColumn().getTimes();
            org.apache.tsfile.block.column.Column[] valueColumns1 = cache1.getValueColumns();
            for(int i=0;i<cache1.getPositionCount();i++){
                System.out.println("\n时间为:"+times[i]);
            }
            for(int i=0; i<valueColumns1.length; i++){
                System.out.println("\n数值为:");
                for(int j=0;j<valueColumns1[i].getPositionCount();j++){
                    System.out.println("  "+valueColumns1[i].getDouble(j)+"  ");
                }
            }
        }else {
            System.out.println("\nleftOuterJoinCache: null");
        }
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
        queryStateManager.getStateMachine().transitionToPreClosed();

        //test end

        //TODO:清除全部中间状态，恢复查询
//        List<String> planNodeIds = queryStateManager.getAllScanPlanNodeIdList();
//        boolean hasFullOuterJoin =false;
//        boolean hasInnerJoin =false;
//        for(Map.Entry<String, ScanInfo> entry : scanInfoMap.entrySet()){
//            ScanInfo value = entry.getValue();
//            if(value.isFullOuterJoin){
//                hasFullOuterJoin = true;
//            }
//            if (value.isInnerJoin) {
//                hasInnerJoin = true;
//            }
//        }
//        if(hasFullOuterJoin){
//            planNodeIds.add("FullOuterJoin");
//        }
//        if (hasInnerJoin){
//            planNodeIds.add("InnerJoin");
//        }
//        queryStateManager.setOperatorClearManager(planNodeIds);
    }

    @Override
    public void ColQueryCloseWithSingleScan(String planNodeId, long offset, String seriesPath, boolean isCloudEqual) throws TException {
        QueryStateManager queryStateManager=QueryStateManager.getInstance();
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
        queryStateManager.getStateMachine().transitionToPreClosed();
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
