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
        QueryStateManager.getLock().readLock().lock();
        List<QueryStateManager.ScanStates>  scanStates =null;
        List<String> seriesPaths=null;
        List<String> planNodeIds=null;
        try{
            scanStates = queryStateManager.getAllScanStatesList();
            seriesPaths = queryStateManager.getAllScanPathList();
            planNodeIds = queryStateManager.getAllScanPlanNodeIdList();
        }finally {
            QueryStateManager.getLock().readLock().unlock();
        }

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
//            String planNodeId = queryStateManager.getAllScanPlanNodeIdList().get(0);
//            QueryStateManager.ScanStates scanStates = queryStateManager.getAllScanStatesList().get(0);
            long offset = scanStates.get(0).getOffset();
            String seriesPath = queryStateManager.getSeriesPath(planNodeIds.get(0));
            callAnsMessageWithSingleScan(edgeFragmentId,planNodeIds.get(0),offset,seriesPath,false);
            queryStateManager.getStateMachine().transitionToColQuery();
        }else {
            int i=0;
            Map<String, ScanInfo> scanInfoMap = new HashMap<>();
            for(QueryStateManager.ScanStates scanState:scanStates)
            {
                ScanInfo scanInfo = ScanInfoConverter.convertToScanInfo(scanState,seriesPaths.get(i));
                scanInfoMap.put(planNodeIds.get(i),scanInfo);
                i++;
            }
            if(queryStateManager.hasLeftOuterJoin()){
                TsBlock cacheLeft = queryStateManager.getLeftOuterJoinCacheLeft();
                TsBlock cacheRight = queryStateManager.getLeftOuterJoinCacheRight();
                ScanInfoConverter.TsBlockColumns valueColumnsLeft=ScanInfoConverter.convertTsBlockToColumns(cacheLeft);
                ScanInfoConverter.TsBlockColumns valueColumnsRight=ScanInfoConverter.convertTsBlockToColumns(cacheRight);
                if(valueColumnsRight==null && valueColumnsLeft!=null){
                    callAnsMessageWithLeftOuterJoin(edgeFragmentId,scanInfoMap,valueColumnsLeft.getTimeColumn(),valueColumnsLeft.getValueColumns(),new TimeColumn(),new ArrayList<>());
                }
                else if(valueColumnsLeft==null && valueColumnsRight!=null){
                    callAnsMessageWithLeftOuterJoin(edgeFragmentId,scanInfoMap,new TimeColumn(),new ArrayList<>(),valueColumnsRight.getTimeColumn(),valueColumnsRight.getValueColumns());

                }
                else if(valueColumnsLeft == null){
                    callAnsMessageWithLeftOuterJoin(edgeFragmentId,scanInfoMap,new TimeColumn(),new ArrayList<>(),new TimeColumn(),new ArrayList<>());
                }
                else {
                    callAnsMessageWithLeftOuterJoin(edgeFragmentId,scanInfoMap,valueColumnsLeft.getTimeColumn(),valueColumnsLeft.getValueColumns(),valueColumnsRight.getTimeColumn(),valueColumnsRight.getValueColumns());
                }
                System.out.println("协同发送给left的数据"+queryStateManager.getStateSummary());
                queryStateManager.getStateMachine().transitionToColQuery();
            }else{
                System.out.println("将要发送的索引为："+queryStateManager.getStateSummary());
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
        callAnsMessageWithLeftOuterJoin(queryStateManager.getEdgeFragmentId(), map,timeColumn,valueColumns,timeColumn,valueColumns);
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
    public void ColQueryCloseWithLeftOuterJoin(Map<String, ScanInfo> scanInfoMap, TimeColumn timeColumnLeft, List<Column> valueColumnsLeft, TimeColumn timeColumnRight, List<Column> valueColumnsRight) throws TException {
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
//        QueryStateManager.getLock().readLock().lock();
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
        //TODO:清除全部中间状态，恢复查询
        List<String> planNodeIds = queryStateManager.getAllScanPlanNodeIdList();
        queryStateManager.setOperatorClearManager(planNodeIds);
        queryStateManager.getStateMachine().transitionToPreClosed();
    }

    public void callAnsMessageWithLeftOuterJoin(int edgeFragmentId, Map<String, ScanInfo> scanInfoMap, TimeColumn timeColumnLeft, List<Column> valueColumnsLeft,TimeColumn timeColumnRight, List<Column> valueColumnsRight) throws TException {
        try (TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9091))) {
            TProtocol protocol = new TBinaryProtocol(transport);
            E2CColService.Client client = new E2CColService.Client(protocol);
            transport.open();
            // 调用服务方法
            client.AnsMessageWithLeftOuterJoin(edgeFragmentId, scanInfoMap, timeColumnLeft, valueColumnsLeft, timeColumnRight, valueColumnsRight);
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
