package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.iotdb.db.queryengine.execution.colquery.colservice.E2CColService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;

public class ResourceMonitor {
    private volatile boolean couldColQuery;

    public ResourceMonitor() {
        couldColQuery = false;
    }

    public boolean isCouldColQuery() {
        return couldColQuery;
    }

    public void setCouldColQuery(boolean couldColQuery) {
        this.couldColQuery = couldColQuery;
    }

    public void startResourceMonitor() {
        //TODO:是否可以协同查询
//        QueryStateManager queryStateManager = QueryStateManager.getInstance();
//        ColQueryStateMachine stateMachine = queryStateManager.getStateMachine();
//        if(couldColQuery && stateMachine.getState() == ColQueryState.CLOSED){
//            //TODO:调用启动程序
//            Thread queryExecution = new Thread(new StartColQuery());
//            queryExecution.start();
//            return;
//        } else if (!couldColQuery && stateMachine.getState() == ColQueryState.COL_QUERY) {
//            queryStateManager.getStateMachine().transitionToPreClosed();
//            //TODO:调用关闭程序
//            return;
//        }
    }

    public static void startColQuery(String sql,String colQueryId) {

//        QueryStateManager queryStateManager = QueryStateManager.getInstance();
        ColQueryConfig cfg = ColQueryConfig.getInstance();
        try (TTransport transport = new TFramedTransport(new TSocket(cfg.getRemoteIp(), cfg.getRemoteRpcPort()))) {
            TProtocol protocol = new TBinaryProtocol(transport);
            E2CColService.Client client = new E2CColService.Client(protocol);
            transport.open();
            // 调用服务方法，使用协同通道 id（edgeQueryId-dataNodeId）
            client.ColQueryStart(sql, colQueryId);
//            System.out.println("ansData:"+SourceId+" sent successfully.");
        } catch (TException x) {
            x.printStackTrace();
        }
//        System.out.println("协同查询状态机变为启动啦");
        QueryStateManager queryStateManager = ColQuerySessions.getByEdgeQueryId(colQueryId);
        if(queryStateManager != null) {
            queryStateManager.getStateMachine().transitionToStart();
        }
    }


    static class StartColQuery implements Runnable {
        @Override
        public void run() {
//            QueryStateManager queryStateManager = QueryStateManager.getInstance();
//            try (TTransport transport = new TFramedTransport(new TSocket("127.0.0.1", 9091))) {
//                TProtocol protocol = new TBinaryProtocol(transport);
//                E2CColService.Client client = new E2CColService.Client(protocol);
//                transport.open();
//                // 调用服务方法
//                String edgeQueryId = queryStateManager.getQueryId().getId();
//                int dataNodeId = org.apache.iotdb.db.conf.IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
//                String colQueryId = edgeQueryId + "-" + dataNodeId;
//                client.ColQueryStart(queryStateManager.getSql(), colQueryId);
////            System.out.println("ansData:"+SourceId+" sent successfully.");
//            } catch (TException x) {
//                x.printStackTrace();
//            }
//            queryStateManager.getStateMachine().transitionToStart();

        }
    }

}
