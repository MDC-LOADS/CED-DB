package org.apache.iotdb.db.service;


import com.google.common.util.concurrent.ListenableFuture;
import org.apache.iotdb.db.queryengine.execution.colquery.*;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.read.common.block.TsBlock;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ColQueryExample {
    public static void main() throws InterruptedException {


//        Thread thriftServer = new Thread(new ThriftServer());
//        thriftServer.start();
//        System.out.println("\nserver start success");

        QueryStateManager.initialize();
        QueryStateManager stateManager = QueryStateManager.getInstance();
        ExecutorService executor = Executors.newCachedThreadPool();
        ColQueryStateMachine colQueryStateMachine = new ColQueryStateMachine("ColQuery-Test", executor);
        stateManager.setStateMachine(colQueryStateMachine);
        stateManager.setSql("select t1,t2 from root.ln.wf01.wt02");
        System.out.println("\ncolQueryStateMachine start success");

        ResourceMonitor.startColQuery();
        System.out.println("\ncolQuery start success");
//        1756819524324
        while(stateManager.getStateMachine().getState()!= ColQueryState.COL_QUERY){

            Thread.sleep(10);
            System.out.println("\nWaiting for AnsMessage"+stateManager.getStateMachine().getState());
        }
        System.out.println("\nAnsMessage end success");

        ISourceHandle colSourceHandle=stateManager.getSourceHandle();
        TsBlock tsBlock_rev = null;
        if(colSourceHandle!=null){
            ListenableFuture<?> isBlocked = colSourceHandle.isBlocked();
            while (!isBlocked.isDone() && !colSourceHandle.isFinished()) {
                try {
                    Thread.sleep(10);//时间
                    System.out.println("\nwaiting sourceHandle");
                    System.out.println("isBlocked.isDone():"+isBlocked.isDone());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            while(!colSourceHandle.isFinished()) {
                if(colSourceHandle.isBlocked().isDone()  && !colSourceHandle.isFinished()){
                    tsBlock_rev = colSourceHandle.receive();
                    long[] times =tsBlock_rev.getTimeColumn().getTimes();
                    Column[] valueColumns = tsBlock_rev.getValueColumns();
                    for(int i=0;i<tsBlock_rev.getPositionCount();i++){
                        System.out.println("\n时间为:"+times[i]);
                    }
                    for(int i=0; i<valueColumns.length; i++){
                        System.out.println("\n数值为:");
                        for(int j=0;j<valueColumns[i].getPositionCount();j++){
                            System.out.println("  "+valueColumns[i].getDouble(j)+"  ");
                        }
                    }
                }
            }
        }


    }
    static class ThriftServer implements Runnable {
        @Override
        public void run() {
            ServerStart serverStart = new ServerStart();
            serverStart.start();
        }
    }


}
