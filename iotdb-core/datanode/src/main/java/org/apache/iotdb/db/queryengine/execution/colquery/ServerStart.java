package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.iotdb.db.queryengine.execution.colquery.colservice.C2EColService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.net.InetSocketAddress;

public class ServerStart {

    public void start(){
        //多线程非阻塞
        try{
            ColQueryConfig cfg = ColQueryConfig.getInstance();
            String bindIp = cfg.getBindIp();//可能以后会改为只有云可以访问
            int rpcPort = cfg.getLocalRpcPort();
            TNonblockingServerSocket transport = new TNonblockingServerSocket(new InetSocketAddress(bindIp, rpcPort));
            C2EColService.Processor processor = new C2EColService.Processor(new ServiceImpl());
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            TFramedTransport.Factory tTransport = new TFramedTransport.Factory();

            THsHaServer.Args targs = new THsHaServer.Args(transport);
            targs.processor(processor);
            targs.protocolFactory(protocolFactory);
            targs.transportFactory(tTransport);

            TServer server = new THsHaServer(targs);
            server.serve();
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
