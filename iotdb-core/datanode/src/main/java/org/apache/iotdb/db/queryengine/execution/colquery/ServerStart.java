package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.iotdb.db.queryengine.execution.colquery.colservice.E2CColService;
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
            String bindIp = cfg.getBindIp();
            int rpcPort = cfg.getLocalRpcPort();
            TNonblockingServerSocket transport = new TNonblockingServerSocket(new InetSocketAddress(bindIp, rpcPort));
            E2CColService.Processor processor = new E2CColService.Processor(new ServiceImpl());
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
