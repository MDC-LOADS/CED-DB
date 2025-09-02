package org.apache.iotdb.db.queryengine.execution.colquery;

import org.apache.iotdb.db.queryengine.execution.colquery.colservice.C2EColService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.net.InetSocketAddress;

public class ServerStart {
    
    private static final String broadcastIp = "0.0.0.0";
    private static final int RPCPort = 9090;

    public void start(){
        //多线程非阻塞
        try{
            TNonblockingServerSocket transport =new TNonblockingServerSocket(new InetSocketAddress(broadcastIp, RPCPort));//9090
            C2EColService.Processor processor = new C2EColService.Processor(new ServiceImpl());
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            TFramedTransport.Factory tTransport = new TFramedTransport.Factory();

            THsHaServer.Args targs = new THsHaServer.Args(transport);
            targs.processor(processor);
            targs.protocolFactory(protocolFactory);
            targs.transportFactory(tTransport);

            TServer server = new THsHaServer(targs);
//            System.out.println("Starting the edge server...");
            server.serve();
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
