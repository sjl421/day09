package drpctest;

import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

public class DRPCServer{

	public static void main(String args[]) throws IOException, InterruptedException, AlreadyAliveException, InvalidTopologyException {
		System.out.println("testDrpc开始");
		LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
		builder.addBolt(new ExclaimBolt(), 3);
		
		
		Config conf = new Config();
		conf.setDebug(true);
		conf.setNumWorkers(2);
		conf.setMaxSpoutPending(1);
		
//		 StormSubmitter.submitTopology("exclamation", conf,
//			     builder.createRemoteTopology());
		
		LocalDRPC drpc = new LocalDRPC();
		LocalCluster cluster = new LocalCluster();
		
		cluster.submitTopology("DRPCTest", conf,
				builder.createLocalTopology(drpc));
		System.out.println("------------------------------------");
		Thread.sleep(3000);
		System.out.println("传入参数返回的结果:" + drpc.execute("exclamation", "hello"));
		System.in.read();

		cluster.shutdown();
		drpc.shutdown();
	}
}
