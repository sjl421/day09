package transtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
		// 定义拓扑
		Map map=new HashMap();
		List list1=new ArrayList();
		List objs1=new ArrayList();
		objs1.add("aaa");
		list1.add(objs1);
		list1.add(objs1);
		list1.add(objs1);
		list1.add(objs1);
		list1.add(objs1);
		list1.add(objs1);
		List list2=new ArrayList();
		List objs2=new ArrayList();
		objs2.add("eee");
		list2.add(objs2);
		list2.add(objs2);
		list2.add(objs2);
		list2.add(objs2);
		list2.add(objs2);
		list2.add(objs2);
		map.put(0, list1);
		map.put(1, list2);
		MemoryTransactionalSpout spout = new MemoryTransactionalSpout(map, new Fields("word"), 1);
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 2);
		builder.setBolt("partial-count", new BatchCount())
		        .shuffleGrouping("spout");
		builder.setBolt("sum", new UpdateGlobalCount())
		        .globalGrouping("partial-count");
		
		// 配置
		Config conf = new Config();
		
		conf.setDebug(false);

		// 运行拓扑
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		conf.setNumWorkers(2);//进程
		try {
//			StormSubmitter.submitTopology("Getting-Started-Topologie", conf, builder.buildTopology());
			
			LocalCluster cluster = new LocalCluster();
			
			cluster.submitTopology("Getting-Started-Topologie", conf,
					builder.buildTopology());
			
			System.in.read();
			cluster.killTopology("Getting-Started-Topologie"); 
			cluster.shutdown();
//		} catch (AlreadyAliveException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (InvalidTopologyException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}
}
