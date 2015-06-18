package stormtest;

import java.io.IOException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class TopologyMain {
	public static void main(String[] args) throws InterruptedException {
		// 定义拓扑
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("word-reader", new WordReader(),2);
		
		builder.setBolt
		("word-normalizer", new WordNormalizer(),2).shuffleGrouping("word-reader").
		setNumTasks(4).setDebug(false);
		
		builder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping(
				"word-normalizer", new Fields("word"));

		
		// 配置
		Config conf = new Config();
		conf.put("wordsFile", "/home/hadoop/test.txt");
		
		conf.setDebug(false);

		// 运行拓扑
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		conf.setNumWorkers(2);//进程
		try {
//			StormSubmitter.submitTopology("Getting-Started-Topologie", conf, builder.createTopology());
			
			LocalCluster cluster = new LocalCluster();
			
			cluster.submitTopology("Getting-Started-Topologie", conf,
					builder.createTopology());
			
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
