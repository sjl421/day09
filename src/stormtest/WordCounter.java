package stormtest;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class WordCounter implements IRichBolt {
	Integer id;
	String name;
	Map<String, Integer> counters;
	private OutputCollector collector;

	/**
	 * 这个spout结束时（集群关闭的时候），我们会显示单词数量
	 */
	@Override
	public void cleanup() {
		System.out.println("-- 单词数 【" + name + "-" + id + "】 --");
		for (Map.Entry<String, Integer> entry : counters.entrySet()) {
			System.out.println(entry.getKey() + ": " + entry.getValue());
		}
	}

	/**
	 * 为每个单词计数
	 */
	@Override
	public void execute(Tuple input) {
		String str = input.getString(0);
		/**
		 * 如果单词尚不存在于map，我们就创建一个，如果已在，我们就为它加1
		 * 此处为了学习例子，没有操作redis，此处按照流程是操作redis
		 */
		if (!counters.containsKey(str)) {
			counters.put(str, 1);
		} else {
			Integer c = counters.get(str) + 1;
			counters.put(str, c);
		}
		// 对元组作为应答
		collector.ack(input);
	}

	/**
	 * 初始化
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}