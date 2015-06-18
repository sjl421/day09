package drpctest;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExclaimBolt implements IBasicBolt {
	@Override
	public void cleanup() {
	}

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		System.out.println("处理数据");
		String input = tuple.getString(1);
		System.out.println("接收到的数据为:" + input);
		collector.emit(new Values(tuple.getValue(0), input + "!"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "result"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
