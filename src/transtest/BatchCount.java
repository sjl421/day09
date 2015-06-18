package transtest;

import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class BatchCount extends BaseBatchBolt {
	Object _id;
	BatchOutputCollector _collector;
	int _count = 0;

	@Override
	public void prepare(Map conf, TopologyContext context,
			BatchOutputCollector collector, Object id) {
		_collector = collector;
		_id = id;
	}

	@Override
	public void execute(Tuple tuple) {
		System.out.println(tuple.toString());
		_count++;
		System.out.println("------------------------------");
		System.out.println(_count);
		System.out.println("------------------------------");
	}

	@Override
	public void finishBatch() {
		System.out.println("+++++++++++++++++++++++++++++++++");
		System.out.println(_count);
		System.out.println("+++++++++++++++++++++++++++++++++");
		_collector.emit(new Values(_id, _count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("id", "count"));
	}
}