package transtest;

import java.util.Map;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {
    TransactionAttempt _attempt;
    BatchOutputCollector _collector;
    int _sum = 0;
 
    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt attempt) {
        _collector = collector;
        _attempt = attempt;
    }
 
    @Override
    public void execute(Tuple tuple) {
        _sum+=tuple.getInteger(1);
        System.out.println("=========================");
        System.out.println(_sum);
        System.out.println("=========================");
    }
 
    @Override
    public void finishBatch() {
//        Value val = DATABASE.get(GLOBAL_COUNT_KEY);
//        Value newval;
//        if(val == null || !val.txid.equals(_attempt.getTransactionId())) {
//            newval = new Value();
//            newval.txid = _attempt.getTransactionId();
//            if(val==null) {
//                newval.count = _sum;
//            } else {
//                newval.count = _sum + val.count;
//            }
//            DATABASE.put(GLOBAL_COUNT_KEY, newval);
//        } else {
//            newval = val;
//        }
    	System.out.println("///////////////////////////");
    	System.out.println(_sum);
    	System.out.println("///////////////////////////");
        _collector.emit(new Values(_attempt, _sum));
    }
 
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "sum"));
    }
}