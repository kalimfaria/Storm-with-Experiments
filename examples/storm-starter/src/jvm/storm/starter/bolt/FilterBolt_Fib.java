package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class FilterBolt_Fib extends BaseRichBolt{
	OutputCollector _collector;
	Random _rand;

    public long fib(int n) {
        if (n <= 1) return n;
        else return fib(n-1) + fib(n-2);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      _rand = new Random();
    }
    @Override
    public void execute(Tuple tuple) {
    	String word = tuple.getString(0);
    	Integer length=word.length();
    	Utils.sleep(length); // SLEEPING ALREADY
        fib(20);
    	if(_rand.nextDouble()<0.8){
    		_collector.emit(tuple, new Values(tuple.getString(0)));
    	    //_collector.ack(tuple);
    	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
}

