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

public class TransformBolt_Fib extends BaseRichBolt{
	OutputCollector _collector;

    public long fib(int n) {
        if (n <= 1) return n;
        else return fib(n-1) + fib(n-2);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }
    @Override
    public void execute(Tuple tuple) {
    	String word = tuple.getString(0);
    	Integer length=word.length();
    	Utils.sleep(length);
        fib(20);
    	word=word.substring(0,(int)(0.8*word.length()));
      _collector.emit(tuple, new Values(word));
      //_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
}

