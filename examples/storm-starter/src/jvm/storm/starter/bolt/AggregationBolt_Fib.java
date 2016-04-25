package storm.starter.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class AggregationBolt_Fib extends BaseRichBolt{
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
    	byte b=0x10;
    	word+=Byte.toString(b);
    	word+=Byte.toString(b);
    	word+=Byte.toString(b);
    	word+=Byte.toString(b);
        fib(47);
      _collector.emit(tuple, new Values(word));
      //_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
}
