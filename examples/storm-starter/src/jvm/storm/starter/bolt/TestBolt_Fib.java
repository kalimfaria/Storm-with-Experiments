package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.BusyWork.BusyWork;

import java.util.Map;

public class TestBolt_Fib extends BaseRichBolt{
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
    	 BusyWork.doWork(10000);
    	 fib(20);
      _collector.emit(tuple, new Values(tuple.getString(0)));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }
}

