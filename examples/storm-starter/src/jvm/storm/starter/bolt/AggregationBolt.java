package storm.starter.bolt;

import java.util.Map;

import storm.starter.ExclamationTopology.ExclamationBolt;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class AggregationBolt extends BaseRichBolt{
	OutputCollector _collector;



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
        String spout = "no";
        Long time = -1l;

        if (tuple.contains("word"))
            word = tuple.getStringByField("word");
        if (tuple.contains("spout"))
            spout = tuple.getStringByField("spout");
        if (tuple.contains("time"))
            time = tuple.getLongByField("time");
        //if(_rand.nextDouble()<0.8){
            // _collector.emit(tuple, new Values(word));
            _collector.emit(tuple, new Values(word, spout, time));
      //_collector.emit(tuple, new Values(word));
         _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      //declarer.declare(new Fields("word"));
        declarer.declare(new Fields("word", "spout", "time"));
    }
}

