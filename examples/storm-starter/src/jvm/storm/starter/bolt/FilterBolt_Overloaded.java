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

public class FilterBolt_Overloaded extends BaseRichBolt{
	OutputCollector _collector;
	Random _rand;
    long time_;

    public FilterBolt_Overloaded()
    {
        time_ = System.currentTimeMillis();
    }
	
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      _rand = new Random();
    }
    @Override
    public void execute(Tuple tuple) {
    	//String word = tuple.getString(0);
    //	Integer length= word.length();
    //	Utils.sleep(10); // should sleep for a hundred milliseconds

        String word = "useless";
        String spout = "no";
        Long time = -1l;
        if (tuple.contains("word"))
            word = tuple.getStringByField("word");
        if (tuple.contains("spout"))
            spout = tuple.getStringByField("spout");
        if (tuple.contains("time"))
            time = tuple.getLongByField("time");

        if ((System.currentTimeMillis() - time_)/1000 < 10*60) {
            _collector.emit(tuple, new Values(word, spout, time));
        }
        else {
            for (int i = 0; i < 30; i++) {
                _collector.emit(tuple, new Values(word, spout, time));
                System.out.println("Do more!");
            }
        }

    /*	if(_rand.nextDouble()<0.8){
           // _collector.emit(tuple, new Values(word));
    		_collector.emit(tuple, new Values(word, spout, time));
    	    _collector.ack(tuple);
    	}
    	*/
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      //declarer.declare(new Fields("word"));
        declarer.declare(new Fields("word", "spout", "time"));
    }
}

