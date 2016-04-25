package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.bolt.*;
import storm.starter.spout.RandomLogSpout;
import storm.starter.spout.RandomLogSpout_WithAcking;

import java.util.Map;

public class PageLoadTopology_8_WithAcking {

	public static void main(String[] args) throws Exception {
		//int numBolt = 3;
		int paralellism = 10; // changed from 50 to 10

		TopologyBuilder builder = new TopologyBuilder();

		
		builder.setSpout("spout_head", new RandomLogSpout_WithAcking(), paralellism).setNumTasks(20);;

		builder.setBolt("bolt_transform", new TransformBolt_WithAcking(), paralellism).shuffleGrouping("spout_head").setNumTasks(20);;
		builder.setBolt("bolt_filter", new FilterBolt_WithAcking(), paralellism).shuffleGrouping("bolt_transform").setNumTasks(20);;
		builder.setBolt("bolt_join", new TestBolt(), paralellism).shuffleGrouping("bolt_filter").setNumTasks(20);; // HAS ACKING
		builder.setBolt("bolt_filter_2", new FilterBolt_WithAcking(), paralellism).shuffleGrouping("bolt_join").setNumTasks(20);;
		builder.setBolt("bolt_aggregate", new AggregationBolt_WithAcking(), paralellism).shuffleGrouping("bolt_filter_2").setNumTasks(20);;
		builder.setBolt("bolt_output_sink", new TestBolt(),paralellism).shuffleGrouping("bolt_aggregate").setNumTasks(20);;


		Config conf = new Config();
		conf.setTopologySlo(0.8);
		conf.setDebug(true);

		conf.setNumAckers(10);

		conf.setNumWorkers(5);



		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}
