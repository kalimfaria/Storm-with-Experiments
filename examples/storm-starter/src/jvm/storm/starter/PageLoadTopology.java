package storm.starter;

import storm.starter.bolt.*;
import storm.starter.spout.RandomLogSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class PageLoadTopology {
	public static void main(String[] args) throws Exception {
		//int numBolt = 3;
		//int paralellism = 10; // changed from 50 to 10
		int paralellism = 5; // changed from 10 to 5

		TopologyBuilder builder = new TopologyBuilder();

		
		builder.setSpout("spout_head", new RandomLogSpout(), paralellism).setNumTasks(20);;

		builder.setBolt("bolt_transform", new TransformBolt(), paralellism).shuffleGrouping("spout_head").setNumTasks(20);;
		builder.setBolt("bolt_filter", new FilterBolt(), paralellism).shuffleGrouping("bolt_transform").setNumTasks(20);;
		builder.setBolt("bolt_join", new TestBolt(), paralellism).shuffleGrouping("bolt_filter").setNumTasks(20);;
		//builder.setBolt("bolt_filter_2", new FilterBolt_Overloaded(), paralellism * 4).shuffleGrouping("bolt_join").setNumTasks(20); // added to create overload
		builder.setBolt("bolt_filter_2", new FilterBolt(), paralellism).shuffleGrouping("bolt_join").setNumTasks(20); // added to create overload
		builder.setBolt("bolt_aggregate", new AggregationBolt(), paralellism).shuffleGrouping("bolt_filter_2").setNumTasks(20);;
		builder.setBolt("bolt_output_sink", new TestBolt("bolt_output_sink"),paralellism).shuffleGrouping("bolt_aggregate").setNumTasks(20);;


		Config conf = new Config();
		conf.setTopologySlo(1.0);
		conf.setTopologySensitivity("throughput");
		conf.setDebug(true);

		conf.setNumAckers(0);

		conf.setNumWorkers(5);



		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}
