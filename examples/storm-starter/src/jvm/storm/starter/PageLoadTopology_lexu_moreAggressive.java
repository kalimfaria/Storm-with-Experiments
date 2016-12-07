package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.bolt.AggregationBolt;
import storm.starter.bolt.FilterBolt;
import storm.starter.bolt.TestBolt;
import storm.starter.bolt.OutBolt;
import storm.starter.bolt.TransformBolt;
import storm.starter.spout.RandomLogSpout;

public class PageLoadTopology_lexu_moreAggressive {
	public static void main(String[] args) throws Exception {
		//int numBolt = 3;
		int paralellism = 50; // changed from 50 to 10
		int tasks = 1000;
		TopologyBuilder builder = new TopologyBuilder();

		
		builder.setSpout("spout_head", new RandomLogSpout(), paralellism).setNumTasks(50);;

		builder.setBolt("bolt_transform", new TransformBolt(), paralellism).shuffleGrouping("spout_head").setNumTasks(tasks);;
		builder.setBolt("bolt_filter", new FilterBolt(), paralellism).shuffleGrouping("bolt_transform").setNumTasks(tasks);;
		builder.setBolt("bolt_join", new TestBolt(), paralellism).shuffleGrouping("bolt_filter").setNumTasks(tasks);;
		builder.setBolt("bolt_filter_2", new FilterBolt(), paralellism).shuffleGrouping("bolt_join").setNumTasks(tasks);;
		builder.setBolt("bolt_aggregate", new AggregationBolt(), paralellism).shuffleGrouping("bolt_filter_2").setNumTasks(tasks);;
		builder.setBolt("bolt_output_sink", new OutBolt("sink"),paralellism).shuffleGrouping("bolt_aggregate").setNumTasks(tasks);;


		Config conf = new Config();
		conf.setTopologySlo(0.9);
		conf.setTopologySensitivity("throughput");
		conf.setDebug(true);

		conf.setNumAckers(0);

		conf.setNumWorkers(10);



		StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
				builder.createTopology());

	}

}
