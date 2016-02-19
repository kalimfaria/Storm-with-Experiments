package storm.starter;

import storm.starter.bolt.AggregationBolt;
import storm.starter.bolt.FilterBolt;
import storm.starter.bolt.TestBolt;
import storm.starter.bolt.TransformBolt;
import storm.starter.spout.RandomLogSpout;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class PageLoadTopologyScaledOut {
    public static void main(String[] args) throws Exception {
        //int numBolt = 3;
        int paralellism = 25;

        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout("spout_head", new RandomLogSpout(), paralellism);

        builder.setBolt("bolt_transform", new TransformBolt(), paralellism).shuffleGrouping("spout_head");
        builder.setBolt("bolt_filter", new FilterBolt(), paralellism).shuffleGrouping("bolt_transform");
        builder.setBolt("bolt_join", new TestBolt(), paralellism).shuffleGrouping("bolt_filter");
        builder.setBolt("bolt_filter_2", new FilterBolt(), paralellism).shuffleGrouping("bolt_join");
        builder.setBolt("bolt_aggregate", new AggregationBolt(), paralellism).shuffleGrouping("bolt_filter_2");
        builder.setBolt("bolt_output_sink", new TestBolt(),paralellism).shuffleGrouping("bolt_aggregate");


        Config conf = new Config();
        conf.setDebug(true);
        conf.setTopologySlo(1.0);

        conf.setNumAckers(0);

        conf.setNumWorkers(5);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
                builder.createTopology());

    }

}
