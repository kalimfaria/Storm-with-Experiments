package storm.starter;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.starter.BusyWork.BusyWork;
import storm.starter.bolt.*;
import storm.starter.spout.RandomLogSpout;
import storm.starter.spout.SeriesSpout;

import java.util.Map;
import java.util.Random;


public class TupleDropper {





    public static void main(String[] args) throws Exception {
        int paralellism = 10; // changed from 50 to 10

        TopologyBuilder builder = new TopologyBuilder();
        // SOMETHING IS WRONG WITH THIS TOPOLOGY
        builder.setSpout("series_spout", new SeriesSpout(), 1).setNumTasks(1);
        builder.setBolt("bolt_1", new DoNothingBolt(), 1).shuffleGrouping("series_spout").setNumTasks(1);
        builder.setBolt("bolt_2", new DoNothingBolt(), 1).shuffleGrouping("bolt_1").setNumTasks(1);
        builder.setBolt("bolt_3", new DoNothingBolt(), 1).shuffleGrouping("bolt_2").setNumTasks(1);
        builder.setBolt("bolt_4", new DoNothingBolt(), 1).shuffleGrouping("bolt_3").setNumTasks(1);
        builder.setBolt("bolt_5", new DoNothingBolt(), 1).shuffleGrouping("bolt_4").setNumTasks(1);
        builder.setBolt("bolt_output", new DoNothingSink() ,1).shuffleGrouping("bolt_5").setNumTasks(1);

        Config conf = new Config();
        conf.setTopologySlo(1.0);
        conf.setDebug(true);

        conf.setNumAckers(0);

        conf.setNumWorkers(3);

        StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
                builder.createTopology());

    }

}
