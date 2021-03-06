/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
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
import storm.starter.BusyWork.BusyWork;

import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

  public static class ExclamationBolt extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
      Utils.sleep(10*1000); // ADDED BY FARIA
      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
     // _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }


  }

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    int parallelism_hint = 10;
    builder.setSpout("word", new TestWordSpout(), parallelism_hint ).setNumTasks(100);
    builder.setBolt("exclaim1", new ExclamationBolt(), parallelism_hint  ).shuffleGrouping("word").setNumTasks(100); //parallelism_hint
    builder.setBolt("exclaim2", new ExclamationBolt(), parallelism_hint ).shuffleGrouping("exclaim1").setNumTasks(100); //parallelism_hint
    builder.setBolt("exclaim3", new ExclamationBolt(), parallelism_hint  ).shuffleGrouping("exclaim2").setNumTasks(100); //parallelism_hint
    builder.setBolt("exclaim4", new ExclamationBolt(), parallelism_hint  ).shuffleGrouping("exclaim3").setNumTasks(100);//parallelism_hint


    Config conf = new Config();
    conf.setTopologySlo(1.0);
    conf.setDebug(true);
    conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(10);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
