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
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import storm.starter.bolt.TestBolt;
import storm.starter.spout.TestSpout;

public class DiamondTopology_2 {
  public static void main(String[] args) throws Exception {
    int paralellism = 6;

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout_head", new TestSpout(), paralellism).setNumTasks(4);

    builder.setBolt("bolt_1", new TestBolt(), paralellism).setNumTasks(12).shuffleGrouping("spout_head");
    builder.setBolt("bolt_2", new TestBolt(), paralellism).setNumTasks(12).shuffleGrouping("spout_head");
    builder.setBolt("bolt_3", new TestBolt(), paralellism).setNumTasks(12).shuffleGrouping("spout_head");
    builder.setBolt("bolt_4", new TestBolt(), paralellism).setNumTasks(12).shuffleGrouping("spout_head");

    BoltDeclarer output = builder.setBolt("bolt_output_3", new TestBolt(), paralellism).setNumTasks(12);
    output.shuffleGrouping("bolt_1");
    output.shuffleGrouping("bolt_2");
    output.shuffleGrouping("bolt_3");
    output.shuffleGrouping("bolt_4");

    Config conf = new Config();
    conf.setDebug(true);

    conf.setNumAckers(0);
    conf.setTopologySlo(0.2);

    conf.setNumWorkers(5);

    StormSubmitter.submitTopologyWithProgressBar(args[0], conf,
            builder.createTopology());

  }

}