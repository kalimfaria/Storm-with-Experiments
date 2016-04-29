package storm.starter.bolt;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import storm.starter.BusyWork.BusyWork;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class TestBolt extends BaseRichBolt {
    OutputCollector _collector;

    String bolt_name;
    File output_file;
    HashMap<String, Long> spout_latency;
    int counter;
    String topology_name;

    public TestBolt(String bolt_name) {
        this.bolt_name = bolt_name;
    }

    public TestBolt() {
        bolt_name = "no-name";
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        String name = "/users/kalim2/output/output-time" + System.currentTimeMillis() + ".log";
        output_file = new File(name);
        counter = 0;
        spout_latency = new HashMap<String, Long>();
        topology_name = context.getStormId();

    }

    @Override
    public void execute(Tuple tuple) {
        BusyWork.doWork(10000);

        counter = (counter + 1) % 20;
        String word = "useless";
        String spout = "no";
        Long time = -1l;
        if (tuple.contains("word"))
            word = tuple.getStringByField("word");
        if (tuple.contains("spout"))
            spout = tuple.getStringByField("spout");
        if (tuple.contains("time"))
            time = tuple.getLongByField("time");

        if (bolt_name.contains("sink")) {
            if (!spout_latency.containsKey(spout)) {
                spout_latency.put(spout, System.currentTimeMillis() - time);
            } else {
                long temp_time = spout_latency.get(spout); // get old
                if (counter == 0) {
                    spout_latency.put(spout, System.currentTimeMillis() - time);
                } else {
                    temp_time = temp_time * counter;
                    temp_time += (System.currentTimeMillis() - time);
                    spout_latency.put(spout, temp_time / (counter + 1));
                }
            }
            if (counter == 0) {
                StringBuffer output = new StringBuffer();
                for (String s : spout_latency.keySet())
                    output.append(topology_name + "," + s + "," + bolt_name + "," + spout_latency.get(s) + "\n");
                writeToFile(output_file, output.toString()); //+ time + ","
            }
        }
        _collector.emit(tuple, new Values(word, spout, time)); //tuple.getString(0)
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "spout", "time"));
    }


    public void writeToFile(File file, String data) {
        try {
            FileChannel channel = new RandomAccessFile(file, "rw").getChannel();
            FileLock lock = channel.tryLock();
            while (lock == null) {
                lock = channel.tryLock();
            }
            //FileWriter fileWriter = new FileWriter(file, true);
            FileWriter fileWriter = new FileWriter(file, false);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
            lock.release();
            channel.close();
        } catch (IOException ex) {
            System.out.println(ex.toString());
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }
}