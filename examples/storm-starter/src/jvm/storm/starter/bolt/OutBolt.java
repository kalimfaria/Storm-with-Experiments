package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.starter.BusyWork.BusyWork;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;

public class OutBolt extends BaseRichBolt {
    OutputCollector _collector;

    String bolt_name;
    File output_file;
    HashMap<String, Double> spout_latency;
    int counter;
    int BATCH_SIZE = 40;
    String topology_name;
    Long start_time;
    private static final Logger LOG = LoggerFactory.getLogger(OutBolt.class);

    public OutBolt(String bolt_name) {
        this.bolt_name = bolt_name;
        this.start_time = System.currentTimeMillis();
    }

    public OutBolt() {
        bolt_name = "no-name";
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
        String name = "/proj/Stella/logs/output-time" + context.getStormId() + context.getThisTaskId() + ".log";
        output_file = new File(name);
        writeToFile(output_file, " ");
        counter = 0;
        spout_latency = new HashMap<String, Double>();
        topology_name = context.getStormId();
    }

    @Override
    public void execute(Tuple tuple) {
        BusyWork.doWork(10000);

    /*    if (bolt_name.contains("overloaded"))
            Utils.sleep(10);
*/
        LOG.info("execute function for output bolt");
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
            LOG.info("counter: {}", counter);
            System.out.println("counter "+counter);
            if (!spout_latency.containsKey(spout)) {
                spout_latency.put(spout, (double)(System.currentTimeMillis() - time));
            } else {
                double temp_time = spout_latency.get(spout); // get old
                if (counter == 0) {
                    spout_latency.put(spout, (double)(System.currentTimeMillis() - time));
                } else {
                    temp_time = temp_time * counter;
                    temp_time += (System.currentTimeMillis() - time);
                    spout_latency.put(spout, temp_time / (counter + 1));
                }
            }
            if (counter == 19) { // so place after every 20 values
                StringBuffer output = new StringBuffer();
                for (String s : spout_latency.keySet()){
                    output.append(topology_name + "," + s + "," + bolt_name + "," + spout_latency.get(s) + "\n");
                    LOG.info("log entry: {}", topology_name + "," + s + "," + bolt_name + "," + spout_latency.get(s));
                }

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
            LOG.info("writing to file!");
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
            LOG.info("finish writing to file!");
        } catch (IOException ex) {
            System.out.println(ex.toString());
            LOG.info("IOException: {}", ex.toString());
        } catch (Exception ex) {
            System.out.println(ex.toString());
            LOG.info("Exception: {}", ex.toString());
        }
    }
}