package storm.starter.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class DoNothingSink extends BaseRichBolt{
	OutputCollector _collector;
    private File output_file;

    public long fib(int n) {
        if (n <= 1) return n;
        else return fib(n-1) + fib(n-2);
    }

    public DoNothingSink()
    {
        output_file = new File("/var/output/output.log");
    }
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }
    @Override
    public void execute(Tuple tuple)
    {
        fib(20);
        writeToFile(output_file, tuple.toString() + " \n" );
      _collector.emit(tuple, new Values(tuple.toString()));
      //_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    public void writeToFile(File file, String data) {
        try {
            FileWriter fileWriter = new FileWriter(file, true);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.append(data);
            bufferWriter.close();
            fileWriter.close();
            //  LOG.info("wrote to file {}", data);
        } catch (IOException ex) {
            System.out.println( "From the sink bolt: Exception:");
            System.out.println( ex.toString());
        }
    }
}

