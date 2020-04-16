package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.text.DecimalFormat;

public class Q4b {

  public static class DegreeMapper
          extends Mapper<Object, Text, Text, DoubleWritable>{

    private Text passenger_count = new Text();
    private DoubleWritable total_fare = new DoubleWritable();

    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      String [] line = value.toString().split("\t");
      if(line.length == 4){
        passenger_count.set(line[2]);
        total_fare.set(Double.valueOf(line[3]).doubleValue());
        context.write(passenger_count, total_fare);
      }
    }
  }

  public static class DegreeReducer
          extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private double avg_fare = 0;

    //private static DecimalFormat df = new DecimalFormat("#,###,###.##");
    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      double sum = 0;
      double ii = 0;
      int i = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
        i += 1;
      }
      ii = Double.valueOf(i);
      avg_fare = Double.valueOf(sum/ii);
      String formattedString = String.format("%.02f", avg_fare);
      double formattedString1 = Double.valueOf(formattedString);
      context.write(key, new DoubleWritable(formattedString1));
    }
  }

  public static void main(String[] args) throws Exception {
    /* TODO: Update variable below with your gtid */
    final String gtid = "zzheng309";

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Q4b");
    job.setJarByClass(Q4b.class);
    job.setMapperClass(DegreeMapper.class);
    job.setCombinerClass(DegreeReducer.class);
    job.setReducerClass(DegreeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    /* TODO: Needs to be implemented */

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
