package edu.gatech.cse6242;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class Q4a {

	public static class DegreeMapper
	    extends Mapper<Object, Text, Text, IntWritable>{

	    private final static IntWritable plus = new IntWritable(1);
	    private final static IntWritable minus = new IntWritable(-1);
	    private Text node = new Text();

	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	      String [] line = value.toString().split("\t");
	      if(line.length == 4){
                node.set(line[0]);
                context.write(node, plus);
                node.set(line[1]);
                context.write(node, minus);
            }
    	}
  	}

	public static class DegreeReducer
	    extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable count = new IntWritable(1);

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      count.set(sum);
	      context.write(key, count);
	    }
	}

	public static class CountMapper
	    extends Mapper<Object, Text, Text, IntWritable>{

	    private final static IntWritable one = new IntWritable(1);
	    private Text node = new Text();

	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	      String [] line  = value.toString().split("\t");
	        if(line.length > 1){
	        	node.set(line[1]);
	        	context.write(node, one);
	        }
	    }
	}

	public static class CountReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable count = new IntWritable();

	    public void reduce(Text key, Iterable<IntWritable> values,
	                       Context context
	                       ) throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable val : values) {
	        sum += val.get();
	      }
	      count.set(sum);
	      context.write(key, count);
	    }
	  }

  public static void main(String[] args) throws Exception {


    /* TODO: Needs to be implemented */
	final String gtid = "zzheng309";

	Configuration conf = new Configuration();
	Job job1 = Job.getInstance(conf, "Q4a");
    job1.setJarByClass(Q4a.class);
    job1.setMapperClass(DegreeMapper.class);
    job1.setCombinerClass(DegreeReducer.class);
    job1.setReducerClass(DegreeReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path("intermediate"));
    job1.waitForCompletion(true);

    Job job2 = Job.getInstance(conf, "Q4a_2");
    job2.setJarByClass(Q4a.class);
    job2.setMapperClass(CountMapper.class);
    job2.setCombinerClass(CountReducer.class);
    job2.setReducerClass(CountReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job2, new Path("intermediate"));
    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
