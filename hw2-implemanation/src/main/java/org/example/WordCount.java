package org.example;

import java.io.IOException;



import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
  
public class WordCount { 



public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();


    protected void setup(Context context)
            throws IOException, InterruptedException {
        // Call the base class setup if needed
        super.setup(context);

        // Your custom setup logic
        System.out.println("Custom setup logic for MyMapper!");

        // Example: Fetch a configuration property
        String myConfig = context.getConfiguration().get("my.custom.property");
        System.out.println("Custom Property: " + myConfig);
    }


    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
            // emit pair <word , one>
        }
    }
}

  // LongWritable key: Represents the offset of the current line in the input file
    // Text value: Represents the line of text being processed by the mapper.
    // Context context : Acts as a bridge between the mapper and the rest of the MapReduce framework.

  public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }
 
    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
      @Override
      public int getPartition(Text key, IntWritable value, int numPartitions) {
        return key.hashCode() % numPartitions;
      }    
    }
 
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);   //set jar
    job.setMapperClass(MapperClass.class);  // set mapper
    job.setPartitionerClass(PartitionerClass.class); // set partitioner
    job.setCombinerClass(ReducerClass.class);        //  set combiner
    job.setReducerClass(ReducerClass.class);         // set reducer
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
 
}