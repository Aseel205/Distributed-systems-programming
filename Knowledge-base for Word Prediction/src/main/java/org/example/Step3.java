package org.example;

import java.io.IOException;

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

public class Step3 {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        public static String[] extractValues(String input) {
            // Split the input string by space
            String[] tokens = input.split("\\s+");

            // Extract the relevant parts
            String word1 = tokens[0];  // "הכלב"
            String year = tokens[1];   // "2024"
            String count1 = tokens[2]; // "50"
            String count2 = tokens[3]; // "10"

            // Return the values in the desired format
            return new String[]{word1, year, count1, count2};
        }


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] words = extractValues(value.toString());
            Text matchCount = new Text(words[2]);


            // Write the  pair: <*,matchCount>
            context.write(new Text("*"), matchCount);


        }
    }


    // there is no need for  PartitionerClass
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            context.write(key, new Text(sum + ""));
            Configuration conf = new Configuration();
            conf.set("C0", sum+"");  // Set the value of wordsCount as "C0" in the configuration


        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, " Step3");
        job.setJarByClass(Step3.class);

        job.setMapperClass(Step3.MapperClass.class);     // mapper
        job.setPartitionerClass(Step3.PartitionerClass.class);  // partitioner
        job.setReducerClass(Step3.ReducerClass.class);          // reducer

        job.setCombinerClass(Step3.ReducerClass.class); // Use reducer as combiner if you need it

        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable


        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
