package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Step5Google {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new Text(""));    // he sent the key as it is
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            // Split the key into words
            String[] words = key.toString().split("\\s+");

            // Ensure there are at least two words
            if (words.length < 2) {
                return 0; // Default to partition 0 if key is invalid
            }

            // Extract the first two words
            String firstTwoWords = words[0] + " " + words[1];

            // Use the hash code of the first two words for partitioning
            return Math.abs(firstTwoWords.hashCode() % numPartitions);
        }
    }


    public static class KeyComparator extends WritableComparator {

        protected KeyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Text t1 = (Text) w1;
            Text t2 = (Text) w2;

            String[] parts1 = t1.toString().split("\\s+");
            String[] parts2 = t2.toString().split("\\s+");

            // Compare first two words
            int wordComparison = (parts1[0] + " " + parts1[1]).compareTo(parts2[0] + " " + parts2[1]);
            if (wordComparison != 0) {
                return wordComparison;
            }

            // Compare the number at the end of the line
            double num1 = Double.parseDouble(parts1[parts1.length - 1]);
            double num2 = Double.parseDouble(parts2[parts2.length - 1]);
            return - Double.compare(num1, num2);
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            String[] parts = key.toString().split("\\s+");

            if (parts.length >= 4) { // Ensure the line has at least three words and a number
                String firstThreeWords = parts[0] + " " + parts[1] + " " + parts[2];
                double number = Double.parseDouble(parts[parts.length - 1]);

                context.write(new Text(firstThreeWords), new Text(""+number));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 5 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort");
        job.setJarByClass(Step5Google.class);
        job.setMapperClass(Step5Google.MapperClass.class);
        job.setPartitionerClass(Step5Google.PartitionerClass.class);
        job.setReducerClass(Step5Google.ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(Step5Google.KeyComparator.class);


        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}




