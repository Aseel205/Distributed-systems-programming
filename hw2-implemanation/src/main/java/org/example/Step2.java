package org.example;

import java.io.IOException;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

public class Step2 {



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
            // Split the input line by spaces
            String[] tokens = value.toString().split("\\s+");

            // Ensure the line has at least 4 fields (3-gram and match_count)
            if (tokens.length >= 4) {
                // Extract words and match_count
                String firstWord = tokens[0];
                String secondWord = tokens[1];
                String thirdWord = tokens[2];
                int matchCount;

                try {
                    matchCount = Integer.parseInt(tokens[3]); // match_count as an integer
                } catch (NumberFormatException e) {
                    return; // Skip the line if match_count is not valid
                }

                // Write the first pair: <*, *, third_word> -> match_count
                context.write(new Text("*,*, " + thirdWord), new IntWritable(matchCount));

                // Write the second pair: <*, second_word, third_word> -> match_count
                context.write(new Text("*, " + secondWord + ", " + thirdWord), new IntWritable(matchCount));

                // Write the third pair: <first_word, second_word, third_word> -> match_count
                context.write(new Text(firstWord + "," + secondWord + "," + thirdWord), new IntWritable(matchCount));
            }
        }

    }

    // LongWritable key: Represents the offset of the current line in the input file
    // Text value: Represents the line of text being processed by the mapper.
    // Context context : Acts as a bridge between the mapper and the rest of the MapReduce framework.

    public class CustomKeyComparator extends WritableComparator {

        protected CustomKeyComparator() {
            super(Text.class, true); // Ensure Text is the key type
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            String key1 = a.toString();
            String key2 = b.toString();

            // Split the keys into parts (assuming format "<first_word,second_word,third_word>")
            String[] parts1 = key1.split(",");
            String[] parts2 = key2.split(",");

            String thirdWord1 = parts1.length > 2 ? parts1[2] : "";
            String thirdWord2 = parts2.length > 2 ? parts2[2] : "";

            String secondWord1 = parts1.length > 1 ? parts1[1] : "";
            String secondWord2 = parts2.length > 1 ? parts2[1] : "";

            String firstWord1 = parts1.length > 0 ? parts1[0] : "";
            String firstWord2 = parts1.length > 0 ? parts2[0] : "";

            // 1. Compare third word with "*" having the highest priority (smallest)
            int cmp = compareWithAsterisk(thirdWord1, thirdWord2);
            if (cmp != 0) return cmp;

            // 2. Compare second word with "*" as the smallest value
            cmp = compareWithAsterisk(secondWord1, secondWord2);
            if (cmp != 0) return cmp;

            // 3. Compare first word with "*" as the smallest value
            return compareWithAsterisk(firstWord1, firstWord2);
        }

        /**
         * Helper function to compare two strings where "*" is treated as smallest.
         */
        private int compareWithAsterisk(String s1, String s2) {
            if (s1.equals("*") && !s2.equals("*")) {
                return -1; // "*" comes first
            } else if (!s1.equals("*") && s2.equals("*")) {
                return 1; // "*" comes first
            }
            return s1.compareTo(s2); // Default lexicographical comparison
        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            // Split the key by commas (assuming the format is "first_word,second_word,third_word")
            String[] parts = key.toString().split(",");

            // Extract the third word from the key
            String thirdWord = (parts.length > 2) ? parts[2] : "";

            // Hashing the third word, treating "*" as a special case.
            int hashCode = thirdWord.equals("*") ? Integer.MIN_VALUE : thirdWord.hashCode();

            // Return the partition index based on the hash code of the third word
            return (hashCode & Integer.MAX_VALUE) % numPartitions;
        }
    }

    public static class ReducerClass extends Reducer<Text,IntWritable,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Initialize temporary variables to store the first and second parameters
            int currentFirstParam = 0;  // <*,*,C>
            int currentSecondParam = 0; // <*,B,C>
            int value;
            int sum = 0;
            List<IntWritable> valueList = new ArrayList<>();


            Iterator<IntWritable> iter = values.iterator();
            value = Integer.parseInt(iter.next().toString());


            long starCount = key.toString().chars().filter(c -> c == '*').count();

            // Case : <*, B ,*>
            if (starCount == 2) {
                currentFirstParam = value;
                currentSecondParam = 0;
            }
            // Case : <A,B ,*>
            if (starCount == 1)
                currentSecondParam = value;


                // Case <A,B,C>
            else {
                String resultValue = currentFirstParam + "," + currentSecondParam + "," +value + ",0,0,0," ;
                context.write(key, new Text( resultValue));
            }
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