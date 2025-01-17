package org.example;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.HashSet;


public class extractFeatures {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private HashSet<String> Lexems = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // Use relative path for unique_words.txt
            Path filePath = new Path("src/main/resources/unique_words.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));

            String line;
            while ((line = reader.readLine()) != null) {
                Lexems.add(line.trim());
            }
            reader.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line into fields
            String[] parts = value.toString().split("\t");
            if (parts.length < 4) return; // Ensure valid format

            String rootWord = parts[0]; // The head_word (root)
            String syntacticNgram = parts[1]; // The syntactic-ngram field
            int totalCount;

            try {
                totalCount = Integer.parseInt(parts[2]); // The total_count field
            } catch (NumberFormatException e) {
                return; // Skip if total_count is not a valid integer
            }

            // Check if the root word is in the HashSet
            if (!Lexems.contains(rootWord)) {
                return; // Skip this line if the root word is not in the HashSet
            }

            // Process the syntactic-ngram field
            String[] tokens = syntacticNgram.split(" ");
            for (String token : tokens) {
                String[] tokenParts = token.split("/"); // Extract word from the token
                if (tokenParts.length < 4) continue; // Ensure correct token format

                String featureWord = tokenParts[0]; // The actual word from the ngram

                // Emit (featureWord, totalCount)
                context.write(new Text(featureWord), new Text(totalCount + ""));
            }
        }
    }



    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key , values.iterator().next());
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }

            context.write(key, new Text(sum + ""));

        }
    }



    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(extractFeatures.class);

        job.setMapperClass(extractFeatures.MapperClass.class);     // mapper
        job.setReducerClass(extractFeatures.ReducerClass.class);          // reducer


        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("s3://aseelhamzahw3/hw3_output.txt") );



        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}