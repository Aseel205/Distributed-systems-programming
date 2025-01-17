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


public class step2 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private HashSet<String> Lexems = new HashSet<>();
        private HashSet<String> Features = new HashSet<>();
        private Stemmer stemmer  ;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // Load lexemes from lexemes.txt
            Path lexemePath = new Path("src/main/resources/lexemes.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader lexemeReader = new BufferedReader(new InputStreamReader(fs.open(lexemePath)));

            String line;
            while ((line = lexemeReader.readLine()) != null) {
                Lexems.add(line.trim());
            }
            lexemeReader.close();

            // Load features from features.txt
            Path featurePath = new Path("src/main/resources/features.txt");
            BufferedReader featureReader = new BufferedReader(new InputStreamReader(fs.open(featurePath)));

            while ((line = featureReader.readLine()) != null) {
                Features.add(line.trim());
            }
            featureReader.close();
        }

            // to do implement this function
        protected int featureToIndex(String feature) {
            return feature.hashCode();  // Example: Using hashCode for simplicity
        }

        protected String IndexToFeature(int index) {
            return  index + ""  ;
        }



        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line into fields
            String[] parts = value.toString().split("\t");
            if (parts.length < 4) return; // Ensure valid format

            String rootWord = parts[0];  // The head_word (root lexeme)
            String syntacticNgram = parts[1];  // The syntactic-ngram field
            int totalCount;

            totalCount = Integer.parseInt(parts[2]);  // The total_count field

            // Check if the root word is a lexeme
            if (!Lexems.contains(rootWord)) {
                return;
            }

            // Process the syntactic-ngram field
            String[] tokens = syntacticNgram.split(" ");
            for (String token : tokens) {
                String[] tokenParts = token.split("/"); // Extract word from the token
                if (tokenParts.length < 4) continue; // Ensure valid token format

                String featureWord = tokenParts[0];  // The actual word from the ngram
                String posTag = tokenParts[1];  // POS tag (unused for now)
                String depLabel = tokenParts[2];  // Dependency label (unused for now)
                int headIndex = Integer.parseInt(tokenParts[3]);  // Head index

                // Apply the stemmer to the feature word
                stemmer.add(featureWord.toCharArray(), featureWord.length());
                String stemmedFeatureWord = stemmer.toString();

                // Only consider features pointing to the root (headIndex == 1)
                if (headIndex == 1 && Features.contains(stemmedFeatureWord +  " " + depLabel ) ) {
                    int featureIndex = featureToIndex(stemmedFeatureWord + " " + depLabel );       // Aseel

                    // Emit <lexeme, h(feature)>, count
                    context.write(new Text(rootWord), new Text(featureIndex + "," + totalCount));
                }
            }
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int[] featureCounts = new int[1000];

            // Initialize the feature counts array to zero
            for (int i = 0; i < featureCounts.length; i++) {
                featureCounts[i] = 0;
            }

            // Process each value associated with the key
            for (Text value : values) {
                // Split the value into featureIndex and count
                String[] parts = value.toString().split(",");
                if (parts.length == 2) {
                    int featureIndex = Integer.parseInt(parts[0]);
                    int count = Integer.parseInt(parts[1]);

                    // Increment the count for the specific featureIndex
                    if (featureIndex >= 0 && featureIndex < featureCounts.length) {
                        featureCounts[featureIndex] += count;
                    }
                }
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < featureCounts.length; i++) {
                sb.append(featureCounts[i]);
                if (i < featureCounts.length - 1) {
                    sb.append(",");  // Use comma to separate counts
                }
            }

            // Write the result: rootWord and the feature counts array
            context.write(key, new Text(sb.toString()));
        }
    }



    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Assigment3 step2 ");
        job.setJarByClass(step2.class);

        job.setMapperClass(step2.MapperClass.class);     // mapper
        job.setReducerClass(step2.ReducerClass.class);          // reducer


        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
     //   FileOutputFormat.setOutputPath(job, new Path("s3://aseelhamzahw3/hw3_output.txt") );



        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}