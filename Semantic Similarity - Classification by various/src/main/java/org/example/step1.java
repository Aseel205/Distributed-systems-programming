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


public class step1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private HashSet<String> Lexems = new HashSet<>();
        private HashSet<String> Features = new HashSet<>();
        private Stemmer stemmer  ;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // Read lexemes file
            Path filePath = new Path("src/main/resources/lexemes.txt");
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)));
            String line;
            while ((line = reader.readLine()) != null) {
                Lexems.add(line.trim());
            }
            reader.close();

            // Read features file
            Path filePath2 = new Path("src/main/resources/features.txt");
            BufferedReader reader2 = new BufferedReader(new InputStreamReader(fs.open(filePath2)));
            String line2;
            while ((line2 = reader2.readLine()) != null) {
                Features.add(line2.trim());
            }
            reader2.close();
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line into fields
            String[] parts = value.toString().split("\t");
            if (parts.length < 3) return; // Ensure valid format

            String rootWord = parts[0];  // The head_word (root)
            String syntacticNgram = parts[1]; // The syntactic-ngram field
            int totalCount;

            try {
                totalCount = Integer.parseInt(parts[2]); // The total_count field
            } catch (NumberFormatException e) {
                return; // Skip if total_count is not a valid integer
            }

            // Check if the root word is a lexeme
            if (Lexems.contains(rootWord)) {
                context.write(new Text("L"), new Text(totalCount+"")); // Count all lexemes
                context.write(new Text( rootWord), new Text(totalCount+"")); // Count specific lexeme
            }

            // Process the syntactic n-gram to extract feature words
            String[] tokens = syntacticNgram.split(" ");
            for (String token : tokens) {
                String[] tokenParts = token.split("/"); // Extract word, POS tag, dep label, and head index from the token
                if (tokenParts.length < 4) continue; // Ensure correct token format (word/pos-tag/dep-label/head-index)

                String featureWord = tokenParts[0]; // The actual feature word
                String posTag = tokenParts[1]; // Part-of-speech tag
                String depLabel = tokenParts[2]; // Dependency label
                int headIndex;
                try {
                    headIndex = Integer.parseInt(tokenParts[3]); // Head index, handle potential parsing errors
                } catch (NumberFormatException e) {
                    continue; // Skip token if headIndex isn't a valid integer
                }

                // Apply the stemmer to the feature word
                String stemmedFeatureWord = null;
                    stemmer =  new Stemmer() ;

                    stemmer.add(featureWord.toCharArray() , featureWord.length()); // Add each character to the stemmer

                    stemmer.stem(); // Stem the word after all characters are added
                    stemmedFeatureWord = stemmer.toString(); // Retrieve the stemmed word


                if (stemmedFeatureWord == null) {
                    continue; // Skip if stemming returned null
                }

                // Only consider features connected to the root (headIndex == 1)
                if (headIndex == 1 && Features.contains(stemmedFeatureWord +  " " + depLabel   )) {     // Aseel check edge case and check M meer output  ..
                    String featureKey = "<" + stemmedFeatureWord + "," + depLabel + ">"; // Format key as <word relationship>

                    context.write(new Text("F"), new Text(String.valueOf(totalCount))); // Count all features
                    context.write(new Text(featureKey), new Text(String.valueOf(totalCount))); // Count specific feature
                }
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
        Job job = Job.getInstance(conf, "Assigment  3 step 1 ");
        job.setJarByClass(step1.class);

        job.setMapperClass(step1.MapperClass.class);     // mapper
        job.setReducerClass(step1.ReducerClass.class);          // reducer


        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
    //    FileInputFormat.addInputPath(job, new Path(args[1]));
  //      FileOutputFormat.setOutputPath(job, new Path("s3://aseelhamzahw3/hw3_output.txt") );



        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}