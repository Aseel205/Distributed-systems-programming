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


public class step3 {

    public static class IdentityMapper extends Mapper<Text, Text, Text, Text> {

       // the key will be the <L=l , F=f>
        protected void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value); // Directly pass key-value pair to reducer
        }
    }



    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private HashMap<String, Integer> lexemeHash = new HashMap<>();   //<L=l>
        private HashMap<String, Integer> featureHash = new HashMap<>();   //<F=f>
        private int lexemeCount = 0;                                       //<L>
        private int featureCount = 0;                                      // <F>

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            FileSystem fs = FileSystem.get(context.getConfiguration());

            // Read lexeme counts from file
            Path lexemePath = new Path("src/main/resources/lexeme_counts.txt");
            BufferedReader lexemeReader = new BufferedReader(new InputStreamReader(fs.open(lexemePath)));

            String line;
            while ((line = lexemeReader.readLine()) != null) {
                String[] parts = line.split("\t"); // Assuming tab-separated values
                if (parts.length == 2) {
                    lexemeHash.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
            lexemeReader.close();

            // Read feature counts from file
            Path featurePath = new Path("src/main/resources/feature_counts.txt");
            BufferedReader featureReader = new BufferedReader(new InputStreamReader(fs.open(featurePath)));

            while ((line = featureReader.readLine()) != null) {
                String[] parts = line.split("\t"); // Assuming tab-separated values
                if (parts.length == 2) {
                    featureHash.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
            featureReader.close();

            // Read total counts from file
            Path totalCountsPath = new Path("src/main/resources/total_counts.txt");
            BufferedReader totalCountsReader = new BufferedReader(new InputStreamReader(fs.open(totalCountsPath)));

            while ((line = totalCountsReader.readLine()) != null) {
                String[] parts = line.split("\t"); // Assuming "lexemeCount<TAB>featureCount"
                if (parts.length == 2) {
                    lexemeCount = Integer.parseInt(parts[0]);
                    featureCount = Integer.parseInt(parts[1]);
                }
            }
            totalCountsReader.close();
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int[] featureVector = new int[1000];

            // Populate feature vector
            Text value = values.iterator().next(); // Assuming each lexeme only has a single value.

            String[] parts = value.toString().split(",");
            for (int i = 0; i < parts.length; i++) {
                featureVector[i] = Integer.parseInt(parts[i]);
            }

            // Compute association measures
            int lexemeOccurrences = lexemeHash.getOrDefault(key.toString(), 1);
            double P_l = (double) lexemeOccurrences / lexemeCount;

            StringBuilder output = new StringBuilder();

            for (int i = 0; i < featureVector.length; i++) {
                if (featureVector[i] > 0) {
                    String feature = IndexToFeature(i);
                    int featureOccurrences = featureHash.getOrDefault(feature, 1);
                    double P_f = (double) featureOccurrences / featureCount;
                    double P_lf = (double) featureVector[i] / featureCount;

                    // Calculate four association measures
                    double assocfreq = P_lf;
                    double assocprob = P_lf / P_l;
                    double assocPMI = Math.log(P_lf / (P_l * P_f)) / Math.log(2);
                    double assoct_test = (P_lf - (P_l * P_f)) / Math.sqrt(P_l * P_f);

                    // Append the four values for each feature
                    output.append(assocfreq).append(",")
                            .append(assocprob).append(",")
                            .append(assocPMI).append(",")
                            .append(assoct_test).append("\t");
                }
            }

            // Write the lexeme and its corresponding vector
            context.write(key, new Text(output.toString()));
        }

        protected String IndexToFeature(int index) {
            return "Feature_" + index;
        }
    }




    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Assigment3 step3");
        job.setJarByClass(step3.class);

        job.setMapperClass(step3.IdentityMapper.class);     // mapper
        job.setReducerClass(step3.ReducerClass.class);          // reducer


        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}