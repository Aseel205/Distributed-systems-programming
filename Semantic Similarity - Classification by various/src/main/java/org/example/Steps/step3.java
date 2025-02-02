package org.example.Steps;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;



public class step3 {

    public static class IdentityMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Split the input value into parts based on tab delimiter
            String[] parts = value.toString().split("\t");

            if (parts.length > 0) {
                String firstWord = parts[0].trim(); // Extract the first word
                String remainingValue = value.toString().substring(firstWord.length()).trim(); // Remove the first word from the value

                // Write the first word as the key and the remaining value as the value
                context.write(new Text(firstWord), new Text(remainingValue));
            }
        }
    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private HashMap<String, Long> lexemeHash = new HashMap<>();   //<L=l>
        private HashMap<String, Long> featureHash = new HashMap<>();   //<F=f>
        private HashMap<Integer, String> indexToFeatureMap = new HashMap<>(); //<IndexToFeature mapping>
        private long lexemeCount = 4216398489l; //<L>
        private long featureCount = 5058942818l; // <F>
        private String features_LexemesFile_path = "s3://aseelhamzahw3/smallOuputs/combined1/";
        private String features_sorted_path = "s3://aseelhamzahw3/input files/features.txt" ;

        protected String IndexToFeature(int index) {
            return indexToFeatureMap.getOrDefault(index, "Unknown_Feature");
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // Set up S3 client
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

            // Parse the S3 path for features_sorted_path
            String s3Bucket = features_sorted_path.split("/")[2];
            String s3Key = features_sorted_path.substring(features_sorted_path.indexOf("/", 5) + 1);

            // Read the sorted features file from S3
            S3Object s3Object = s3Client.getObject(s3Bucket, s3Key);
            List<String> features = new ArrayList<>();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    features.add(line.trim()); // Add each feature to the list
                }
            }

            // Sort the features
            Collections.sort(features);

            // Populate indexToFeatureMap with sorted features
            int index = 0;
            for (String feature : features) {
                indexToFeatureMap.put(index++, feature);
            }


            // Parse the S3 path for features_LexemesFile_path
            s3Bucket = features_LexemesFile_path.split("/")[2];
            String s3Prefix = features_LexemesFile_path.substring(features_LexemesFile_path.indexOf("/", 5) + 1);

            // List files in the S3 directory (using the prefix)
            List<S3ObjectSummary> fileSummaries = s3Client.listObjects(s3Bucket, s3Prefix).getObjectSummaries();

            // Iterate through each file and process it
            for (S3ObjectSummary fileSummary : fileSummaries) {
                // Read the file from S3
                S3Object lexemeObject = s3Client.getObject(s3Bucket, fileSummary.getKey());
                try (BufferedReader countsReader = new BufferedReader(new InputStreamReader(lexemeObject.getObjectContent()))) {
                    String line;
                    while ((line = countsReader.readLine()) != null) {
                        String[] parts = line.split("\t"); // Assuming tab-separated values
                        if (parts.length == 2) {
                            String key = parts[0];
                            long count = Long.parseLong(parts[1]);

                            if (key.startsWith("$"))  // Lexeme
                                lexemeHash.put(key.substring(1).trim().toLowerCase(), count);
                            else if (key.startsWith("@"))  // Feature
                                featureHash.put(key.substring(1).trim().toLowerCase(), count);
                             else if (key.startsWith("L"))
                                lexemeCount = Integer.parseInt((key.substring(1).trim()));
                             else if (key.startsWith("F"))
                                featureCount = Integer.parseInt((key.substring(1).trim()));

                        }
                    }
                }
            }
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long[] featureVector = new long[1000];

            // Populate feature vector
            Text value = values.iterator().next(); // Assuming each lexeme only has a single value.
            String[] parts = value.toString().split(",");
            for (int i = 0; i < parts.length; i++) {
                featureVector[i] = Long.parseLong(parts[i]);
            }

            // Compute association measures
            long lexemeOccurrences = lexemeHash.get(key.toString().trim().toLowerCase());
            double P_l = (double) lexemeOccurrences / lexemeCount;


            StringBuilder output = new StringBuilder();

            for (int i = 0; i < featureVector.length; i++) {
                double assocfreq = 0.0, assocprob = 0.0, assocPMI = 0.0, assoct_test = 0.0;

                if (featureVector[i] > 0) {
                    String feature = IndexToFeature(i);
                    long featureOccurrences = featureHash.get(feature.trim().toLowerCase());
                    double P_f = (double) featureOccurrences / featureCount;
                    double P_lf = (double) featureVector[i] / featureCount;

                    // Calculate association measures
                    assocfreq = P_lf;
                    assocprob = P_lf / P_l;
                    assocPMI = Math.log(P_lf / (P_l * P_f)) / Math.log(2);
                    assoct_test = (P_lf - (P_l * P_f)) / Math.sqrt(P_l * P_f);
                }

                // Append the four values (regardless of featureVector[i])
                output.append(assocfreq).append(",")
                        .append(assocprob).append(",")
                        .append(assocPMI).append(",")
                        .append(assoct_test);

                if (i < featureVector.length - 1) {
                    output.append("\t"); // Separate values for each feature with a tab
                }
            }
            // Write the lexeme and its corresponding 4000-length vector
            context.write(new Text(key.toString().toLowerCase()), new Text(output.toString()));
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

        FileInputFormat.addInputPath(job, new Path(args[1])); // first arg
        FileOutputFormat.setOutputPath(job, new Path(args[2]));// second  arg


        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}