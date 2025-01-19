package org.example;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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

        private HashMap<String, Integer> lexemeHash = new HashMap<>();   //<L=l>
        private HashMap<String, Integer> featureHash = new HashMap<>();   //<F=f>
        private HashMap<Integer, String> indexToFeatureMap = new HashMap<>(); //<IndexToFeature mapping>
        private int lexemeCount = 582;                                         //<L>   here no need to read from file , you will take the number manually from the file
        private int featureCount = 607;                                      // <F>     // the same thing here , no need to take it from file you will add it manually ..
        private String features_LexemesFile_path = "s3://aseelhamzahw3/outputs/step1 output.txt/";

        protected String IndexToFeature(int index) {
            return indexToFeatureMap.getOrDefault(index, "Unknown_Feature");
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // Set up S3 client
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

            // Parse the S3 path to get bucket name and key prefix
            String s3Bucket = features_LexemesFile_path.split("/")[2];
            String s3Prefix = features_LexemesFile_path.substring(features_LexemesFile_path.indexOf("/", 5) + 1);

            // List files in the S3 directory (using the prefix)
            List<S3ObjectSummary> fileSummaries = s3Client.listObjects(s3Bucket, s3Prefix).getObjectSummaries();

            // Iterate through each file and process it
            for (S3ObjectSummary fileSummary : fileSummaries) {
                // Read the file from S3
                S3Object s3Object = s3Client.getObject(s3Bucket, fileSummary.getKey());
                try (BufferedReader countsReader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
                    String line;
                    int featureIndex = 0;
                    while ((line = countsReader.readLine()) != null) {
                        String[] parts = line.split("\t"); // Assuming tab-separated values
                        if (parts.length == 2) {
                            String key = parts[0];
                            int count = Integer.parseInt(parts[1]);

                            if (key.startsWith("$")) { // Lexeme
                                lexemeHash.put(key.substring(1), count);
                            } else if (key.startsWith("@")) { // Feature
                                featureHash.put(key.substring(1), count);
                                indexToFeatureMap.put(featureIndex++, key.substring(1)); // Map index to feature
                            }
                        }
                    }
                }
            }
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
                double assocfreq = 0.0, assocprob = 0.0, assocPMI = 0.0, assoct_test = 0.0;

                if (featureVector[i] > 0) {
                    String feature = IndexToFeature(i);
                    int featureOccurrences = featureHash.getOrDefault(feature, 1);
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
            context.write(key, new Text(output.toString()));
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
/*
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
*/
        FileInputFormat.addInputPath(job, new Path("s3://aseelhamzahw3/outputs/step2 output.txt/"));
        FileOutputFormat.setOutputPath(job, new Path("s3://aseelhamzahw3/outputs/step3 output.txt"));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}