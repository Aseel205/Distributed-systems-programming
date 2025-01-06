<<<<<<< HEAD
package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class Step4Google {



    public static class MapperClass extends Mapper<LongWritable , Text, Text, Text> {



        public void map(LongWritable  key, Text value, Context context) throws IOException, InterruptedException {
            // Convert the input line to string
            String line = value.toString();

            // Split the line by tab character
            String[] parts = line.split("\\$");

            String recipeName = parts[0].trim().replaceAll("\\s+", " "); // Trimming and normalizing spaces

            String numbers = parts[1].trim().replaceAll("\\s+", " "); // Trimming and normalizing spaces


            context.write(new Text(recipeName), new Text(numbers));
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

    public static class MultiKeyComparator extends WritableComparator {
        protected MultiKeyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text key1 = (Text) a;
            Text key2 = (Text) b;

            // Compare the second word in reverse order
            int secondWordComparison = compareWithStar(Step1Google.TextUtils.getSecondWord(key1), Step1Google.TextUtils.getSecondWord(key2));
            if (secondWordComparison != 0) {
                return -secondWordComparison; // Reverse the comparison
            }

            // Compare the first word in reverse order
            int firstWordComparison = compareWithStar(Step1Google.TextUtils.getFirstWord(key1), Step1Google.TextUtils.getFirstWord(key2));
            if (firstWordComparison != 0) {
                return -firstWordComparison; // Reverse the comparison
            }

            // Compare the third word in reverse order
            return -compareWithStar(Step1Google.TextUtils.getThirdWord(key1), Step1Google.TextUtils.getThirdWord(key2)); // Reverse the comparison
        }

        // Helper method to compare with "*" treated as the largest
        private int compareWithStar(String word1, String word2) {
            if (word1.equals("*") && !word2.equals("*")) {
                return 1; // "*" is greater than any other word
            } else if (!word1.equals("*") && word2.equals("*")) {
                return -1; // "*" is greater than any other word
            } else {
                return word1.compareTo(word2); // Regular comparison for other words
            }
        }
    }






    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        public  static AmazonS3 S3;
        public  static   int C0;

        @Override
        protected void  setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // Use the default AWS credentials provider (IAM role) for EMR
            S3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(DefaultAWSCredentialsProviderChain.getInstance()) // Default credentials provider
                    .withRegion("us-east-1") // Ensure this matches your bucket region
                    .build();

            // Extract the C0 value from Step 3 output stored in S3
            String bucketName = "jars123123123"; // Replace with your actual bucket name
            String c0Value = extractNumberFromStep3(bucketName, "step3_output"); // Replace with the actual file path

            if (c0Value == null) {
                throw new IllegalArgumentException("C0 not provided in the Step 3 output!");
            }

            C0 = Integer.parseInt(c0Value);
            System.out.println("[INFO] C0 value retrieved in Reducer: " + C0);
        }

        // Method to extract number from Step 3 output stored in S3
        public  String extractNumberFromStep3(String bucketName, String outputDir) {
            try {
                // List all objects in the output directory
                ObjectListing objectListing = S3.listObjects(bucketName, outputDir);
                for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                    // Skip directories
                    if (summary.getKey().endsWith("/")) {
                        continue;
                    }

                    // Fetch each file
                    S3Object object = S3.getObject(bucketName, summary.getKey());
                    S3ObjectInputStream inputStream = object.getObjectContent();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

                    String line;
                    // Read the file to find a non-empty line with a valid number
                    while ((line = reader.readLine()) != null) {
                        if (!line.isEmpty()) {
                            String[] parts = line.split("\\s+");
                            if (parts.length == 2) {
                                String number = parts[1];
                                System.out.println("[INFO] Extracted number: " + number);
                                return number; // Return the number from the first valid file
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("[ERROR] Failed to parse Step 3 output: " + e.getMessage());
            }
            return null; // Return null if no valid number is found
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int C1 = 0, C2 = 0, N1 = 0, N2 = 0, N3 = 0;
            int counter = 0 ;
            Iterator<Text> iter = values.iterator() ;

            while (iter.hasNext()) {
                String valueStr = (iter.next()).toString();

                // in this case the combiner was activated
                if (counter==0 && ! iter.hasNext()) {
                    context.write(key, new Text(valueStr));
                    break ;
                }
                // regular reduce
               else if (valueStr.charAt(0) == '0') {
                    String[] tokens = valueStr.split(",");
                    if (tokens.length == 6) {
                        C1 = Integer.parseInt(tokens[4]);
                        C2 = Integer.parseInt(tokens[5]);
                    }
                } else {
                    String[] tokens = valueStr.split(",");
                    if (tokens.length == 6) {
                        N1 = Integer.parseInt(tokens[0]);
                        N2 = Integer.parseInt(tokens[1]);
                        N3 = Integer.parseInt(tokens[2]);
                    }
                }
                counter ++ ;
            }


            String probabilityResult = computeProbability(C0, C1, C2, N1, N2, N3);
            context.write(key, new Text(probabilityResult));
        }

<<<<<<<< HEAD:Knowledge-base for Word Prediction/src/main/java/org/example/Step4Google.java
        private String computeProbability(int C0, int C1, int C2, int N1, int N2, int N3) {

            if(C0 == 0 )
                throw new IllegalArgumentException("C0 must be non-zero.");


            if (C1 == 0)
                throw new IllegalArgumentException("C1 must be non-zero.");


            if (C2 == 0)
                throw new IllegalArgumentException("C2 must be non-zero.");


            double k2 = (double) (Math.log(N2 + 1) + 1) / (double) (Math.log(N2 + 1) + 2);
            double k3 = (double) (Math.log(N3 + 1) + 1) / (double) (Math.log(N3 + 1) + 2);

            double term1 = k3 * (float) N3 / C2;
            double term2 = (1 - k3) * k2 * (float) N2 / C1;
            double term3 = (1 - k3) * (1 - k2) * (float) N1 / C0;

            double probability = term1 + term2 + term3;

            return String.valueOf(probability);
        }
    }

========
>>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad:Knowledge-base for Word Prediction/src/main/java/org/example/Step4.java
    public static void main(String[] args) throws Exception {
        System.out.println(" we are in the new Step4");
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");


        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "step4 Count");
        job.setJarByClass(Step4Google.class);

        job.setMapperClass(Step4Google.MapperClass.class);     // mapper
        job.setPartitionerClass(Step4Google.PartitionerClass.class);  // partitioner
        job.setReducerClass(Step4Google.ReducerClass.class);          // reducer
      //  job.setCombinerClass(ReducerClass.class); // Use reducer as combiner if you need it


        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable


        // Define input and output paths
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
=======
package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

public class Step4Google {



    public static class MapperClass extends Mapper<LongWritable , Text, Text, Text> {



        public void map(LongWritable  key, Text value, Context context) throws IOException, InterruptedException {
            // Convert the input line to string
            String line = value.toString();

            // Split the line by tab character
            String[] parts = line.split("\\$");

            String recipeName = parts[0].trim().replaceAll("\\s+", " "); // Trimming and normalizing spaces

            String numbers = parts[1].trim().replaceAll("\\s+", " "); // Trimming and normalizing spaces


            context.write(new Text(recipeName), new Text(numbers));
        }
    }


    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode() % numPartitions);
        }
    }


    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        public  static AmazonS3 S3;
        public  static   int C0;

        @Override
        protected void  setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // Use the default AWS credentials provider (IAM role) for EMR
            S3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(DefaultAWSCredentialsProviderChain.getInstance()) // Default credentials provider
                    .withRegion("us-east-1") // Ensure this matches your bucket region
                    .build();

            // Extract the C0 value from Step 3 output stored in S3
            String bucketName = "jars123123123"; // Replace with your actual bucket name
            String c0Value = extractNumberFromStep3(bucketName, "step3_output"); // Replace with the actual file path

            if (c0Value == null) {
                throw new IllegalArgumentException("C0 not provided in the Step 3 output!");
            }

            C0 = Integer.parseInt(c0Value);
            System.out.println("[INFO] C0 value retrieved in Reducer: " + C0);
        }

        // Method to extract number from Step 3 output stored in S3
        public  String extractNumberFromStep3(String bucketName, String outputDir) {
            try {
                // List all objects in the output directory
                ObjectListing objectListing = S3.listObjects(bucketName, outputDir);
                for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                    // Skip directories
                    if (summary.getKey().endsWith("/")) {
                        continue;
                    }

                    // Fetch each file
                    S3Object object = S3.getObject(bucketName, summary.getKey());
                    S3ObjectInputStream inputStream = object.getObjectContent();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

                    String line;
                    // Read the file to find a non-empty line with a valid number
                    while ((line = reader.readLine()) != null) {
                        if (!line.isEmpty()) {
                            String[] parts = line.split("\\s+");
                            if (parts.length == 2) {
                                String number = parts[1];
                                System.out.println("[INFO] Extracted number: " + number);
                                return number; // Return the number from the first valid file
                            }
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("[ERROR] Failed to parse Step 3 output: " + e.getMessage());
            }
            return null; // Return null if no valid number is found
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int C1 = 0, C2 = 0, N1 = 0, N2 = 0, N3 = 0;
            int counter = 0 ;
            Iterator<Text> iter = values.iterator() ;

            while (iter.hasNext()) {
                String valueStr = (iter.next()).toString();

                // in this case the combiner was activated
                if (counter==0 && ! iter.hasNext()) {
                    context.write(key, new Text(valueStr));
                    break ;
                }
                // regular reduce
               else if (valueStr.charAt(0) == '0') {
                    String[] tokens = valueStr.split(",");
                    if (tokens.length == 6) {
                        C1 = Integer.parseInt(tokens[4]);
                        C2 = Integer.parseInt(tokens[5]);
                    }
                } else {
                    String[] tokens = valueStr.split(",");
                    if (tokens.length == 6) {
                        N1 = Integer.parseInt(tokens[0]);
                        N2 = Integer.parseInt(tokens[1]);
                        N3 = Integer.parseInt(tokens[2]);
                    }
                }
                counter ++ ;
            }


            String probabilityResult = computeProbability(C0, C1, C2, N1, N2, N3);
            context.write(key, new Text(probabilityResult));
        }

        private String computeProbability(int C0, int C1, int C2, int N1, int N2, int N3) {

            if(C0 == 0 )
                throw new IllegalArgumentException("C0 must be non-zero.");


            if (C1 == 0)
                throw new IllegalArgumentException("C1 must be non-zero.");


            if (C2 == 0)
                throw new IllegalArgumentException("C2 must be non-zero.");


            float k2 = (float) (Math.log(N2 + 1) + 1) / (float) (Math.log(N2 + 1) + 2);
            float k3 = (float) (Math.log(N3 + 1) + 1) / (float) (Math.log(N3 + 1) + 2);

            float term1 = k3 * (float) N3 / C2;
            float term2 = (1 - k3) * k2 * (float) N2 / C1;
            float term3 = (1 - k3) * (1 - k2) * (float) N1 / C0;

            float probability = term1 + term2 + term3;

            return String.valueOf(probability);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(" we are in the new Step4");
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");


        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "step4 Count");
        job.setJarByClass(Step4Google.class);

        job.setMapperClass(Step4Google.MapperClass.class);     // mapper
        job.setPartitionerClass(Step4Google.PartitionerClass.class);  // partitioner
        job.setReducerClass(Step4Google.ReducerClass.class);          // reducer
      //  job.setCombinerClass(ReducerClass.class); // Use reducer as combiner if you need it


        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable


        // Define input and output paths
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad
