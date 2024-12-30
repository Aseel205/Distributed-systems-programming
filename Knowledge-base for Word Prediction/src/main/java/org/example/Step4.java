package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
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

public class Step4 {



    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {



        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Convert the input line to string
            String line = value.toString();

            // Split the line by tab character
            String[] parts = line.split("\\$");

            String recipeName = parts[0].trim().replaceAll("\\s+", " "); // Trimming and normalizing spaces

            String numbers = parts[1].trim().replaceAll("\\s+", " "); // Trimming and normalizing spaces


            context.write(new Text(recipeName), new Text(numbers));
        }
    }



        public static class PartitionerClass extends Partitioner<Text, IntWritable> {
            @Override
            public int getPartition(Text key, IntWritable value, int numPartitions) {
                return key.hashCode() % numPartitions;
            }
        }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        public static int C0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String c0Value = context.getConfiguration().get("C0");
            if (c0Value == null) {
                throw new IllegalArgumentException("C0 not provided in the job configuration!");
            }
            C0 = Integer.parseInt(c0Value);
            System.out.println("[INFO] C0 value retrieved in Reducer: " + C0);
        }


            @Override
            public void reduce(Text key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {

                int C1 = 0, C2 = 0, N1 = 0, N2 = 0, N3 = 0;

                for (Text value : values) {
                    String valueStr = value.toString();

                    if (valueStr.charAt(0) == '0') {
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
                }

                String probabilityResult = computeProbability(C0, C1, C2, N1, N2, N3);
                context.write(key, new Text(probabilityResult));
            }

            private String computeProbability(int C0, int C1, int C2, int N1, int N2, int N3) {
                if (C0 == 0 || C1 == 0 || C2 == 0) {
                    throw new IllegalArgumentException("C0, C1, and C2 must be non-zero.");
                }

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

        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        // Extract the number from Step 3 output (e.g., wordsCount)
        String wordsCount = args[3];

        // Remove the square brackets and any surrounding whitespace
 //       wordsCount = wordsCount.replaceAll("[\\[\\]]", "").trim();

        Configuration conf = new Configuration();
        conf.set("C0", wordsCount);  // Set the value of wordsCount as "C0" in the configuration


        Job job = Job.getInstance(conf, "step4 Count");
        job.setJarByClass(Step4.class);

        job.setMapperClass(Step4.MapperClass.class);     // mapper
        job.setPartitionerClass(Step4.PartitionerClass.class);  // partitioner
        job.setReducerClass(Step4.ReducerClass.class);          // reducer

        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable

   //     FileInputFormat.addInputPath(job, new Path("s3://jars123123123/step4_input.txt")) ;
     //   FileOutputFormat.setOutputPath(job,new Path ("s3://jars123123123/step4_output.txt"));



        // Define input and output paths
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
