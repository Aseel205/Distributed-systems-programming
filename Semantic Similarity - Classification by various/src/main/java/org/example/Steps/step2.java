

package org.example.Steps;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import java.util.*;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class step2 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

            private HashSet<String> Lexems = new HashSet<>();
            private Hashtable<String, Integer> FeatureTable = new Hashtable<>(); // Hashtable for feature indices
            private Stemmer stemmer;
            private String featuresFile_path = "s3://aseelhamzahw3/input files/features.txt";
            private String lexemesFile_path = "s3://aseelhamzahw3/input files/lexemes.txt";

            @Override
            protected void setup(Context context) throws IOException, InterruptedException {

                stemmer = new Stemmer() ;

                super.setup(context);

                // Set up S3 client
                AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

                // Extract bucket and key from the featuresFile_path URL
                String featuresBucket = featuresFile_path.split("/")[2];
                String featuresKey = featuresFile_path.substring(featuresFile_path.indexOf("/", 5) + 1);

                // Extract bucket and key from the lexemesFile_path URL
                String lexemesBucket = lexemesFile_path.split("/")[2];
                String lexemesKey = lexemesFile_path.substring(lexemesFile_path.indexOf("/", 5) + 1);

                // Load lexemes from lexemes.txt (from S3)
                S3Object lexemesS3Object = s3Client.getObject(lexemesBucket, lexemesKey);
                BufferedReader lexemeReader = new BufferedReader(new InputStreamReader(lexemesS3Object.getObjectContent()));

                String line;
                while ((line = lexemeReader.readLine()) != null) {
                    Lexems.add(line.trim());
                }
                lexemeReader.close();

                // Load features from features.txt (from S3) and populate FeatureTable
                S3Object featuresS3Object = s3Client.getObject(featuresBucket, featuresKey);
                BufferedReader featureReader = new BufferedReader(new InputStreamReader(featuresS3Object.getObjectContent()));

                List<String> sortedFeatures = new ArrayList<>(); // Temporary list to sort features
                while ((line = featureReader.readLine()) != null) {
                    sortedFeatures.add(line.trim());
                }
                featureReader.close();

                // Sort features alphabetically and assign indices
                int index = 0; // Start indexing from 0
                Collections.sort(sortedFeatures);
                for (String feature : sortedFeatures) {
                    FeatureTable.put(feature, index++);
                }
            }

            // Returns the index of a feature from FeatureTable
            protected int featureToIndex(String feature) {
                return FeatureTable.getOrDefault(feature, -1); // Return -1 if the feature is not found
            }


            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                // Split the input line by tab
                String[] parts = value.toString().split("\t", -1); // Use -1 to preserve all fields, even empty ones
                if (parts.length < 4) return; // Ensure valid format

                String rootWord = parts[0].trim(); // The head_word (root lexeme)
                String syntacticNgram = parts[1].trim(); // The syntactic-ngram field
                int totalCount = Integer.parseInt(parts[2].trim()); // The total_count field

                // Process the syntactic-ngram field
                String[] tokens = syntacticNgram.split(" ");
                String[] words = new String[tokens.length]; // To store words from the ngram
                String[] dependcyLabels = new String[tokens.length] ;
                int  []   headIndexes =  new int[tokens.length] ;

                 for (int i = 0; i < tokens.length; i++) {
                    String[] tokenParts = tokens[i].split("/"); // Extract word from the token
                    if (tokenParts.length < 4) continue; // Ensure valid token format

                    words[i] = tokenParts[0].trim().toLowerCase();
                    dependcyLabels[i] = tokenParts[2].trim();
                    headIndexes [i] =  Integer.parseInt(tokenParts[tokenParts.length-1].trim()) ;

                }

               for (int i = 0 ; i< tokens.length ; i++) {

                   String featureWord = words[i];
                   String dependcyLabel = dependcyLabels[i];
                   int headIndex = headIndexes[i];


                   // Apply the stemmer to the feature word (only once)
                   stemmer.add(featureWord.toCharArray(), featureWord.length());
                   stemmer.stem();
                   String stemmedFeatureWord = stemmer.toString();

                   // Check if the word corresponding to headIndex is a lexeme
                   String wordAtHeadIndex = headIndex > 0 ? words[headIndex - 1] : rootWord;

                   // if the feature point to the lexeme
                   if (Lexems.contains(wordAtHeadIndex) && FeatureTable.containsKey(stemmedFeatureWord + "-" + dependcyLabel)) {
                       int featureIndex = featureToIndex(stemmedFeatureWord + "-" + dependcyLabel); // Fetch index from FeatureTable
                       context.write(new Text(wordAtHeadIndex), new Text(featureIndex + "," + totalCount));
                   }
                   else {
                       stemmer.add(wordAtHeadIndex.toCharArray(), wordAtHeadIndex.length());
                       stemmer.stem();
                       stemmedFeatureWord = stemmer.toString();
                       dependcyLabel = headIndex > 0 ? dependcyLabels[headIndex - 1] :  dependcyLabels[0] ;

                       // if the lexeme point to the feature
                       if (Lexems.contains(featureWord) && FeatureTable.containsKey(stemmedFeatureWord + "-" + dependcyLabel)) {
                           int featureIndex = featureToIndex(stemmedFeatureWord + "-" + dependcyLabel); // Fetch index from FeatureTable
                           context.write(new Text(featureWord), new Text(featureIndex + "," + totalCount));
                       }

                   }
               }


            }
    }




    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {



            long[] featureCounts = new long[1000];

            // Process each value associated with the key
            for (Text value : values) {
                // Split the value into featureIndex and count
                String[] parts = value.toString().split(",");
                if (parts.length == 2) {
                    int featureIndex = Integer.parseInt(parts[0]);
                    long count = Integer.parseInt(parts[1]);

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

        // Add multiple input paths
        for (int i = 1; i < 10; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(args[10]));



        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}


