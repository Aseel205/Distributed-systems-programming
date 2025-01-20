package org.example;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import java.util.HashSet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class step1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private final HashSet<String> Lexems = new HashSet<>();
        private final HashSet<String> Features = new HashSet<>();
        private Stemmer stemmer;
        private String featuresFile_path = "s3://aseelhamzahw3/input files/features.txt";
        private String lexemesFile_path = "s3://aseelhamzahw3/input files/lexemes.txt";

        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // Set up S3 client
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();

            // Parse the S3 paths to get bucket names and keys
            String featuresBucket = featuresFile_path.split("/")[2];
            String featuresKey = featuresFile_path.substring(featuresFile_path.indexOf("/", 5) + 1);

            String lexemesBucket = lexemesFile_path.split("/")[2];
            String lexemesKey = lexemesFile_path.substring(lexemesFile_path.indexOf("/", 5) + 1);

            // Read lexemes file from S3
            S3Object lexemesS3Object = s3Client.getObject(lexemesBucket, lexemesKey);
            try (BufferedReader lexemesReader = new BufferedReader(new InputStreamReader(lexemesS3Object.getObjectContent()))) {
                String line;
                while ((line = lexemesReader.readLine()) != null) {
                    Lexems.add(line.trim());
                }
            }

            // Read features file from S3
            S3Object featuresS3Object = s3Client.getObject(featuresBucket, featuresKey);
            try (BufferedReader featuresReader = new BufferedReader(new InputStreamReader(featuresS3Object.getObjectContent()))) {
                String line;
                while ((line = featuresReader.readLine()) != null) {
                    Features.add(line.trim());
                }
            }

        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // Split the input line by tab
            String[] parts = value.toString().split("\t", -1); // Use -1 to preserve all fields, even empty ones
            if (parts.length < 4) { // Ensure all fields exist
                return;
            }

            String rootWord = parts[0].trim();         // The head_word (root)
            String syntacticNgram = parts[1].trim();  // The syntactic-ngram field
            String totalCountField = parts[2].trim(); // The total_count field

                // Parse the total_count as an integer
            int totalCount = Integer.parseInt(totalCountField);


            // Process the syntactic n-gram to extract feature words as before...
            String[] tokens = syntacticNgram.split(" ");
            for (String token : tokens) {
                String[] tokenParts = token.split("/"); // Extract word, POS tag, dep label, and head index from the token
                if (tokenParts.length < 4)
                    continue; // Ensure correct token format (word/pos-tag/dep-label/head-index)

                String featureWord = tokenParts[0]; // The actual feature word
                String depLabel = tokenParts[2]; // Dependency label

                // Apply the stemmer to the feature word
                stemmer = new Stemmer();
                stemmer.add(featureWord.toCharArray(), featureWord.length()); // Add each character to the stemmer
                stemmer.stem(); // Stem the word after all characters are added
                String stemmedFeatureWord = stemmer.toString(); // Retrieve the stemmed word



                // we count the lexeme as lexeme and the feature as feature
                if (Lexems.contains(stemmedFeatureWord)) {
                    context.write(new Text("L"), new Text(String.valueOf(totalCount))); // Count all lexemes
                    context.write(new Text("$ " + rootWord), new Text(String.valueOf(totalCount))); // Count specific lexeme
                }


                if (Features.contains(stemmedFeatureWord + "-" + depLabel)) {
                    String featureKey = stemmedFeatureWord + "-" + depLabel; // Format key as <word relationship>
                    context.write(new Text("F"), new Text(String.valueOf(totalCount))); // Count all features
                    context.write(new Text("@ " + featureKey), new Text(String.valueOf(totalCount))); // Count specific feature
                }
            }
        }



    }

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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

      //  job.setInputFormatClass(SequenceFileInputFormat.class);
      //  job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("s3://aseelhamzahw3/input files/small input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://aseelhamzahw3/outputs/step1 output.txt"));



        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}