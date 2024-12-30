package org.example;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.HashSet;


public class Step1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private HashSet<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // Initialize the stopWords HashSet
            String[] stopWordsArray = {
                    "״", "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי",
                    "מהם", "מה", "מ", "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה",
                    "כלי", "כל", "כי", "יש", "ימים", "יותר", "יד", "י", "זה", "ז", "ועל", "ומי",
                    "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה", "היו", "היה",
                    "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את",
                    "אשר", "אם", "אלה", "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז",
                    "אותו", "־", "^", "?", ";", ":", "1", ".", "-", "*", "\"", "!", "שלשה", "בעל",
                    "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו", "להם", "ישראל",
                    "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי",
                    "במקום", "בהם", "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק", "שני",
                    "שכל", "שאר", "ש", "ר", "פעמים", "נעשה", "ן", "ממנו", "מלא", "מזה", "ם",
                    "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין", "ואין", "הן",
                    "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או", "אבל",
                    "א"
            };

            // Add stop words to the HashSet
            for (String word : stopWordsArray) {
                stopWords.add(word);
            }
        }


        public static String[] extractValues(String input) {
            // Split the input string by space
            String[] tokens = input.split("\\s+");

            // Extract the relevant parts
            String word1 =  tokens[0];  // "הכלב"
            String word2 =  tokens[1];  // "רץ"
            String word3 =  tokens[2];  // "מהר"
            String year =   tokens[3];   // "2024"
            String count1 = tokens[4]; // "50"
            String count2 = tokens[5]; // "10"

            // Return the values in the desired format
            return new String[] { word1, word2, word3, year, count1, count2 };
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



            // Parse the first, second, and third words from the ngram
            String[] words = extractValues(value.toString());

                String firstWord =  words[0];
                String secondWord = words[1];
                String thirdWord =  words[2];


                if(stopWords.contains(firstWord) || stopWords.contains(secondWord) || stopWords.contains(thirdWord))
                    return  ;

                Text Value = new Text(words[4]);

                // Output in the same format as before, which is just ngram and match count
                context.write(new Text("* " + firstWord +  " *" + " $"), Value);
                context.write(new Text("* "+  secondWord +" *" + " $"), Value);
                context.write(new Text("* " + thirdWord + " *" + " $"), Value);
                context.write(new Text(firstWord +  " "+ secondWord + " " + "*" + " $"), Value);
                context.write(new Text(secondWord + " " + thirdWord+ " " +  "*" + " $"), Value);
                context.write(new Text(firstWord + " "+  secondWord + " "+  thirdWord + " $"), Value);
            }
        }


    public static class TextUtils {
        public static String getFirstWord(Text key) {
            String[] parts = key.toString().split(" ");
            if (parts.length >= 1) {
                return parts[0]; // Return the first word
            }
            return ""; // Default if first word doesn't exist
        }

        public static String getSecondWord(Text key) {
            String[] parts = key.toString().split(" ");
            if (parts.length >= 2) {
                return parts[1]; // Return the second word
            }
            return ""; // Default if second word doesn't exist
        }

        public static String getThirdWord(Text key) {
            String[] parts = key.toString().split(" ");
            if (parts.length >= 3) {
                return parts[2]; // Return the third word
            }
            return ""; // Default if third word doesn't exist
        }

    }



    //Partition
    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String secondWord = TextUtils.getSecondWord(key);
            return (secondWord.hashCode() & Integer.MAX_VALUE) % numPartitions;

        }
    }


    //Comparator
    public static class MultiKeyComparator extends WritableComparator {
        protected MultiKeyComparator() {
            super(Text.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text key1 = (Text) a;
            Text key2 = (Text) b;

            // Compare the second word in reverse order
            int secondWordComparison = compareWithStar(TextUtils.getSecondWord(key1), TextUtils.getSecondWord(key2));
            if (secondWordComparison != 0) {
                return -secondWordComparison; // Reverse the comparison
            }

            // Compare the first word in reverse order
            int firstWordComparison = compareWithStar(TextUtils.getFirstWord(key1), TextUtils.getFirstWord(key2));
            if (firstWordComparison != 0) {
                return -firstWordComparison; // Reverse the comparison
            }

            // Compare the third word in reverse order
            return -compareWithStar(TextUtils.getThirdWord(key1), TextUtils.getThirdWord(key2)); // Reverse the comparison
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


    // Reducer


    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {

        private int currentFirstParam = 0;   //
        private  int currentSecondParam = 0 ;   //



        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Initialize temporary variables to store the first and second parameters
            int sum = 0;

            Iterator<Text> iter = values.iterator();

            while(iter.hasNext())
                sum=sum+Integer.parseInt(iter.next().toString());


            long starCount = key.toString().chars().filter(c -> c == '*').count();

            // Case : <*, B ,*>
            if (starCount == 2) {
                currentFirstParam = sum;
            }

            // Case : <A,B ,*>
            else if (starCount == 1) {
                currentSecondParam = sum;
            }

            // Case <A,B,C>
            else {
                String resultValue = "0,0," + sum + ",0," + currentFirstParam + "," + currentSecondParam;
                context.write(key, new Text( resultValue));
            }
        }
    }

/*
    public static class ReducerClass2 extends Reducer<Text,Text,Text,Text> {

        private int currentFirstParam ;    //
        private  int currentSecondParam ;  //



        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Initialize temporary variables to store the first and second parameters
            int value = 0;
            int sum = 0;

            Iterator<Text> iter = values.iterator();

            while(iter.hasNext())
                sum=sum+Integer.parseInt(iter.next().toString());


                context.write(key, new Text( sum+""));

        }
    }

    
 */


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(Step1.class);

        job.setMapperClass(Step1.MapperClass.class);     // mapper
        job.setPartitionerClass(Step1.PartitionerClass.class);  // partitioner
        job.setReducerClass(Step1.ReducerClass.class);          // reducer
        job.setGroupingComparatorClass(Step1.MultiKeyComparator.class);    // comparator
        job.setSortComparatorClass(Step1.MultiKeyComparator.class);        //  another comparator

        // job.setCombinerClass(Step1.ReducerClass.class); // Use reducer as combiner if you need it

        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable


        // Define input and output paths
        FileInputFormat.addInputPath(job, new Path("s3://jars123123123/step1_input.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://jars123123123/step1_output.txt"));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}



