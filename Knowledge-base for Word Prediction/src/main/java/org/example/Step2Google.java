<<<<<<< HEAD
package org.example;

import java.io.IOException;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import java.util.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public  class Step2Google {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private HashSet<String> stopWords = new HashSet<>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // Initialize the stopWords HashSet with stop words
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




        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String[] splits = value.toString().split("\t");
            if (splits.length < 4) {
                return;
            }
            String n_gram = splits[0];
            String count_s = splits[2];
<<<<<<<< HEAD:Knowledge-base for Word Prediction/src/main/java/org/example/Step2Google.java
            String[] word1_word2_word3 = n_gram.split("\\s+");

            if(word1_word2_word3.length != 3) {
                return; // Ensure ngram has 3 words
            }


========
            String year_s = splits[1];
            String[] word1_word2_word3 = n_gram.split(" ");

>>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad:Knowledge-base for Word Prediction/src/main/java/org/example/Step2.java
            String firstWord =  word1_word2_word3[0];
            String secondWord = word1_word2_word3[1];
            String thirdWord =  word1_word2_word3[2];


            if(stopWords.contains(firstWord) || stopWords.contains(secondWord) || stopWords.contains(thirdWord))
                return  ;

            Text Value = new Text(count_s);


            context.write(new Text("* * " + firstWord ), Value);
            context.write(new Text("* * " + secondWord), Value);
            context.write(new Text("* * " + thirdWord ), Value);
            context.write(new Text("* " + firstWord + " " + secondWord), Value);
            context.write(new Text("* " + secondWord + " " + thirdWord), Value);
            context.write(new Text(firstWord + " " + secondWord + " " + thirdWord), Value);
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
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String thirdWord = TextUtils.getThirdWord(key);
            return (thirdWord.hashCode() & Integer.MAX_VALUE) % numPartitions;

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

            // Compare the third word
            int thirdWordComparison = compareWithStar(TextUtils.getThirdWord(key1), TextUtils.getThirdWord(key2));
            if (thirdWordComparison != 0) {
                return  -thirdWordComparison;
            }

            // Compare the second word
            int secondWordComparison = compareWithStar(TextUtils.getSecondWord(key1), TextUtils.getSecondWord(key2));
            if (secondWordComparison != 0) {
                return -secondWordComparison;
            }

            // Compare the first word
            return -compareWithStar(TextUtils.getFirstWord(key1), TextUtils.getFirstWord(key2));


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




    public class SumCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }



    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private int currentFirstParam = 0;  // <*,*,C>
        private int currentSecondParam = 0; // <*,B,C>


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Initialize temporary variables to store the first and second parameters
            int sum = 0;

            for (Text value : values) {
                sum += Integer.parseInt(value.toString()); // Sum the counts of the word pair
            }
            long starCount = key.toString().chars().filter(c -> c == '*').count();

            // Case : <* * B>
            if (starCount == 2) {
                currentFirstParam = sum;
            }
            // Case : <* B C>
            else if (starCount == 1) {
                currentSecondParam = sum;
            }

            // Case <A,B,C>
            else {                  // <* * B>                 <A B *>
                String resultValue = currentFirstParam + "," + currentSecondParam + "," + sum + ",0,0,0";
                context.write(new Text(key + " $"), new Text(resultValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
<<<<<<<< HEAD:Knowledge-base for Word Prediction/src/main/java/org/example/Step2Google.java
        job.setJarByClass(Step2Google.class);
========
        job.setJarByClass(Step2.class);  // here was error , mit was Step1 , I dont know  how ....
>>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad:Knowledge-base for Word Prediction/src/main/java/org/example/Step2.java

        job.setMapperClass(Step2Google.MapperClass.class);     // mapper
        job.setPartitionerClass(Step2Google.PartitionerClass.class);  // partitioner
        job.setReducerClass(Step2Google.ReducerClass.class);          // reducer
        job.setGroupingComparatorClass(Step2Google.MultiKeyComparator.class);    // comparator
        job.setSortComparatorClass(Step2Google.MultiKeyComparator.class);        //  another comparator

         job.setCombinerClass(SumCombiner.class); // Use reducer as combiner if you need it

        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable
/*
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
*/

   //     job.setInputFormatClass(SequenceFileInputFormat.class);
   //     job.setOutputFormatClass(TextOutputFormat.class);
    //    SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
    //    FileOutputFormat.setOutputPath(job, new Path(args[3]));



        // Define input and output paths
           FileInputFormat.addInputPath(job, new Path(args[1]));
           FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


=======
package org.example;

import java.io.IOException;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import java.util.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public  class Step2Google {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private HashSet<String> stopWords = new HashSet<>();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            // Initialize the stopWords HashSet with stop words
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




        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {


            String[] splits = value.toString().split("\t");
            if (splits.length < 4) {
                return;
            }
            String n_gram = splits[0];
            String count_s = splits[2];
            String[] word1_word2_word3 = n_gram.split("\\s+");

            if(word1_word2_word3.length != 3) {
                return; // Ensure ngram has 3 words
            }


            String firstWord =  word1_word2_word3[0];
            String secondWord = word1_word2_word3[1];
            String thirdWord =  word1_word2_word3[2];


            if(stopWords.contains(firstWord) || stopWords.contains(secondWord) || stopWords.contains(thirdWord))
                return  ;

            Text Value = new Text(count_s);


            context.write(new Text("* * " + firstWord ), Value);
            context.write(new Text("* * " + secondWord), Value);
            context.write(new Text("* * " + thirdWord ), Value);
            context.write(new Text("* " + firstWord + " " + secondWord), Value);
            context.write(new Text("* " + secondWord + " " + thirdWord), Value);
            context.write(new Text(firstWord + " " + secondWord + " " + thirdWord), Value);
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
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String thirdWord = TextUtils.getThirdWord(key);
            return (thirdWord.hashCode() & Integer.MAX_VALUE) % numPartitions;

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

            // Compare the third word
            int thirdWordComparison = compareWithStar(TextUtils.getThirdWord(key1), TextUtils.getThirdWord(key2));
            if (thirdWordComparison != 0) {
                return  -thirdWordComparison;
            }

            // Compare the second word
            int secondWordComparison = compareWithStar(TextUtils.getSecondWord(key1), TextUtils.getSecondWord(key2));
            if (secondWordComparison != 0) {
                return -secondWordComparison;
            }

            // Compare the first word
            return -compareWithStar(TextUtils.getFirstWord(key1), TextUtils.getFirstWord(key2));


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




    public class SumCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (Text value : values) {
                sum += Integer.parseInt(value.toString());
            }
            context.write(key, new Text(String.valueOf(sum)));
        }
    }



    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {

        private int currentFirstParam = 0;  // <*,*,C>
        private int currentSecondParam = 0; // <*,B,C>


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Initialize temporary variables to store the first and second parameters
            int sum = 0;

            for (Text value : values) {
                sum += Integer.parseInt(value.toString()); // Sum the counts of the word pair
            }
            long starCount = key.toString().chars().filter(c -> c == '*').count();

            // Case : <* * B>
            if (starCount == 2) {
                currentFirstParam = sum;
            }
            // Case : <* B C>
            else if (starCount == 1) {
                currentSecondParam = sum;
            }

            // Case <A,B,C>
            else {                  // <* * B>                 <A B *>
                String resultValue = currentFirstParam + "," + currentSecondParam + "," + sum + ",0,0,0";
                context.write(new Text(key + " $"), new Text(resultValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(Step2Google.class);

        job.setMapperClass(Step2Google.MapperClass.class);     // mapper
        job.setPartitionerClass(Step2Google.PartitionerClass.class);  // partitioner
        job.setReducerClass(Step2Google.ReducerClass.class);          // reducer
        job.setGroupingComparatorClass(Step2Google.MultiKeyComparator.class);    // comparator
        job.setSortComparatorClass(Step2Google.MultiKeyComparator.class);        //  another comparator

         job.setCombinerClass(SumCombiner.class); // Use reducer as combiner if you need it

        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));



        // Define input and output paths
        //   FileInputFormat.addInputPath(job, new Path(args[1]));
        //   FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad
}