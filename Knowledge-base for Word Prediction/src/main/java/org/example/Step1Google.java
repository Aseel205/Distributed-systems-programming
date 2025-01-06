<<<<<<< HEAD
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.HashSet;


public class Step1Google {

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


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

<<<<<<<< HEAD:Knowledge-base for Word Prediction/src/main/java/org/example/Step1Google.java
            String line = value.toString();
            String[] splits = line.split("\t");
========

            String[] splits = value.toString().split("\t");
>>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad:Knowledge-base for Word Prediction/src/main/java/org/example/Step1.java
            if (splits.length < 4) {
                return;
            }
            String n_gram = splits[0];
            String count_s = splits[2];
<<<<<<<< HEAD:Knowledge-base for Word Prediction/src/main/java/org/example/Step1Google.java
            String[] word1_word2_word3 = n_gram.split("\\s+");

            if(word1_word2_word3.length!=3) {
                return;
            }
========
            String year_s = splits[1];
            String[] word1_word2_word3 = n_gram.split(" ");


>>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad:Knowledge-base for Word Prediction/src/main/java/org/example/Step1.java


            String firstWord =  word1_word2_word3[0];
            String secondWord = word1_word2_word3[1];
            String thirdWord =  word1_word2_word3[2];


<<<<<<<< HEAD:Knowledge-base for Word Prediction/src/main/java/org/example/Step1Google.java
            if(stopWords.contains(firstWord) || stopWords.contains(secondWord) || stopWords.contains(thirdWord)) {
                return;
            }
========
            if(stopWords.contains(firstWord) || stopWords.contains(secondWord) || stopWords.contains(thirdWord))
                return  ;
>>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad:Knowledge-base for Word Prediction/src/main/java/org/example/Step1.java

            Text Value = new Text(count_s);

            // Output in the same format as before, which is just ngram and match count
            context.write(new Text("* " + firstWord +  " *" ), Value);
            context.write(new Text("* "+  secondWord +" *" ), Value);
            context.write(new Text("* " + thirdWord + " *" ), Value);
            context.write(new Text(firstWord +  " "+ secondWord + " " + "*" ), Value);
            context.write(new Text(secondWord + " " + thirdWord+ " " +  "*" ), Value);
            context.write(new Text(firstWord + " "+  secondWord + " "+  thirdWord ), Value);
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
                context.write(new Text(key + " $"), new Text( resultValue));
            }
        }
    }



    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(Step1Google.class);

        job.setMapperClass(Step1Google.MapperClass.class);     // mapper
        job.setPartitionerClass(Step1Google.PartitionerClass.class);  // partitioner
        job.setReducerClass(Step1Google.ReducerClass.class);          // reducer
        job.setGroupingComparatorClass(Step1Google.MultiKeyComparator.class);    // comparator
        job.setSortComparatorClass(Step1Google.MultiKeyComparator.class);        //  another comparator

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
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
*/


   //     job.setInputFormatClass(SequenceFileInputFormat.class);
   //     job.setOutputFormatClass(TextOutputFormat.class);     // I think this line will cause erros
  //      SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
  //      FileOutputFormat.setOutputPath(job, new Path(args[3]));




        // Define input and output paths
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


<<<<<<<< HEAD:Knowledge-base for Word Prediction/src/main/java/org/example/Step1Google.java
}
========
}
>>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad:Knowledge-base for Word Prediction/src/main/java/org/example/Step1.java
=======
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.HashSet;


public class Step1Google {

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


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String[] splits = line.split("\t");
            if (splits.length < 4) {
                return;
            }
            String n_gram = splits[0];
            String count_s = splits[2];
            String[] word1_word2_word3 = n_gram.split("\\s+");

            if(word1_word2_word3.length!=3) {
                return;
            }


            String firstWord =  word1_word2_word3[0];
            String secondWord = word1_word2_word3[1];
            String thirdWord =  word1_word2_word3[2];


            if(stopWords.contains(firstWord) || stopWords.contains(secondWord) || stopWords.contains(thirdWord)) {
                return;
            }

            Text Value = new Text(count_s);

            // Output in the same format as before, which is just ngram and match count
            context.write(new Text("* " + firstWord +  " *" ), Value);
            context.write(new Text("* "+  secondWord +" *" ), Value);
            context.write(new Text("* " + thirdWord + " *" ), Value);
            context.write(new Text(firstWord +  " "+ secondWord + " " + "*" ), Value);
            context.write(new Text(secondWord + " " + thirdWord+ " " +  "*" ), Value);
            context.write(new Text(firstWord + " "+  secondWord + " "+  thirdWord ), Value);
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
                context.write(new Text(key + " $"), new Text( resultValue));
            }
        }
    }



    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Count");
        job.setJarByClass(Step1Google.class);

        job.setMapperClass(Step1Google.MapperClass.class);     // mapper
        job.setPartitionerClass(Step1Google.PartitionerClass.class);  // partitioner
        job.setReducerClass(Step1Google.ReducerClass.class);          // reducer
        job.setGroupingComparatorClass(Step1Google.MultiKeyComparator.class);    // comparator
        job.setSortComparatorClass(Step1Google.MultiKeyComparator.class);        //  another comparator

        // job.setCombinerClass(SumCombiner.class); // Use reducer as combiner if you need it

        // Set output key/value types for the Mapper output
        job.setMapOutputKeyClass(Text.class);  // Mapper outputs NGramCompositeKey
        job.setMapOutputValueClass(Text.class);

        // Set output key/value types for the final output (Reducer output)
        job.setOutputKeyClass(Text.class);  // Final output key is Text
        job.setOutputValueClass(Text.class);  // Final output value is IntWritable

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));




        // Define input and output paths
    //    FileInputFormat.addInputPath(job, new Path(args[1]));
    //    FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad
