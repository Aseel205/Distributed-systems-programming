<<<<<<< HEAD
package org.example;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step3Google {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private HashSet<String> stopWords = new HashSet<>();


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




        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splits = value.toString().split("\t");
            if (splits.length < 4) {
                return;
            }
            String word = splits[0];
            String count_s = splits[2];
<<<<<<<< HEAD:Knowledge-base for Word Prediction/src/main/java/org/example/Step3Google.java

            if(word.length()!=1 || stopWords.contains(word))
                return ;

========
            String year_s = splits[1];


            if(stopWords.contains(word))
                return;
>>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad:Knowledge-base for Word Prediction/src/main/java/org/example/Step3.java

            Text matchCount = new Text(count_s);

            // Write the  pair: <*,matchCount>
            context.write(new Text("*"), matchCount);

        }
    }


    // there is no need for  PartitionerClass
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
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
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, " Step3");
        job.setJarByClass(Step3Google.class);

        job.setMapperClass(Step3Google.MapperClass.class);     // mapper
        job.setPartitionerClass(Step3Google.PartitionerClass.class);  // partitioner
        job.setReducerClass(Step3Google.ReducerClass.class);          // reducer

        job.setCombinerClass(Step3Google.ReducerClass.class); // Use reducer as combiner if you need it

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

<<<<<<<< HEAD:Knowledge-base for Word Prediction/src/main/java/org/example/Step3Google.java
          FileInputFormat.addInputPath(job, new Path(args[1]));
          FileOutputFormat.setOutputPath(job, new Path(args[3]));
========
   //     job.setInputFormatClass(SequenceFileInputFormat.class);
   //     job.setOutputFormatClass(TextOutputFormat.class);
   //     SequenceFileInputFormat.addInputPath(job, new Path(args[1]));
   //     FileOutputFormat.setOutputPath(job, new Path(args[3]));


        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
>>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad:Knowledge-base for Word Prediction/src/main/java/org/example/Step3.java

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
=======
package org.example;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step3Google {


    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {

        private HashSet<String> stopWords = new HashSet<>();


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




        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] splits = value.toString().split("\t");
            if (splits.length < 4) {
                return;
            }
            String word = splits[0];
            String count_s = splits[2];

            if(word.length()!=1 || stopWords.contains(word))
                return ;


            Text matchCount = new Text(count_s);

            // Write the  pair: <*,matchCount>
            context.write(new Text("*"), matchCount);

        }
    }


    // there is no need for  PartitionerClass
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return key.hashCode() % numPartitions;
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
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, " Step3");
        job.setJarByClass(Step3Google.class);

        job.setMapperClass(Step3Google.MapperClass.class);     // mapper
        job.setPartitionerClass(Step3Google.PartitionerClass.class);  // partitioner
        job.setReducerClass(Step3Google.ReducerClass.class);          // reducer

    //    job.setCombinerClass(Step3Google.ReducerClass.class); // Use reducer as combiner if you need it

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


//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        // Wait for the job to complete
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
>>>>>>> 87718d7be38280cd76a57829026e5c1bdc14ccad
}