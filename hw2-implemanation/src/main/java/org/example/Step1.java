package org.example;
import java.io.IOException;
import java.util.StringTokenizer;
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


public class Step1 {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable number = new IntWritable(0);
        private Text word = new Text();


        protected void setup(Context context)
                throws IOException, InterruptedException {
            // Call the base class setup if needed
            super.setup(context);

            // Your custom setup logic
            System.out.println("Custom setup logic for MyMapper!");

            // Example: Fetch a configuration property
            String myConfig = context.getConfiguration().get("my.custom.property");
            System.out.println("Custom Property: " + myConfig);
        }


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the input line by spaces
            String[] tokens = value.toString().split("\\s+");

            // Ensure the line has at least 4 fields (3-gram and match_count)
            if (tokens.length >= 4) {
                // Extract words and match_count
                String firstWord = tokens[0];
                String secondWord = tokens[1];
                String thirdWord = tokens[2];
                int matchCount;

                try {
                    matchCount = Integer.parseInt(tokens[3]); // match_count as an integer
                } catch (NumberFormatException e) {
                    return; // Skip the line if match_count is not valid
                }

                // Write the first pair: <*, second_word, *> -> match_count
                context.write(new Text("*," + secondWord + ",*"), new IntWritable(matchCount));

                // Write the second pair: <first_word, second_word, *> -> match_count
                context.write(new Text(firstWord + "," + secondWord + ",*"), new IntWritable(matchCount));

                // Write the third pair: <first_word, second_word, third_word> -> match_count
                context.write(new Text(firstWord + "," + secondWord + "," + thirdWord), new IntWritable(matchCount));
            }
        }
        // LongWritable key: Represents the offset of the current line in the input file
        // Text : Represents the line of text being processed by the mapper.
        // Context  : Acts as a bridge between the mapper and the rest of the MapReduce framework.

public class NGramCompositeKey implements WritableComparable<NGramCompositeKey> {
    private Text firstWord;
    private Text secondWord;
    private Text thirdWord;
    private int matchCount;

    public NGramCompositeKey() {
        this.firstWord = new Text();
        this.secondWord = new Text();
        this.thirdWord = new Text();
    }



    @Override
    public void write(DataOutput out) throws IOException {
        firstWord.write(out);
        secondWord.write(out);
        thirdWord.write(out);
        out.writeInt(matchCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        firstWord.readFields(in);
        secondWord.readFields(in);
        thirdWord.readFields(in);
        matchCount = in.readInt();
    }

    @Override
    public int compareTo(NGramCompositeKey other) {
        // Compare first by the main key (second, first, third words)
        int cmp = this.secondWord.compareTo(other.secondWord);
        if (cmp != 0) return cmp;

        cmp = this.firstWord.compareTo(other.firstWord);
        if (cmp != 0) return cmp;

        cmp = this.thirdWord.compareTo(other.thirdWord);
        if (cmp != 0) return cmp;

        // Finally, sort by matchCount in ascending order
        return Integer.compare(this.matchCount, other.matchCount);
    }
    public String toString() {
        return firstWord + " " + secondWord + " " + thirdWord + " " + matchCount;
    }


    public Text getFirstWord() { return firstWord; }
    public Text getSecondWord() { return secondWord; }
    public Text getThirdWord() { return thirdWord; }
    public int getMatchCount() { return matchCount; }
}


//Petitioner
public static class NGramPartitioner extends Partitioner<NGramCompositeKey, IntWritable> {
    @Override
    public int getPartition(NGramCompositeKey key, IntWritable value, int numPartitions) {
        // Partition based on the second word
        return (key.getSecondWord().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }


//Comparator

public class NGramGroupingComparator extends WritableComparator {
    protected NGramGroupingComparator() {
        super(NGramCompositeKey.class, true);
    }

    @Override
    public int compare(Object a, Object b) {
        NGramCompositeKey key1 = (NGramCompositeKey) a;
        NGramCompositeKey key2 = (NGramCompositeKey) b;
        return key1.getSecondWord().compareTo(key2.getSecondWord());
    }
}




// Reducer

public static class NGramReducer extends Reducer<Text, IntWritable, Text, Text > {

    // This method is called for each set of keys with their corresponding values
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // Initialize temporary variables to store the first and second parameters
        int currentFirstParam = 0;  // <*, B ,*>
        int currentSecondParam = 0; // <A,B ,*>
        int value;
        int sum = 0;
        List<IntWritable> valueList = new ArrayList<>();


        Iterator<IntWritable> iter = values.iterator();
        value = Integer.parseInt(iter.next().toString());


        long starCount = key.toString().chars().filter(c -> c == '*').count();

        // Case : <*, B ,*>
        if (starCount == 2) {
            currentFirstParam = value;
            currentSecondParam = 0;
        }
        // Case : <A,B ,*>
        if (starCount == 1)
            currentSecondParam = value;


        // Case <A,B,C>
        else {
            String resultValue = "0,0," + value + ",0," + currentFirstParam + "," + currentSecondParam;
            context.write(key, new Text(resultValue));
        }
    }
}









        public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);   //set jar
        job.setMapperClass(WordCount.MapperClass.class);  // set mapper
        job.setPartitionerClass(WordCount.PartitionerClass.class); // set partitioner
        job.setCombinerClass(WordCount.ReducerClass.class);        //  set combiner
        job.setReducerClass(WordCount.ReducerClass.class);         // set reducer
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

}


