import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class WordCountMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    // hadoop data type
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        // take each from input file and tokenize it
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);

        // iterate through each word
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            // sending to output and passes to reducer
            output.collect(word, one);
        }
    }
}