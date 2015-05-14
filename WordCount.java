import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


public class WordCount extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        // create 
        JobConf job_conf = new JobConf(getConf(), WordCount.class);
        job_conf.setJobName("WordCount");

        // set output key value data type
        job_conf.setOutputKeyClass(Text.class);
        job_conf.setOutputValueClass(IntWritable.class);

        // set mapper and reducer
        job_conf.setMapperClass(WordCountMapper.class);
        job_conf.setReducerClass(WordCountReducer.class);

        // in run time, we will supply the input path and output path
        Path path_input = new Path(args[0]);
        Path path_output = new Path(args[1]);

        // set HDFS input output directory
        FileInputFormat.addInputPath(job_conf, path_input);
        FileOutputFormat.setOutputPath(job_conf, path_output);

        JobClient.runJob(job_conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(result);
    }
}