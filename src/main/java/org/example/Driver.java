package org.example;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

public class Driver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Path wc_input = new Path(args[0]);
        Path wc_output = new Path(args[1]);
        Path top5_output = new Path(args[1]);

        JobConf wc_conf = new JobConf(Driver.class);
        wc_conf.setJobName("WordCount");
        wc_conf.setOutputKeyClass(Text.class);
        wc_conf.setOutputValueClass(IntWritable.class);
        wc_conf.setMapperClass(WC_Mapper.class);
        wc_conf.setCombinerClass(WC_Reducer.class);
        wc_conf.setReducerClass(WC_Reducer.class);
        wc_conf.setInputFormat(TextInputFormat.class);
        wc_conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(wc_conf,wc_input);
        FileOutputFormat.setOutputPath(wc_conf,wc_output);

        Job wc_job = new Job(wc_conf);
        wc_job.waitForCompletion(true);

        JobConf top5_conf = new JobConf(Driver.class);
        top5_conf.setJobName("Top5");
        top5_conf.setMapOutputKeyClass(Text.class);
        top5_conf.setMapOutputValueClass(IntWritable.class);
        top5_conf.setOutputKeyClass(IntWritable.class);
        top5_conf.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(top5_conf,wc_output);
        FileOutputFormat.setOutputPath(top5_conf,top5_output);

        Job top5_job = new Job(top5_conf);
        top5_job.setMapperClass(Top5_Mapper.class);
        top5_job.setReducerClass(Top5_Reducer.class);
        top5_job.waitForCompletion(true);
    }
}