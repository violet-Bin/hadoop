package com.bingo.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 17:49 2020/9/13
 * @description:
 */
public class OutputDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(OutputDriver.class);

        job.setOutputFormatClass(MyOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\tmp\\7"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\tmp\\tmp10"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
