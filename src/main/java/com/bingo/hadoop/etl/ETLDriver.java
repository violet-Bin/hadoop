package com.bingo.hadoop.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 23:20 2020/9/14
 * @description:
 */
public class ETLDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(ETLDriver.class);
        job.setMapperClass(ETLMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\tmp\\8"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\tmp\\tmp12"));
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
