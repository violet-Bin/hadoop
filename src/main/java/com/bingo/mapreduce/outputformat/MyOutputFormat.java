package com.bingo.mapreduce.outputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 17:35 2020/9/13
 * @description:
 */
public class MyOutputFormat extends FileOutputFormat<LongWritable, Text> {

    @Override
    public RecordWriter<LongWritable, Text> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        MyRecodWriter myRecodWriter = new MyRecodWriter();
        myRecodWriter.initialize(job);
        return myRecodWriter;
    }
}
