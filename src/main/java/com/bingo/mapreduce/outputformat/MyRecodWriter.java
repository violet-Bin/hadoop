package com.bingo.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


/**
 * @author: jiangjiabin
 * @date: Create in 17:37 2020/9/13
 * @description:
 */
public class MyRecodWriter extends RecordWriter<LongWritable, Text> {

    private FSDataOutputStream classOne;
    private FSDataOutputStream classTwo;

    /**
     * 初始换方法
     * @param job
     */
    public void initialize(TaskAttemptContext job) throws IOException {
        String outdir = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());
        classOne = fileSystem.create(new Path(outdir + "\\classOne.log"));
        classTwo = fileSystem.create(new Path(outdir + "\\classTwo.log"));
    }

    /**
     * 将KV写出，每对KV调用一次
     *
     * @param key
     * @param value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        String out = value.toString() + "\n";
        if (out.contains("atguigu")) {
            classOne.write(out.getBytes());
        } else {
            classTwo.write(out.getBytes());
        }

    }

    /**
     * 关闭资源
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(classOne);
        IOUtils.closeStream(classTwo);
    }
}
