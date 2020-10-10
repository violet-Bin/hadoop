package com.bingo.hadoop.mapreduce.writeablecomparable2;

import com.bingo.hadoop.mapreduce.writeablecomparable.FlowBean;
import com.bingo.hadoop.mapreduce.writeablecomparable.SortMapper;
import com.bingo.hadoop.mapreduce.writeablecomparable.SortReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 14:20 2020/8/29
 * @description:
 */
public class SortDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(SortDriver.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(MyPartitioner2.class);
        job.setNumReduceTasks(5);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\tmp\\4"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\tmp\\tmp7"));

        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}