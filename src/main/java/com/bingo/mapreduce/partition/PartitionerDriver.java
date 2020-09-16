package com.bingo.mapreduce.partition;

import com.bingo.mapreduce.flow.FlowBean;
import com.bingo.mapreduce.flow.FlowMapper;
import com.bingo.mapreduce.flow.FlowReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 14:07 2020/8/29
 * @description:
 */
public class PartitionerDriver {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
//1.获取Job实例
        Job job = Job.getInstance(new Configuration());

        //2.设置类路径
        job.setJarByClass(PartitionerDriver.class);

        //3.设置Mapper和Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setNumReduceTasks(5);
        job.setPartitionerClass(MyPartitioner.class);

        //4. 设置输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //5. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path("E:\\tmp\\2"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\tmp\\tmp5"));

        //6. 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
