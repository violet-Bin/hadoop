package com.bingo.hadoop.mapreduce.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 23:18 2020/8/27
 * @description:
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text phoneNum = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fileds = value.toString().split("\t");
        phoneNum.set(fileds[1]);
        long upFlow = Long.parseLong(fileds[fileds.length - 3]);
        long downFlow = Long.parseLong(fileds[fileds.length - 2]);
        flow.set(upFlow, downFlow);
        context.write(phoneNum, flow);

    }
}
