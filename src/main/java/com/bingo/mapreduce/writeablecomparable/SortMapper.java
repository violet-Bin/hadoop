package com.bingo.mapreduce.writeablecomparable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 14:20 2020/8/29
 * @description:
 */
public class SortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private FlowBean flowBean = new FlowBean();
    private Text phoneNam = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        phoneNam.set(fields[0]);
        flowBean.setUpFlow(Long.parseLong(fields[1]));
        flowBean.setDownFlow(Long.parseLong(fields[2]));
        flowBean.setSumFlow(Long.parseLong(fields[3]));

        context.write(flowBean, phoneNam);
    }
}
