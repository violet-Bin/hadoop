package com.bingo.reducejoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 20:36 2020/8/29
 * @description:
 */
public class RJMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private static final  String ORDER_FILE_NAME = "order.txt";

    private OrderBean orderBean = new OrderBean();
    private String fileName;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        fileName = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fieds = value.toString().split("\t");
        if (StringUtils.equals(fileName, ORDER_FILE_NAME)) {
            orderBean.setId(fieds[0]);
            orderBean.setPid(fieds[1]);
            orderBean.setAmount(Integer.parseInt(fieds[2]));
            orderBean.setPname("");
        } else {
            orderBean.setPid(fieds[0]);
            orderBean.setPname(fieds[1]);
            orderBean.setId("");
            orderBean.setAmount(0);
        }
        context.write(orderBean, NullWritable.get());
    }
}
