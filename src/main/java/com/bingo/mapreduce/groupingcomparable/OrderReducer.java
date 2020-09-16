package com.bingo.mapreduce.groupingcomparable;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author: jiangjiabin
 * @date: Create in 16:14 2020/8/29
 * @description:
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //取最大的那一个
//        context.write(key, NullWritable.get());

        //取前两个
//        Iterator<NullWritable> iterator = values.iterator();
//        for (int i = 0; i < 2; i++) {
//            if (iterator.hasNext()) {
//                context.write(key, iterator.next());
//            }
//        }
        Iterator<NullWritable> iterator = values.iterator();
        if (iterator.hasNext()) {
            context.write(key, iterator.next());
        }

    }
}
