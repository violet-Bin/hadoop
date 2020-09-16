package com.bingo.mapreduce.partition;

import com.bingo.mapreduce.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author: jiangjiabin
 * @date: Create in 14:04 2020/8/29
 * @description:
 */
public class MyPartitioner extends Partitioner<Text, FlowBean> {
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String phoneNam = text.toString();

        switch (phoneNam.substring(0, 3)){
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
