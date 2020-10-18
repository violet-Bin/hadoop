package com.bingo.hbase.mr.tool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * @author: jiangjiabin
 * @date: Create in 20:51 2020/10/18
 * @description:
 */
public class HbaseMRTool implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(HbaseMRTool.class);

        

        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }
}
