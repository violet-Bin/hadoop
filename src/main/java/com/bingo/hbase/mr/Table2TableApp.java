package com.bingo.hbase.mr;

import com.bingo.hbase.mr.tool.HbaseMRTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author: jiangjiabin
 * @date: Create in 20:49 2020/10/18
 * @description:
 */
public class Table2TableApp {

    public static void main(String[] args) throws Exception {
        //ToolRunner可以运行MR
        ToolRunner.run(new HbaseMRTool(), args);

    }
}
