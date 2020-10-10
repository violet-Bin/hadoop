package com.bingo.hadoop.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 1:04 2020/8/27
 * @description: LongWritable 行首在文件中的偏移量（第一个字母）
 * 第二个Text是文本输入
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final String BLACK = " ";

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿到这行数据
        String line = value.toString();

        //按空格切分
        String[] words = line.split(BLACK);

        //遍历数组，把单词变成(w, 1)形式交给框架
        for (String w : words)
            this.word.set(w);
        context.write(this.word, this.one);
    }
}
