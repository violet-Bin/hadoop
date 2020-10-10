package com.bingo.hadoop.mapreduce.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @author: jiangjiabin
 * @date: Create in 23:52 2020/8/25
 * @description:
 */
public class HDFSClient {

    FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        System.out.println("Before--");
        fs = FileSystem.get(URI.create("hdfs://bingo100:9000"), new Configuration(), "bingo");
    }

    @Test
    public void put() throws IOException, InterruptedException {
        fs.copyFromLocalFile(new Path("E:\\SoftWare\\Hadoop\\test\\1.txt"), new Path("/"));
    }

    @Test
    public void append() throws IOException {
        FSDataOutputStream append = fs.append(new Path("/1.txt"));
        FileInputStream open = new FileInputStream("E:\\SoftWare\\Hadoop\\test\\2.txt");
        IOUtils.copyBytes(open, append, 1024);
    }

    @Test
    public void ls() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isFile()) {
                System.out.println("以下信息是文件信息");
                System.out.println(fileStatus.getPath());
                System.out.println(fileStatus.getLen());
            } else {
                System.out.println("以下信息是文件jia信息");
                System.out.println(fileStatus.getPath());
            }
        }
    }

    @Test
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);

        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            System.out.println("=======================");

            System.out.println(file.getPath());
            System.out.println("块信息--");
            BlockLocation[] blockLocations = file.getBlockLocations();
            for (BlockLocation b : blockLocations) {
                String[] hosts = b.getHosts();
                System.out.print("块在：");
                for (String host : hosts) {
                    System.out.print(host + " ");
                }
            }
        }
    }

    @After
    public void after() throws IOException {
        System.out.println("After--");
        fs.close();
    }

}
