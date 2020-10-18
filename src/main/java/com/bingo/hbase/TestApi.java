package com.bingo.hbase;

import com.bingo.hbase.util.HbaseUtils;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 20:21 2020/10/18
 * @description:
 */
public class TestApi {

    public static void main(String[] args) throws IOException {
        HbaseUtils.makeConnection();

//        HbaseUtils.insertData("1015", "mynamespace:cosmic", "logs", "data", "2");

        HbaseUtils.getRowData("mynamespace:cosmic", "1015");

        System.out.println("===============");

        HbaseUtils.scanData("mynamespace:cosmic", "1010", "1016");
        HbaseUtils.close();
    }
}
