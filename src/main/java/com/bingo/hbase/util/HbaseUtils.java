package com.bingo.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 20:00 2020/10/18
 * @description:
 */
public class HbaseUtils {

    private static ThreadLocal<Connection> connHolder = new ThreadLocal<>();

    /**
     * 创建连接
     *
     * @throws IOException
     */
    public static Connection makeConnection() throws IOException {
        Connection conn = connHolder.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            connHolder.set(conn);
        }
        return conn;
    }

    /**
     * 插入数据
     *
     * @param rowKey
     * @param tableName
     * @param family
     * @param column
     * @param value
     * @throws IOException
     */
    public static void insertData(String rowKey, String tableName, String family, String column, String value) throws IOException {
        Connection connection = connHolder.get();
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));

        table.put(put);
        table.close();
    }

    /**
     * 获取一行的数据
     *
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static Result getRowData(String tableName, String rowKey) throws IOException {

        Connection conn = connHolder.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        if (!result.isEmpty()) {
            for (Cell cell : result.rawCells()) {
                System.out.println("rowKey: " + Bytes.toString(result.getRow()));
                System.out.println("cf: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        return result;
    }

    /**
     * 扫描区间数据
     */
    public static void scanData(String tableName, String startRow, String endRow) throws IOException {
        Connection connection = connHolder.get();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(endRow));

        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("rowKey: " + Bytes.toString(result.getRow()));
                System.out.println("cf: " + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value: " + Bytes.toString(CellUtil.cloneValue(cell)));
            }
            System.out.println();
            System.out.println("-----------------");
            System.out.println();
        }
    }

    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public static void close() throws IOException {
        Connection conn = connHolder.get();
        if (conn != null) {
            conn.close();
            connHolder.remove();
        }
    }
}
