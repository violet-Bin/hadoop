package com.bingo.hbase.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 22:09 2020/10/18
 * @description:
 */
public class TestApi2 {

    public static void main(String[] args) throws IOException, DeserializationException {
        Connection connection = HbaseUtils.makeConnection();
        String tableName = "mynamespace:cosmic";
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

//        scan.addFamily(Bytes.toBytes("logs"));

//        ByteArrayComparable comparable = new BinaryComparator(Bytes.toBytes("1001"));
//        Filter filter = new RowFilter(CompareOperator.EQUAL, comparable);

        FilterList filterList = new FilterList();
        Filter columnValueFilter = new ColumnValueFilter(Bytes.toBytes("logs"), Bytes.toBytes("date"), CompareOperator.EQUAL, Bytes.toBytes("2"));
        filterList.addFilter(columnValueFilter);
        scan.setFilter(filterList);

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
}
