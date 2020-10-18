package com.bingo.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author: jiangjiabin
 * @date: Create in 2:36 2020/10/16
 * @description:
 */
public class TestHbaseAPI_1 {

    private static final Logger log = LoggerFactory.getLogger(TestHbaseAPI_1.class);

    public static void main(String[] args) throws IOException {

        //0、创建配置对象，获取hbase的连接
        Configuration conf = HBaseConfiguration.create();

        //1、获取hbase连接对象
        //classloader: Thread.currentThread().getContextClassLoader()
        //classpath: hbase-default.xml、hbase-site.xml
        Connection connection = ConnectionFactory.createConnection();

        //2、获取操作对象: admin
        Admin admin = connection.getAdmin();

        //3、操作数据
        //3-1) 判断命名空间
        String namespace = "bingo";
        try {
            NamespaceDescriptor descriptor = admin.getNamespaceDescriptor(namespace);
        } catch (NamespaceNotFoundException e) {
            //创建命名空间
            NamespaceDescriptor nd = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(nd);
            log.info("create namespace success: {}", namespace);
        } catch (IOException e2) {
            throw e2;
        }

        //3-2)
        TableName cosmicTable = TableName.valueOf(namespace + ":school");
        boolean exists = admin.tableExists(cosmicTable);
        if (exists) {
            //获取指定的表对象
            Table table = connection.getTable(cosmicTable);
            String rowKey = "1001";
            byte[] bytes = Bytes.toBytes(rowKey);
            Get get = new Get(bytes);

            Result result = table.get(get);
            boolean isEmpty = result.isEmpty();
            log.info("is empty? => {}", isEmpty);
            if (isEmpty) {
                //add data
                Put put = new Put(bytes);
                String family = "logs";
                String column = "date";
                String value = "10.18";
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
                table.put(put);
                log.info("add data success!");
            }
            //query
            for (Cell cell : result.rawCells()) {
                log.info("value: {}", Bytes.toString(CellUtil.cloneValue(cell)));
                log.info("cf: {}", Bytes.toString(CellUtil.cloneFamily(cell)));
                log.info("qf: {}", Bytes.toString(CellUtil.cloneQualifier(cell)));
                log.info("rowKey: {}", Bytes.toString(CellUtil.cloneRow(cell)));
            }


        } else {
            //create table
            //表描述器构造器
            TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(cosmicTable);
            //列族描述起构造器
            ColumnFamilyDescriptorBuilder cdb = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("logs"));
            //获得列描述起
            ColumnFamilyDescriptor cfd = cdb.build();
            //添加列族
            tdb.setColumnFamily(cfd);
            //获得表描述器
            TableDescriptor td = tdb.build();
            //创建表
//            admin.addColumnFamily(cosmicTable, cfd); //给已存在的表添加列族
            admin.createTable(td);
            log.info("create table success! table name: {}", cosmicTable);
        }

        //4、获取操作结果

        //5、关闭数据库连接

    }
}
