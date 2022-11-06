package com.xfd.HbaseConnection;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDML {
    //静态属性
    public static Connection connection = HbaseConnection.connection;


    /**
     * 插入数据
     *
     * @param namespace   命名空间名称
     * @param tableName   表格名称
     * @param rowKey      主键
     * @param columFamily 列族名称
     * @param columName   列名
     * @param value       值
     */
    public static void putCell(String namespace, String tableName, String rowKey, String columFamily,
                               String columName, String value) throws IOException {
        //1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 调用相关方法插入数据
        // 2.1 创建Put对象
        Put put = new Put(Bytes.toBytes(rowKey));

        // 2.2 给put对象添加数据
        put.addColumn(Bytes.toBytes(columFamily), Bytes.toBytes(columName), Bytes.toBytes(value));

        // 2.3 将对象写入对应方法
        try {
            table.put(put);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 3. 关闭table
        table.close();
    }


    /**
     * 读取数据 读取对应的一行中的数据
     *
     * @param namespace   命名空间名称
     * @param tableName   表格名称
     * @param rowKey      主键
     * @param columFamily 列族名称
     * @param columName   列名
     */
    public static void getCells(String namespace, String tableName, String rowKey, String columFamily,
                                String columName) throws IOException {
        // 1. 获取table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        // 2. 创建get
        Get get = new Get(Bytes.toBytes(rowKey));

        // 如果直接调用get方法读取数据 此时读取一整行数据
        // 如果想读取某一列数据 需要添加对应参数

        get.addColumn(Bytes.toBytes(columFamily), Bytes.toBytes(columName));

        // 设置读取数据版本
        get.readAllVersions();

        try {
            // 读取对象 得到result对象
            Result result = table.get(get);

            // 处理数据
            Cell[] cells = result.rawCells();

            // 测试方法：直接把读取的数据打印到数据台
            // 如果是实际开发 需要在额外写方法 对应处理参数
            for (Cell cell : cells) {
                // cell 存储数据比较底层
                String value = new String(CellUtil.cloneValue(cell));
                System.out.println(value);
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        // 关闭table
        table.close();
    }

    /**
     * 扫描数据
     *
     * @param namespace 命名空间
     * @param tableName 表格名称
     * @param startRow  开始的row 包含的
     * @param stopRow   结束的row 不包含
     */
    public static void scanRows(String namespace, String tableName, String startRow, String stopRow) throws IOException {
        // 1。 获取Table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //2. 创建scan对象
        Scan scan = new Scan();
        // 如果此时直接调用，会直接扫描整张表

        //添加参数 来控制扫描的数据
        // 默认包含
        scan.withStartRow(Bytes.toBytes(startRow));

        // 默认不包含
        scan.withStopRow(Bytes.toBytes(stopRow));


        try {
            // 读取多行数据 获得scanner
            ResultScanner scanner = table.getScanner(scan);
            // result来记录一行数据  cell数组
            // ResultScanner来记录多行数据 result的数组
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell)) + "-" +
                            new String(CellUtil.cloneFamily(cell)) + "-" +
                            new String(CellUtil.cloneQualifier(cell)) + "-" +
                            new String(CellUtil.cloneValue(cell)) + "\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // 关闭table
        table.close();
    }


    /**
     * 带过滤的扫描
     *
     * @param namespace    命名空间
     * @param tableName    表格名称
     * @param startRow     开始的row 包含的
     * @param stopRow      结束的row 不包含
     * @param columnFamily 列族名称
     * @param columnName   列名
     * @param value        value值
     * @throws IOException
     */
    public static void filterScan(String namespace, String tableName, String startRow,
                                  String stopRow, String columnFamily, String columnName, String value) throws IOException {
        // 1。 获取Table
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //2. 创建scan对象
        Scan scan = new Scan();
        // 如果此时直接调用，会直接扫描整张表

        //添加参数 来控制扫描的数据
        // 默认包含
        scan.withStartRow(Bytes.toBytes(startRow));

        // 默认不包含
        scan.withStopRow(Bytes.toBytes(stopRow));


        // 可以添加多个过滤
        FilterList filterList = new FilterList();

        // 创建过滤器
        // (1) 结果只保留当前列的数据
        ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                // 列族名称
                Bytes.toBytes(columnFamily),
                // 列名
                Bytes.toBytes(columnName),
                // 比较关系
                CompareOperator.EQUAL,
                //值
                Bytes.toBytes(value)
        );
        // 结果保留整行数据
        // 结果会保留没有当前列的数据
         SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                 // 列族名称
                 Bytes.toBytes(columnFamily),
                 // 列名
                 Bytes.toBytes(columnName),
                 // 比较关系
                 CompareOperator.EQUAL,
                 //值
                 Bytes.toBytes(value)
         );

        // (1)
        //filterList.addFilter(columnValueFilter);
        // (2)
        filterList.addFilter(singleColumnValueFilter);
        // 添加过滤
        scan.setFilter(filterList);


        try {
            // 读取多行数据 获得scanner
            ResultScanner scanner = table.getScanner(scan);
            // result来记录一行数据  cell数组
            // ResultScanner来记录多行数据 result的数组
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();

                for (Cell cell : cells) {
                    System.out.print(new String(CellUtil.cloneRow(cell)) + "-" +
                            new String(CellUtil.cloneFamily(cell)) + "-" +
                            new String(CellUtil.cloneQualifier(cell)) + "-" +
                            new String(CellUtil.cloneValue(cell)) + "\t");
                }
                System.out.println();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // 关闭table
        table.close();
    }

    /**
     * 删除一行中的一列数据
     * @param namespace 命名空间
     * @param tableName 表格名称
     * @param rowKey 主键
     * @param columnFamily 列族
     * @param columnName 列名
     */
    public static void deleteColumn(String namespace, String tableName,String rowKey,String columnFamily, String columnName) throws IOException {
        // 1. 获取table
         Table table = connection.getTable(TableName.valueOf(namespace, tableName));

         // 2. 闯进Delete对象
          Delete delete = new Delete(Bytes.toBytes(rowKey));

        // 3. 添加列信息
        // addColumn 删除一个版本
        delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        // addColumns 删除所有版本
        // 按照逻辑需要删除所有版本的数据
        delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));

        try {
            table.delete(delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 3. 关闭table
        table.close();
    }




    public static void main(String[] args) throws IOException {
//        // 测试添加数据
//        putCell("bigdata", "student", "2001", "info",
//                "name", "xiaoming");
//        putCell("bigdata", "student", "2002", "info",
//                "name", "wangwu");
//
//        // 测试读取数据
        getCells("bigdata", "student", "2001", "info",
                "name");

        // 测试扫描数据
        //scanRows("bigdata", "student", "1001", "2002");

//        filterScan("bigdata", "student", "1001", "2002", "info",
//                "name", "lisi");

        // 测试删除数据
        deleteColumn("bigdata", "student", "2001", "info",
                "name");

        // 其他代码
        System.out.println("其他代码");

        // 关闭连接
        HbaseConnection.closeConnection();
    }
}
