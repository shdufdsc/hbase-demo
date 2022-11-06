package com.xfd.HbaseConnection;

import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseDDL {
    //声明一个静态属性
    public static Connection connection = HbaseConnection.connection;

    /*
     * 创建命名空间  namespace命名空间名称
     * 创建命名空间
     * */
    public static void createNamespace(String namespace)
            throws IOException {
        //1. 获取admin
        //admin 的连接是轻量级的，不是线程安全的，不推荐池化或者缓存这个连接
        Admin admin = connection.getAdmin();

        //2. 调用方法创建命名空间
        // 代码相对 shell 更加底层 所以 shell 能够实现的功能 代码一定能实现
        // 所以需要填写完整的命名空间描述

        //2.1 创建命名空间描述
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(namespace);

        //2.2 给命名空间添加需求
        builder.addConfiguration("user", "xfd");

        //2.3 使用builder 构造出对应的添加完参数的对象 完成创建
        try {
            admin.createNamespace(builder.build());
        } catch (IOException e) {
            System.out.println("命名空间已经存在");
            e.printStackTrace();
        }

        //3. 关闭admin
        admin.close();
    }


    /*
     * 判断表格是否存在
     * namespace 命名空间名称
     * tabName 表格名称
     * true  表示存在
     * */
    public static boolean isTableExists(String namespace, String tableName)
            throws IOException {
        //1. 获取admin
        Admin admin = connection.getAdmin();

        //2. 使用方法判断表格是否存在
        boolean b = false;
        try {
            b = admin.tableExists(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        //3. 关闭admin
        admin.close();

        //4. 返回结果
        return b;
    }


    /**
     * 创建表格
     *
     * @param namespace      命名空间名称
     * @param tableName      表格名称
     * @param columnFamilies 列族名称 可以有多个
     */
    public static void createTable(String namespace, String tableName, String... columnFamilies)
            throws IOException {
        //判断是否最少有一个列族
        if (columnFamilies.length == 0) {
            System.out.println("创建表格至少有一个列族");
            return;
        }

        // 判断表格是否存在
        if (isTableExists(namespace, tableName)) {
            System.out.println("表格已经存在");
            return;
        }


        // 1.获取admin
        Admin admin = connection.getAdmin();


        // 2.调用方法创建表格
        // 2.1 创建表格描述的建造者
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName
                .valueOf(namespace, tableName));

        // 2.2 添加参数
        for (String columnFamily : columnFamilies) {
            // 2.3 创建列族描述的建造者
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(columnFamily));

            // 2.4 对应当前的列族添加参数
            // 添加版本参数
            columnFamilyDescriptorBuilder.setMaxVersions(5);

            // 2.5 创建添加完参数的列族描述
            tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptorBuilder.build());
        }
        // 2.6 创建对应的表格描述
        try {
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            //System.out.println("表格已经存在");
            e.printStackTrace();
        }

        // 3 关闭admin
        admin.close();
    }


    /**
     * 修改表格中一个列族的版本
     *
     * @param namespace   命名空间名称
     * @param tableName   表格名称
     * @param columFamily 列族名称
     * @param version     版本
     */
    public static void modifyTable(String namespace, String tableName, String columFamily, int
            version) throws IOException {

        // 判断表格是否存在
        if (!isTableExists(namespace, tableName)) {
            System.out.println("表格不存在无法修改");
            return;
        }


        //1. 获取admin
        Admin admin = connection.getAdmin();

        try {
            // 2. 调用方法修改表格
            // 2.0 获取之前的表格对象
            TableDescriptor descriptor = admin.getDescriptor(TableName.valueOf(namespace, tableName));

            // 2.1 创建一个表格描述建造者
            // 如果使用填写tableName 的方法 相当于创建了一个新的表格描述建造者 没有之前的信息
            // 想要修改之前的信息 必须调用方法填写一个旧的表格描述
            TableDescriptorBuilder tableDescriptorBuilder =
                    TableDescriptorBuilder.newBuilder(descriptor);

            // 2.2 对应建造者进行表格数据的修改
            ColumnFamilyDescriptor columnFamily1 =
                    descriptor.getColumnFamily(Bytes.toBytes(columFamily));

            // 创建列族描述建造者
            //需要填写旧的列族描述
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(columnFamily1);

            // 修改对应版本
            columnFamilyDescriptorBuilder.setMaxVersions(version);

            // 此处修改的时候 如果填写的新创建 那么别的参数会初始化
            tableDescriptorBuilder.modifyColumnFamily(columnFamilyDescriptorBuilder.build());


            admin.modifyTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 3. 关闭admin
        admin.close();
    }


    /**
     * 删除表格
     *
     * @param namespace 命名空间名称
     * @param tableName 表格名称
     * @return true 表示删除成功
     */
    public static boolean deleteTable(String namespace, String tableName) throws IOException {
        // 1， 判断报告是否存在
        if (!isTableExists(namespace, tableName)) {
            System.out.println("表格不存在无法删除");
            return false;
        }

        // 2. 获取admin
        Admin admin = connection.getAdmin();

        // 3. 调研相关的方法删除表格
        try {
            //Hbase 删除表格之前 一定要标记为不可以
            TableName tableName1 = TableName.valueOf(namespace, tableName);
            admin.disableTable(tableName1);
            admin.deleteTable(tableName1);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 4. 关闭admin
        admin.close();
        return true;
    }


    public static void main(String[] args)
            throws IOException {
        //测试创建命名空间
        //createNamespace("xfd");

        // 测试判断表格是否存在
        //System.out.println(isTableExists("bigdata1211", "student"));

        // 测试创建表格
        //createTable("xfd", "student", "info", "msg");

        // 测试修改表格
        //modifyTable("bigdata", "student", "info", 3);

        // 测试删除表格
        //deleteTable("xfd","student");

        // 其他代码
        System.out.println("其他代码");

        //关闭Hbase 的连接
        HbaseConnection.closeConnection();
    }
}
