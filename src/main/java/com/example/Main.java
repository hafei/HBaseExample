package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class Main {
    static {
        try {
            Field field = Class.forName("sun.misc.Unsafe").getDeclaredField("theUnsafe");
            field.setAccessible(true);
            Object unsafe = field.get(null);
            Method method = Class.forName("java.nio.Bits").getDeclaredMethod("unaligned");
            method.setAccessible(true);
            method.invoke(unsafe);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Hello world!");
        connectionTest(args);
//        createTable(args);
        insertData(args);
        getData(args);
    }


    public static void createTable(String[] args) throws IOException {
        Connection connection = getConnection();

        // Instantiating Admin class
        Admin admin = connection.getAdmin();

        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("emp"));

        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
        tableDescriptor.addFamily(new HColumnDescriptor("professional"));

        // Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println("Table created");

        // Closing the admin
        admin.close();
        connection.close();
    }

    public static void insertData(String[] args) throws IOException {
        // Instantiating configuration class
        Connection connection = getConnection();

        // Instantiating HTable class
        Table table = connection.getTable(TableName.valueOf("t_example"));

        // 准备插入数据
        String rowKey = "user1";
        byte[] rowKeyBytes = Bytes.toBytes(rowKey);

        Put put = new Put(rowKeyBytes);
        put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("name"), Bytes.toBytes("John Doe"));
        put.addColumn(Bytes.toBytes("cf_info"), Bytes.toBytes("age"), Bytes.toBytes(30));
        put.addColumn(Bytes.toBytes("cf_details"), Bytes.toBytes("email"), Bytes.toBytes("john.doe@example.com"));

        // 插入数据
        table.put(put);

        // 关闭表和连接
        table.close();
        connection.close();

        System.out.println("Data inserted into HBase successfully!");
    }

    public static void connectionTest(String[] args) {
        try {
            Connection connection = getConnection();

            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void getData(String[] args) {

        try {

            Connection connection = getConnection();

            Table table = connection.getTable(TableName.valueOf("t_example"));

            String rowKey = "user1";
            byte[] rowKeyBytes = Bytes.toBytes(rowKey);

            Get get = new Get(rowKeyBytes);
            get.addFamily(Bytes.toBytes("cf_info"));

            Result result = table.get(get);

            if (!result.isEmpty()) {
                System.out.println("Row data:");
                for (Cell cell : result.rawCells()) {
                    String family = Bytes.toString(CellUtil.cloneFamily(cell));
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    System.out.format("Family: %s, Qualifier: %s, Value: %s%n", family, qualifier, value);
                }
            } else {
                System.out.println("No data found for the given row key.");
            }
            table.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static Connection getConnection() throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.setInt("hbase.rpc.timeout", 60000); //
        config.setInt("hbase.client.operation.timeout", 60000); //
        config.setInt("hbase.client.retries.number", 10); //

        Connection connection = ConnectionFactory.createConnection(config);
        System.out.println("Connected to HBase successfully!");
        return connection;
    }
}