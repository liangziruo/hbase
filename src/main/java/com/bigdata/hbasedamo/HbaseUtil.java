package com.bigdata.hbasedamo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author yanglei
 * @date 2019/6/13
 */
public class HbaseUtil {
    private static Configuration conf;
    private static Connection connection;
    private static Admin admin;
    static{
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "192.168.1.102");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
//        if (isTableExist("students")){
//            System.out.println("存在");
//        }else{
//            System.out.println("不存在");
//        }
//        createTable("student1","info1");
//        addRowData("student1","2001","info","name","yumimi");
        getAllRows("student1");
        close();
    }

    /**
     * 关闭流
     */
    public static void close(){
        try {
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 判断表是否存在
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        //在HBase中管理、访问表需要先创建HBaseAdmin对象
//        Connection connection = ConnectionFactory.createConnection(conf);
//        HBaseAdmin admin = new HBaseAdmin(conf);
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        return b;
    }

    /**
     * 创建表
     * @param tableName
     * @param columnFamily
     *
     * @throws IOException
     */
    public static void createTable(String tableName, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        //判断表是否存在
        if(isTableExist(tableName)){
            System.out.println("表" + tableName + "已存在");
        }else{
            //创建表属性对象,表名需要转字节
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            //创建多个列族
            for(String cf : columnFamily){
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据对表的配置，创建表
            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
        }
    }

    /**
     * 插入一条数据
     * @param tableName
     * @param rowKey
     * @param columnFamily
     * @param column
     * @param value
     * @throws IOException
     */
    public static void addRowData(String tableName, String rowKey, String columnFamily, String
            column, String value) throws IOException{
        //创建HTable对象
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
//        HTable hTable = new HTable(conf, tableName);
        //向表中插入数据
        Put put = new Put(Bytes.toBytes(rowKey));
        //向Put对象中组装数据
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }

    /**
     * 获取所有数据
     * @param tableName
     * @throws IOException
     */
    public static void getAllRows(String tableName) throws IOException{
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
        //得到用于扫描region的对象
        Scan scan = new Scan();
        //使用HTable得到resultcanner实现类的对象
        ResultScanner resultScanner = hTable.getScanner(scan);
        for(Result result : resultScanner){
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                //得到rowkey
                System.out.println("行键:" + Bytes.toString(CellUtil.cloneRow(cell)));
                //得到列族
                System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    /**
     * 精确获取某一行数据
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void getRow(String tableName, String rowKey) throws IOException{
        HTable hTable = (HTable) connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
//        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));  //获取指定列族数据
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = hTable.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

}
