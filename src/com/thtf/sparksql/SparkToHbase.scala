package com.thtf.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Scan
import java.util.Arrays
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Put
import java.util.Properties
import java.io.FileInputStream
import java.io.File

object SparkToHbase {
  def main(args: Array[String]): Unit = {

    val properties = new Properties
    properties.load(new FileInputStream(new File("/home/spark/august/test.properties")))
    
    //1.创建sc
    val conf = new SparkConf();
    conf.setAppName(properties.getProperty("appname"));
    //conf.setMaster("local");
    val sc = new SparkContext(conf);

    //2.指定相关配置信息
    val confx = HBaseConfiguration.create()
    confx.set("hbase.zookeeper.quorum", "nn.hadoop,snn.hadoop,dn2.hadoop")
    confx.set("zookeeper.znode.parent", "/hbase-unsecure");
    confx.set("hbase.zookeeper.property.clientPort", "2181")
    //Storm_Height_100
    //irregular_data_201808
    //spark-test-01
    confx.set(TableInputFormat.INPUT_TABLE, properties.getProperty("inputtable"))
    confx.set(TableInputFormat.SCAN_COLUMN_FAMILY, properties.getProperty("columnfamily"))
    
    // confx.set(TableInputFormat.SCAN_ROW_START , "rk1")
    // confx.set(TableInputFormat.SCAN_ROW_STOP , "rk3")
    //val scan = new Scan()
    //scan.setStartRow("rk1".getBytes)
    //scan.setStopRow("rk2".getBytes)
    //scan.setFilter(new RandomRowFilter(0.5f))
    //confx.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

    //3.读取hbase
    val rdd = sc.newAPIHadoopRDD(confx, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]);

    //4.遍历数据
    val outputrdd = rdd.map(t => {
      val result = t._2
      //rowkey散列设计，不用于查询
      val rowkey = Bytes.toString(result.getRow)
      val point_id = Bytes.toInt(result.getValue("c".getBytes, "point_id".getBytes))
      val sdate = Bytes.toInt(result.getValue("c".getBytes, "sdate".getBytes))
      val time = Bytes.toInt(result.getValue("c".getBytes, "time".getBytes))
    	val data = Bytes.toFloat(result.getValue("c".getBytes, "data".getBytes))
      //println(rowkey + "~" + point_id + "~" + sdate + "~" + time + "~" + data)
    	val put = new Put(Bytes.toBytes(rowkey))
    	put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("point_id"), Bytes.toBytes(point_id))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("sdate"), Bytes.toBytes(sdate))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("time"), Bytes.toBytes(time))
      put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("data"), Bytes.toBytes(data))
      
      (new ImmutableBytesWritable(), put)
      
    })
    
    //5.写入数据
    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val confy = job.getConfiguration();
    confy.set("hbase.zookeeper.quorum", "nn.hadoop,snn.hadoop,dn2.hadoop")
    confy.set("zookeeper.znode.parent", "/hbase-unsecure");
    confy.set("hbase.zookeeper.property.clientPort", "2181")
    confy.set(TableOutputFormat.OUTPUT_TABLE, properties.getProperty("outputtable"))
      
    outputrdd.saveAsNewAPIHadoopDataset(confy)
    
    sc.stop()

  }
}