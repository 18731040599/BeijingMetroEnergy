package com.thtf.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

object SparkWrite {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf();
    conf.setAppName("spark_hbase");
    conf.setMaster("local");
    val sc = new SparkContext(conf);

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "nn.hadoop,snn.hadoop,dn2.hadoop")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    //dzb_p_d01
    //irregular_data_201707
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "dzb_p_d01")

    val job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val confx = job.getConfiguration();

    val indataRDD = sc.makeRDD(Array("rk1,zhang,19", "rk2,li,29", "rk3,wang,39"))
    val rdd = indataRDD.map(_.split(',')).map { arr =>
      {
        val put = new Put(Bytes.toBytes(arr(0)))
        put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
        put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("age"), Bytes.toBytes(arr(2).toInt))
        (new ImmutableBytesWritable(), put)
      }
    }
    rdd.saveAsNewAPIHadoopDataset(confx)
  }
}