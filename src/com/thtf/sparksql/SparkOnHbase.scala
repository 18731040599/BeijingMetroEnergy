package com.thtf.sparksql

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf

/**
 * Spark 读取和写入 HBase
 * 将Hbase表读成RDD，以编程的方式指定 Schema，将RDD转化为DataFrame。
 **/
object SparkOnHBase {


  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def main(args: Array[String]) {
    
    println("Start!")
    
    val sparkConf = new SparkConf()
      .setAppName("test spark sql")
      .setMaster("local")
      .set("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")
	  val sc = new SparkContext(sparkConf)


    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "stest")
    
    // ======Load RDD from HBase========
    // use `newAPIHadoopRDD` to load RDD from HBase
    //直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]

    //设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, "newuser")

    val usersRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val rowRDD = usersRDD
      .map(_._2)
      .map(result => (Bytes.toString(result.getRow),
    		  Bytes.toString(result.getValue("f".getBytes,"name".getBytes)),
    		  Bytes.toInt(result.getValue("f".getBytes,"age".getBytes)),
    		  Bytes.toString(result.getValue("f".getBytes,"password".getBytes))))
      .map(row => Row(row._1,row._2,row._3,row._4))
      // =================================
    
    // 以编程的方式指定 Schema，将RDD转化为DataFrame
    val schemaString = "rowkey name age password"
    val fields = schemaString.split(" ").map(field => StructField(field,StringType,nullable = true))
    //val schema = StructType(fields)
    val schema = StructType(StructField("rowkey",StringType,true)::
                        	  StructField("name",StringType,true)::
                        		StructField("age",IntegerType,true)::
                        	  StructField("password",StringType,true)::Nil)
      
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    
    val df = sqlContext.createDataFrame(rowRDD, schema)
    df.createTempView("newuser")
    sqlContext.sql("select * from newuser").show()
    sqlContext.sql("select * from newuser where age > 26").show()
      
    //val count = usersRDD.count()
    //println("Users RDD Count:" + count)
    usersRDD.cache()

    println("Done!")
  }
}
