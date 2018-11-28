package com.thtf.sparksql

import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{ Base64, Bytes }
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.client._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.filter._

/**
 * Spark 读取和写入 HBase
 * 将Hbase表读成RDD，以编程的方式指定 Schema，将RDD转化为DataFrame。
 */
object SparkOnHbase {

  def convertScanToString(scan: Scan) = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def main(args: Array[String]) {

    println("Start!")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")
      .getOrCreate()
    import spark.implicits._

    // 表的字段名
    val center_fields = "lineName abbreviation projectName description eneryMgr".split(" ")
    val line_fields = "stationName abbreviation lineName description area eneryMgr transferstation".split(" ")
    val point_fields = "lineName stationName pointName type controlID equipmentID equipmentName dataType statics unit quantity lowerLimitValue upperLimitValue description".split(" ")
    val user_fields = "id name age password".split(" ")
    val tables = Map(
      "center" -> center_fields,
      "line" -> line_fields,
      "point" -> point_fields)

    // ======Load RDD from HBase========
    // use `newAPIHadoopRDD` to load RDD from HBase
    //直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]

    val hConf = HBaseConfiguration.create()
    hConf.set("hbase.zookeeper.property.clientPort", "2181")
    hConf.set("hbase.zookeeper.quorum", "stest")
    //设置查询的表名
    hConf.set(TableInputFormat.INPUT_TABLE, "user")

    val scan = new Scan
    //scan.setStartRow(Bytes.toBytes(".*[13]"));
    //scan.setStopRow(Bytes.toBytes(""))
    // 用于监测一个子串是否存在于值中，并且不区分大小写
    //val filter = new RowFilter(CompareFilter.CompareOp.EQUAL,new SubstringComparator("2"))

    //val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(".*(11|22){1}"))
    //scan.setFilter(filter)
    //conf.set(TableInputFormat.SCAN, convertScanToString(scan))

    val usersRDD = spark.sparkContext.newAPIHadoopRDD(hConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val rowRDD = usersRDD
      .map(_._2)
      .map(result => (
        Bytes.toString(result.getRow),
        Bytes.toString(result.getValue("f".getBytes, "name".getBytes)),
        Bytes.toString(result.getValue("f".getBytes, "age".getBytes)),
        Bytes.toString(result.getValue("f".getBytes, "password".getBytes))))
      .map(row => Row(row._1, row._2, row._3, row._4))
    rowRDD.foreach(row => {
      println(row.toString())
    })
    val rRDD = usersRDD
      .map(_._2)
      .map(result => {
        var row = Row(Bytes.toString(result.getRow))
        println(result)
        println(result.size())
        for (i <- 1 to result.size()) {
          row = Row.merge(row, Row(Bytes.toString(result.getValue("f".getBytes, user_fields(i).getBytes))))
        }
        row
      })
    rRDD.foreach(row => {
      println(row.toString())
    })
    // =================================

    // 以编程的方式指定 Schema，将RDD转化为DataFrame
    // map转换
    val fields = user_fields.map(field => StructField(field, StringType, nullable = true))
    // fields(2) = StructField("age", IntegerType, true)
    val schema = StructType(fields)
    // 逐个转换
    /*val schema = StructType(StructField("rowkey", StringType, true) ::
      StructField("name", StringType, true) ::
      StructField("age", IntegerType, true) ::
      StructField("password", StringType, true) :: Nil)*/

    val df = spark.createDataFrame(rowRDD, schema)
    df.createTempView("newuser")
    spark.sql("select * from newuser").show()
    val row1 = spark.sql("select * from newuser where age > 26").take(2)(0).getString(0)
    println(row1.toInt)

    hisdataYCToDF("hisdataYC")
    spark.sql("select * from hisdataYC").show()
    //spark.sql("select * from hisdataYC where TIME > 20180810001000").show()

    hisdataYXToDF("hisdataYX")
    spark.sql("select * from hisdataYX").show()

    //val count = usersRDD.count()
    //println("Users RDD Count:" + count)
    usersRDD.cache()

    println("Done!")

    def hisdataYCToDF(tablename: String) {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.zookeeper.quorum", "stest")
      //直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]
      conf.set(TableInputFormat.INPUT_TABLE, tablename)
      val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(s".*(20180810002000|20180810001000){1}"))
      conf.set(TableInputFormat.SCAN, convertScanToString(new Scan().setFilter(filter)))
      val tableRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
        .map(_._2)
        .map(result => {
          var row = Row(Bytes.toString(result.getRow).dropRight(14), Bytes.toString(result.getRow).takeRight(14))
          for (i <- 0 to result.size() - 1) {
            row = Row.merge(row, Row(Bytes.toString(result.getValue("c".getBytes, i.toString().getBytes))))
          }
          row
        })
      // 以编程的方式指定 Schema，将RDD转化为DataFrame
      val fields = (-2 to 599).toArray.map(field => StructField(field.toString(), StringType, nullable = true))
      fields(0) = StructField("abbreviation".toString(), StringType, nullable = true)
      fields(1) = StructField("time".toString(), StringType, nullable = true)
      spark.createDataFrame(tableRDD, StructType(fields)).createTempView(tablename)
    }

    def hisdataYXToDF(tablename: String) {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.zookeeper.quorum", "stest")
      //直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]
      conf.set(TableInputFormat.INPUT_TABLE, tablename)
      val tableRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
        .map(_._2)
        .map(result => {
          val str = Bytes.toString(result.getRow)
          Row(str.replaceAll("[0-9]", ""), str.replaceAll("[^0-9]", "").takeRight(14), str.replaceAll("[^0-9]", "").dropRight(14),Bytes.toString(result.getValue("c".getBytes, "value".getBytes)))
        })
      // 以编程的方式指定 Schema，将RDD转化为DataFrame
      val schema = StructType(StructField("abbreviation", StringType, true) ::
        StructField("time", StringType, true) ::
        StructField("id", StringType, true) ::
        StructField("value", StringType, true) :: Nil)
      spark.createDataFrame(tableRDD, schema).filter($"time" > "20180810000000" || $"time" <= "20180810002000").createTempView(tablename)
    }

  }
}
