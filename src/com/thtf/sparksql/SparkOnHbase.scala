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
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._TableCol
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{ Logger, Level }

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
    
    Logger.getLogger("org").setLevel(Level.ERROR)

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

    // KPI表的字段名
    val KPI_fields = ArrayBuffer[String]("abbreviation", "time", "d1", "d2", "d3", "d4", "d5", "d10", "d15", "d20", "d24")

    // 时间
    val endTime = "20180216202000"
    val startTime = "20180216200000"
    val lastDay = "20180215202000"

    // =================== 将读取hbase，将rdd转换为dataframe ===================
    def hisdataKPIToDF(tablename: String) = {
      val fields = KPI_fields.clone()
      fields.remove(2)
      // 定义Hbase的配置
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.zookeeper.quorum", "stest")
      // 直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]
      conf.set(TableInputFormat.INPUT_TABLE, tablename)
      val tableRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
        .map(_._2)
        .map(result => {
          var row = Row(Bytes.toString(result.getRow).dropRight(14), Bytes.toString(result.getRow).takeRight(14))
          for (i <- 0 until result.size()) {
            row = Row.merge(row, Row(Bytes.toString(result.getValue("c".getBytes, fields(i + 2).getBytes))))
          }
          row
        })
      // 以编程的方式指定 Schema，将RDD转化为DataFrame
      val schema = fields.map(field => StructField(field, StringType, nullable = true))
      spark.createDataFrame(tableRDD, StructType(schema)).filter($"time" > lastDay && $"time" <= endTime)
    }
    // ========================================

    def finalHtableToDF(tablename: String) = {
      // 定义Hbase的配置
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.zookeeper.quorum", "stest")
      // 直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]
      conf.set(TableInputFormat.INPUT_TABLE, tablename)
      val columns = tables.getOrElse(tablename, null)
      val tableRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
        .map(_._2)
        .map(result => {
          var row = Row()
          for (i <- 0 until columns.length) {
            row = Row.merge(row, Row(Bytes.toString(result.getValue("c".getBytes, columns(i).getBytes))))
          }
          row
        })
      // 以编程的方式指定 Schema，将RDD转化为DataFrame
      val fields = columns.map(field => StructField(field, StringType, nullable = true))
      spark.createDataFrame(tableRDD, StructType(fields))
    }

    def hisdataYCToDF(tablename: String) = {
      // 定义Hbase的配置
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.zookeeper.quorum", "stest")
      // 直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]
      conf.set(TableInputFormat.INPUT_TABLE, tablename)
      //                                                                                  ".*(20171212104000|20171213102000|20171213104000){1}"
      val filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(s".*(${lastDay}|${startTime}|${endTime}){1}"))
      conf.set(TableInputFormat.SCAN, convertScanToString(new Scan().setFilter(filter)))
      val tableRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat],
        classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
        classOf[org.apache.hadoop.hbase.client.Result])
        .map(_._2)
        .map(result => {
          var row = Row(Bytes.toString(result.getRow).dropRight(14), Bytes.toString(result.getRow).takeRight(14))
          for (i <- 0 to 599) {
            val value = Bytes.toString(result.getValue("c".getBytes, i.toString().getBytes))
            if (value == null) {
              row = Row.merge(row, Row("0"))
            } else {
              row = Row.merge(row, Row(value))
            }
          }
          row
        })
      // 以编程的方式指定 Schema，将RDD转化为DataFrame
      val fields = (-2 to 599).toArray.map(field => StructField(field.toString(), StringType, nullable = true))
      fields(0) = StructField("abbreviation".toString(), StringType, nullable = true)
      fields(1) = StructField("time".toString(), StringType, nullable = true)
      spark.createDataFrame(tableRDD, StructType(fields))
    }

    def hisdataYXToDF(tablename: String) = {
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
          Row(str.replaceAll("[0-9]", ""), str.replaceAll("[^0-9]", "").takeRight(14), str.replaceAll("[^0-9]", "").dropRight(14), Bytes.toString(result.getValue("c".getBytes, "value".getBytes)))
        })
      // 以编程的方式指定 Schema，将RDD转化为DataFrame
      val schema = StructType(StructField("abbreviation", StringType, true) ::
        StructField("time", StringType, true) ::
        StructField("id", StringType, true) ::
        StructField("value", StringType, true) :: Nil)
      spark.createDataFrame(tableRDD, schema).filter($"time" > lastDay && $"time" <= endTime)
    }

    // ======Load RDD from HBase========
    // use `newAPIHadoopRDD` to load RDD from HBase
    //直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]
    /*
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
    val schema = StructType(StructField("rowkey", StringType, true) ::
      StructField("name", StringType, true) ::
      StructField("age", IntegerType, true) ::
      StructField("password", StringType, true) :: Nil)

    val df = spark.createDataFrame(rowRDD, schema)
    df.createTempView("newuser")
    spark.sql("select * from newuser").show()
    println(spark.sql("select * from newuser where age > 100").count())
    println(spark.sql("select * from newuser where age > 100").collect().length)
    val row1 = spark.sql("select * from newuser where age > 26").take(2)(0).getString(0)
    println(row1.toInt)
	*/
    //val name = "hisdataYC"
    hisdataYCToDF("hisdataYC").printSchema()
    hisdataYXToDF("hisdataYX").printSchema()
    finalHtableToDF("center").printSchema()
    finalHtableToDF("point").printSchema()
    finalHtableToDF("line").printSchema()
    hisdataKPIToDF("KPI_real").printSchema()
    hisdataKPIToDF("hisstatus").printSchema()
    // val day = hisdataKPIToDF("KPI_day")
    println("-----------------------")
    //YC.show()
    //YX.show()
    //tatus.filter($"abbreviation" === "slgynm").show
    // day.filter($"abbreviation" === "slgynm").show
    //real.filter($"abbreviation" === "slgynm").agg("d2" -> "sum", "d3" -> "sum", "d4" -> "sum", "d5" -> "sum", "d10" -> "sum", "d15" -> "sum", "d20" -> "sum", "d24" -> "sum").select("sum(d2)", "sum(d3)", "sum(d4)", "sum(d5)", "sum(d10)", "sum(d15)", "sum(d20)", "sum(d24)").show
    // day.filter($"abbreviation" === "slgynm").agg("d2" -> "sum", "d3" -> "sum", "d4" -> "sum", "d5" -> "sum", "d10" -> "sum", "d15" -> "sum", "d20" -> "sum", "d24" -> "sum").show
    
    //val arr = Array($"20",$"30")
    //val arr = Array(new ColumnName("20"),new ColumnName("30"))
    //DF.select("20","30").show()
    //DF.select(arr: _*).show()
    // DF.filter($"id".isin(Array(80, 304): _*)).show()
    // DF.filter($"value" === 1).select("id").show()

    //spark.sql(s"select * from ${name} where abbreviation == 'slgynm'").show()
    //println(spark.sql(s"select `448`,`455` from ${name} where abbreviation == 'bdz'").take(1)(0).toString())
    //println(spark.sql(s"select `448`,`455` from ${name} where abbreviation == 'bdz'").take(1)(0).get(0) == null)
    //println(spark.sql(s"select `448`,`455` from ${name} where abbreviation == 'bdz'").take(1)(0).get(0) == "\\N")
    /*
    spark.sql(s"select * from ${name}").show()
    spark.sql(s"SELECT SUM(d2),SUM(d3),SUM(d4),SUM(d5),SUM(d10),SUM(d15),SUM(d20),SUM(d24) FROM `${name}`").show()
    val sum = spark.sql(s"SELECT SUM(d2),SUM(d3),SUM(d4),SUM(d5),SUM(d10),SUM(d15),SUM(d20),SUM(d24) FROM `${name}`").collect()
    println("count:"+sum.length)
    println("result:"+sum(0).toString())
    println("result:"+sum(0).getDouble(0))
    println("result:"+sum(0).getString(0))
    println(sum(0).get(0) == null)
    println(sum(0).get(0) == "null")
    println(sum(0).getString(0) == null)
    println(sum(0).getString(0) == "null")
    */
    //spark.sql("select * from hisdataYC where abbreviation == 'llq'").show()
    //spark.sql("select * from hisdataYC where TIME > 20180810001000").show()

    //hisdataYXToDF("hisdataYX")
    //spark.sql("select * from hisdataYX").show()
    //spark.sql("SELECT o.* FROM `hisdataYX` o ,(SELECT MAX(TIME) AS mt,id FROM `hisdataYX` GROUP BY id) t WHERE o.id=t.id AND o.time =t.mt").show()

    //val count = usersRDD.count()
    //println("Users RDD Count:" + count)
    //usersRDD.cache()

    println("Done!")

    /*def hisdataYCToDF(tablename: String) {
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
    }*/

  }
}
