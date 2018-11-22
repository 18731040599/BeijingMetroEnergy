package com.thtf.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import scala.util.Random
import java.text.SimpleDateFormat
import java.util.Calendar
import java.io.File
import java.io.FilenameFilter

/**
 * 将txt类型的文件中的数据存到hbase表中
 */
object FileToHbase {
  def main(args: Array[String]): Unit = {

    println("Start!")

    // 时间
    val calendar = Calendar.getInstance
    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm00")
    val endTime = simpleDateFormat.format(calendar.getTime)
    //val endTime = "2018-10-10 10:20:00"

    val file = new File("F:/java/mysql")
    val files = file.list(new FilenameFilter() {
      def accept(dir: File, name: String): Boolean = {
        if (name.startsWith("")) {
          return true
        }
        return false
      }
    })

    // 创建SparkSession
    val spark = SparkSession
      .builder()
      .appName("FileToHbase")
      .master("local")
      .config("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    // 表的字段名
    val center_fields = "lineName abbreviation projectName description eneryMgr".split(" ")
    val line_fields = "stationName abbreviation lineName description area eneryMgr transferstation".split(" ")
    val point_fields = "lineName stationName pointName type controlID equipmentID equipmentName dataType statics unit quantity lowerLimitValue upperLimitValue description".split(" ")
    val user_fields = "id name password age".split(" ")

    // 改变列名
    //df_csv.withColumnRenamed("_c0", "id").show()
    /*
    // 读取文件为DateFrame
    val df_csv = spark.read.csv("F:/java/mysql/user.csv").toDF("id","name","password","age")
    df_csv.show()
    df_csv.select("name", "age").show()
    val rdd = df_csv.select("id","name","password", "age").rdd
      .map(row => (row(0).asInstanceOf[Int],row(1).asInstanceOf[String],row(2).asInstanceOf[String],row(3).asInstanceOf[Int]))
    */

    var rowId: Int = 0
    // 将 *RDD[(id:Int, name:String, password:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
    def center_convert(fields: Array[String]) = {
      val p = new Put(Bytes.toBytes(rowId))
      rowId += 1
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(center_fields(0)), Bytes.toBytes(fields(0)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(center_fields(1)), Bytes.toBytes(fields(1)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(center_fields(2)), Bytes.toBytes(fields(2)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(center_fields(3)), Bytes.toBytes(fields(3)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(center_fields(4)), Bytes.toBytes(fields(4).toInt))
      (new ImmutableBytesWritable, p)
    }
    // 将 *RDD[(id:Int, name:String, password:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
    def line_convert(fields: Array[String]) = {
      val p = new Put(Bytes.toBytes(rowId))
      rowId += 1
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(line_fields(0)), Bytes.toBytes(fields(0)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(line_fields(1)), Bytes.toBytes(fields(1)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(line_fields(2)), Bytes.toBytes(fields(2)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(line_fields(3)), Bytes.toBytes(fields(3)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(line_fields(4)), Bytes.toBytes(fields(4).toDouble))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(line_fields(5)), Bytes.toBytes(fields(5).toInt))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(line_fields(6)), Bytes.toBytes(fields(6).toInt))
      (new ImmutableBytesWritable, p)
    }
    // 将 *RDD[(id:Int, name:String, password:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
    def point_convert(fields: Array[String]) = {
      // 随机生成rowket
      val p = new Put(Bytes.toBytes(rowId))
      rowId += 1
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(0)), Bytes.toBytes(fields(0)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(1)), Bytes.toBytes(fields(1)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(2)), Bytes.toBytes(fields(2)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(3)), Bytes.toBytes(fields(3)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(4)), Bytes.toBytes(fields(4).toInt))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(5)), Bytes.toBytes(fields(5)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(6)), Bytes.toBytes(fields(6)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(7)), Bytes.toBytes(fields(7)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(8)), Bytes.toBytes(fields(8).toInt))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(9)), Bytes.toBytes(fields(9)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(10)), Bytes.toBytes(fields(10).toInt))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(11)), Bytes.toBytes(fields(11).toDouble))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(12)), Bytes.toBytes(fields(12).toDouble))
      if (fields.length > 13)
        p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(13)), Bytes.toBytes(fields(13)))
      (new ImmutableBytesWritable, p)
    }
    // 将 *RDD[(id:Int, name:String, password:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
    def hisdataYC_convert(fields: Array[String]) = {
      val p = new Put(Bytes.toBytes(""))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes("TIME"), Bytes.toBytes(fields(0)))
      (new ImmutableBytesWritable, p)
    }

    // 将 *RDD[(id:Int, name:String, password:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
    def user_convert(fields: Array[String]) = {
      val p = new Put(Bytes.toBytes(fields(0)))
      p.addColumn(Bytes.toBytes("f"), Bytes.toBytes(user_fields(1)), Bytes.toBytes(fields(1)))
      p.addColumn(Bytes.toBytes("f"), Bytes.toBytes(user_fields(2)), Bytes.toBytes(fields(2)))
      p.addColumn(Bytes.toBytes("f"), Bytes.toBytes(user_fields(3)), Bytes.toBytes(fields(3).toInt))
      (new ImmutableBytesWritable, p)
    }

    // 定义Hbase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "stest")
    // 指定输出格式和输出表名
    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    for (f <- files) {
      println(f)
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, f.dropRight(4))
      // 读取文件转化为RDD
      val sc = spark.sparkContext
      val rdd = sc.textFile(s"F:/java/mysql/${f}").map(line => {
        line.split("\t")
      })
      rowId = 0
      if (f.dropRight(4) == "center") {
        val resultRdd = rdd.map(center_convert)
        resultRdd.saveAsHadoopDataset(jobConf)
      } else if (f.dropRight(4) == "line") {
        val resultRdd = rdd.map(line_convert)
        resultRdd.saveAsHadoopDataset(jobConf)
      } else if (f.dropRight(4) == "point") {
        val resultRdd = rdd.map(point_convert)
        resultRdd.saveAsHadoopDataset(jobConf)
      }

    }

    println("Done!")

  }
}