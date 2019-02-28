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
import java.util.{ Calendar, Properties }
import java.io.{ File, FilenameFilter, FileInputStream }
import com.thtf.entity.DateEntity

/**
 * 将txt类型的文件中的数据存到hbase表中
 */
object FileToHbase {
  def main(args: Array[String]): Unit = {

    // 时间
    val endTime = new DateEntity().getCurrentTime

    // 读取配置文件
    val properties = new Properties
    try {
      properties.load(new FileInputStream(new File("defaults.properties")))
    } catch {
      case t: Throwable => t.printStackTrace()
      println("Error reading default configuration file!")
    }
    // /opt/modules/test/data/
    // F:/java/mysql/
    var filepath = "/opt/modules/test/data/"
    if (!filepath.endsWith("/")) {
      filepath += "/"
    }
    val file = new File(filepath)
    if (!file.isDirectory()) {
      throw new RuntimeException(filepath + ": No such directory")
    }
    val files = file.list(new FilenameFilter() {
      def accept(dir: File, name: String): Boolean = {
        if (name.startsWith(endTime)) {
          return true
        }
        return false
      }
    })

    if (files.length == 0) {
      println("There is no matching file")
      return
    }

    // 创建SparkSession
    val spark = SparkSession
      .builder()
      .appName("FileToHbase")
      //.master("local")
      //.config("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    // 表的字段名
    val center_fields = "lineName abbreviation projectName description eneryMgr".split(" ")
    val line_fields = "stationName abbreviation lineName description area eneryMgr transferstation".split(" ")
    val point_fields = "lineName stationName pointName type controlID equipmentID equipmentName dataType statics unit quantity lowerLimitValue upperLimitValue description".split(" ")
    val tables = Map(
      "center" -> center_fields,
      "line" -> line_fields,
      "point" -> point_fields)

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

    // 定义Hbase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "stest")
    // 指定输出格式和输出表名
    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    // 方案二：全部存String类型的数据
    filepath = "file://" + filepath
    files.foreach(f => {
      val fs = f.split("_")
      if (fs.length == 2) {
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, fs(1).dropRight(4))
        // 读取文件转化为RDD
        val sc = spark.sparkContext
        val rdd = sc.textFile(filepath + f).map(line => {
          line.split("\t")
        })
        if (fs(1).dropRight(4) == "point") {
          val resultRdd = rdd.map(row => {
            val p = new Put(Bytes.toBytes(row(1) + row(3) + row(4)))
            for (i <- 0 to row.length - 1) {
              p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(tables.getOrElse(fs(1).dropRight(4), null)(i)), Bytes.toBytes(row(i)))
            }
            (new ImmutableBytesWritable, p)
          })
          resultRdd.saveAsHadoopDataset(jobConf)
        } else {
          val resultRdd = rdd.map(row => {
            val p = new Put(Bytes.toBytes(row(1)))
            for (i <- 0 to row.length - 1) {
              p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(tables.getOrElse(fs(1).dropRight(4), null)(i)), Bytes.toBytes(row(i)))
            }
            (new ImmutableBytesWritable, p)
          })
          resultRdd.saveAsHadoopDataset(jobConf)
        }
      } else if (fs.length == 4) {
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "hisdataYX")
        // 读取文件转化为RDD
        val sc = spark.sparkContext
        val rdd = sc.textFile(filepath + f).map(line => {
          line.split("\t")
        })
        val resultRdd = rdd.map(row => {
          val p = new Put(Bytes.toBytes(fs(3).dropRight(4) + row(0) + row(1).replaceAll("[^0-9]", "")))
          p.addColumn(Bytes.toBytes("c"), Bytes.toBytes("value"), Bytes.toBytes(row(2)))
          (new ImmutableBytesWritable, p)
        })
        resultRdd.saveAsHadoopDataset(jobConf)
      } else if (fs.length == 5) {
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "hisdataYC")
        // 读取文件转化为RDD
        val sc = spark.sparkContext
        val rdd = sc.textFile(filepath + f).map(line => {
          line.split("\t")
        })
        val resultRdd = rdd.map(row => {
          val p = new Put(Bytes.toBytes(fs(3) + row(0).replaceAll("[^0-9]", "")))
          val columns = (fs(4).dropRight(4).toInt - 1).*(200)
          for (i <- 1 to row.length - 1) {
            p.addColumn(Bytes.toBytes("c"), Bytes.toBytes((columns + i - 1).toString()), Bytes.toBytes(row(i)))
          }
          (new ImmutableBytesWritable, p)
        })
        resultRdd.saveAsHadoopDataset(jobConf)
      }
    })

    /*
    // 方案一：存入对应类型的数据
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

    // 定义Hbase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "stest")
    // 指定输出格式和输出表名
    val jobConf = new JobConf(conf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    for (f <- files) {
      val fs = f.split("_")
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
    */

    println("Done!")

  }
}