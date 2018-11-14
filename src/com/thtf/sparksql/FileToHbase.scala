package com.thtf.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import scala.util.Random

case class User(
  id : Long,
  name : String,
  password : String,
  age : Long
)
/**
 * 将txt类型的文件中的数据存到hbase表中
 */
object FileToHbase {
  def main(args: Array[String]): Unit = {
    
    // 创建SparkSession
    val spark = SparkSession
      .builder()
      .appName("FileToHbase")
      .master("local")
      .config("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")
      .getOrCreate()
      
    import spark.implicits._
    
    println("Start!")
    // 读取文件转化为RDD
    val sc = spark.sparkContext
    val rdd = sc.textFile("F:/java/mysql/user.txt").map(line => {
      line.split("\t")
    })
    
    //rdd.map(arr => arr(13)).saveAsTextFile("F:/java/mysql/newPoint.txt")
    //println("Over! ~~~~~~")
    
    // point表的字段名
    val point_fields = "lineName stationName pointName type controlID equipmentID equipmentName dataType statics unit quantity lowerLimitValue upperLimitValue description"
      .split(" ")
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
    
    // 定义Hbase的配置
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "stest")
    
    // 指定输出格式和输出表名
    val jobConf = new JobConf(conf,this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "newuser")
    
    
    /*
    // 将 *RDD[(id:Int, name:String, password:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
    def convert(quadruple:Array[String]) = {
      // 随机生成rowket
      val row_id = (1000+(new Random).nextInt(1000))+""+(1000+(new Random).nextInt(1000))+""+(1000+(new Random).nextInt(1000))
      val p = new Put(Bytes.toBytes(row_id))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(0)), Bytes.toBytes(quadruple(0)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(1)), Bytes.toBytes(quadruple(1)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(2)), Bytes.toBytes(quadruple(2)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(3)), Bytes.toBytes(quadruple(3)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(4)), Bytes.toBytes(quadruple(4)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(5)), Bytes.toBytes(quadruple(5)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(6)), Bytes.toBytes(quadruple(6)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(7)), Bytes.toBytes(quadruple(7)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(8)), Bytes.toBytes(quadruple(8)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(9)), Bytes.toBytes(quadruple(9)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(10)), Bytes.toBytes(quadruple(10)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(11)), Bytes.toBytes(quadruple(11)))
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(12)), Bytes.toBytes(quadruple(12)))
      if(quadruple.length > 13)
      p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(point_fields(13)), Bytes.toBytes(quadruple(13)))
      (new ImmutableBytesWritable,p)
    }
    */
    
    // 将 *RDD[(id:Int, name:String, password:String, age:Int)]* 转换成 *RDD[(ImmutableBytesWritable, Put)]*
    def convert(quadruple:Array[String]) = {
    		// 随机生成rowket
    				val p = new Put(Bytes.toBytes(quadruple(0)))
    				p.addColumn(Bytes.toBytes("f"), Bytes.toBytes(user_fields(1)), Bytes.toBytes(quadruple(1)))
    				p.addColumn(Bytes.toBytes("f"), Bytes.toBytes(user_fields(2)), Bytes.toBytes(quadruple(2)))
    				p.addColumn(Bytes.toBytes("f"), Bytes.toBytes(user_fields(3)), Bytes.toBytes(quadruple(3).toInt))
    				(new ImmutableBytesWritable,p)
    }
    
    
    val resultRdd = rdd.map(convert)
    
    resultRdd.saveAsHadoopDataset(jobConf)
    println("Done!")
    
  }
}