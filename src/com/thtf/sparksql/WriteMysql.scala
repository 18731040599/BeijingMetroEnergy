package com.thtf.sparksql

import org.apache.spark.sql.{ SQLContext, SaveMode }
import java.util.Properties
import org.apache.spark.sql.SparkSession
import scala.util.Random
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.collection.mutable.ListBuffer

/**
 *
 */

case class resultset(
  point: String,
  time: Int,
  value: Float)

object WriteMysql {
	val spark = SparkSession
			.builder()
			.appName("write to mysql")
			.master("local")
			.getOrCreate()
	import spark.implicits._

  def main(args: Array[String]): Unit = {

    //定义数据库和表信息
    //"jdbc:mysql://localhost:3306/my_test_db01?useUnicode=true&characterEncoding=UTF-8"
    val url = "jdbc:mysql://localhost:3306/my_test_db01?useSSL=true"
    val table = "test"
    //写MySQL的方法1
    /*val list = List(
      resultset(11, "名字1", "标题1", "简介1"),
      resultset(22, "名字2", "标题2", "简介2"),
      resultset(33, "名字3", "标题3", "简介3"),
      resultset(44, "名字4", "标题4", "简介4"))*/
    var list = ListBuffer[resultset]()
    for(i <- 10010 to 10010){
      list = list.+:(resultset(i.toString(),i+3990,Random.nextFloat().*(Random.nextInt(100))))
    }
    val listRDD = spark.sparkContext.parallelize(list)
    val map = listRDD.map(x => Row(x.point,x.time,x.value))
    val schema = StructType(List(StructField("point",StringType,true),StructField("time",IntegerType,true),StructField("value",FloatType,true)))
    val jdbcDF = spark.createDataFrame(map,schema)
    jdbcDF.collect().take(20).foreach(println)
    //jdbcDF.rdd.saveAsTextFile("/home/mi/coding/coding/Scala/spark-hbase/output")
    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    //jdbcDF.write.mode(SaveMode.Overwrite).jdbc(url,table,prop)
    jdbcDF.write.mode(SaveMode.Append).jdbc(url, table, prop)
    
    
    /*
    //写入MySQL
    val userRDD = spark.sparkContext.parallelize(Array("13 jack 666888 27", "24 wade 999999 50")).map(x => x.split(" "))
    val ROWRDD = userRDD.map(x => Row(x(0).toInt, x(1).trim, x(2).trim, x(3).toInt))
    ROWRDD.foreach(print)
    //设置模式信息
    val schema = StructType(List(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("password", StringType, true), StructField("age", IntegerType, true)))

    val userDF = spark.createDataFrame(ROWRDD, schema)

    val parameter = new Properties()
    parameter.put("user", "****")
    parameter.put("password", "****")
    parameter.put("driver", "com.mysql.jdbc.Driver")
    userDF.write.mode("append").jdbc("jdbc:mysql://localhost:3306/my_test_db01", table, parameter)
    df.show()
    */

  }
}