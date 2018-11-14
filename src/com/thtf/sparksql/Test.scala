package com.thtf.sparksql

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{ HBaseConfiguration, HColumnDescriptor, HTableDescriptor }
import org.apache.hadoop.hbase.client.{ HBaseAdmin, HTable, Put }
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.datasources.hbase._
import org.apache.hadoop.hbase.spark.datasources.HBaseScanPartition
import org.apache.hadoop.hbase.util.Bytes
/*
case class HBaseRecord(
  col0: String,
  col1: Int)

object HBaseRecord {
  def apply(i: Int, t: Int): HBaseRecord = {
    val s = s"""row${"%03d".format(i)}"""
    HBaseRecord(s,
      i)
  }
}
*/
/**
 * Hortonworks的spark on hbase方式读取hbase。
 * spark 1.* API
 * hbase版本不兼容。
 */
object Test {
  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("test spark sql")
      //.setMaster("local")
      //.set("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")

    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("hbase.zookeeper.quorum", "stest")
    
    //config.addResource("/home/hadoop/hbase-1.2.2/conf/hbase-site.xml");
    //config.set("hbase.zookeeper.quorum", "node1,node2,node3");
    
    val hbaseContext = new HBaseContext(sc, conf, null)

    def catalog = s"""{
       |"table":{"namespace":"default", "name":"newuser"},
       |"rowkey":"key",
       |"columns":{
         |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
         |"age":{"cf":"f", "col":"age", "type":"int"},
         |"name":{"cf":"f", "col":"name", "type":"string"},
         |"password":{"cf":"f", "col":"password", "type":"string"}
       |}
     |}""".stripMargin

    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._

    def withCatalog(cat: String): DataFrame = {
      sqlContext
        .read
        .options(Map(HBaseTableCatalog.tableCatalog -> cat))
        .format("org.apache.hadoop.hbase.spark")
        .load()
    }
    val df = withCatalog(catalog)
    
    df.createOrReplaceTempView("newuser")
    sqlContext.sql("select * from newuser").show()
    df.filter($"age" > 26).show()
    
    //sqlContext.sql("select * from newuser where age > 26").show()
    //sqlContext.sql("select count(rowkey),sum(age) from newuser where age>'20' and age<'26' ").show
    //sqlContext.sql("select count(age),avg(age) from newuser").show
  }
}