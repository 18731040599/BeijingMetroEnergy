package com.thtf.sparksql

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Dataset
import org.apache.hadoop.hbase.spark.HBaseContext

/**
 * Hortonworks的spark on hbase方式读取hbase。
 * spark2.0 API
 * hbase版本不兼容。
 */
object HbaseToHbase {
  val spark = SparkSession
    .builder()
    .appName("hbasetohbase")
    // 本地测试
    .master("local")
    .config("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")
    .getOrCreate()
  import spark.implicits._
  // 配置Hbase
  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("hbase.zookeeper.quorum", "stest")
  // 配置HbaseContext
  val hbaseContext = new HBaseContext(spark.sparkContext, conf)
  // 建立一个数据库的连接
  val conn = ConnectionFactory.createConnection(conf);
  // 创建一个扫描对象
  val scan = new Scan

  // 各列对应的数据类型
  val columns_type = Map[String, String](
    "id" -> "string",
    "name" -> "string",
    "password" -> "string",
    "age" -> "int")

  def main(args: Array[String]): Unit = {

    println("Start!")

    val user = "user"
    val point = "point"
    val newuser = "newuser"

    val tablename = newuser

    val hAdmin = conn.getAdmin
    if (tablename.equals("")) {
      println("Table name can not be null !")
      return
    }
    if (!hAdmin.tableExists(TableName.valueOf(tablename))) {
      println(tablename + " is not exist");
      return
    }
    val userCatalog = tableCatalog(tablename)
    // 读取hbase表为DataFrame
    val df = spark.sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog -> userCatalog))
      .format("org.apache.hadoop.hbase.spark")
      .load()

    //df.show()
    df.createOrReplaceTempView("user")
    spark.sql("select * from user").show()
    spark.sql("select age from user where age > 26").show()
    //df.printSchema()
    //df.describe()
    //df.select($"name", $"age" + 1).show()
    
    //df.groupBy("age").count().show()
    //println(df.collect().length)
    //df.filter($"age" > 21).show()
    //df.filter(conditionExpr = "age>25").show()
    
    

    /*
    // 写入hbase表
    val newuserCatalog = tableCatalog(table_newuser)
    df.write
      .mode(SaveMode.Append)
      .options(Map(HBaseTableCatalog.tableCatalog -> newuserCatalog))
      .format("org.apache.hadoop.hbase.spark")
      .save()
    */

    println("Done!")
    
    // 关闭资源
    conn.close()
    spark.stop()

  }

  // 生成表的catalog
  def tableCatalog(tablename: String) = {
    // 获取表
    val table = conn.getTable(TableName.valueOf(tablename))
    // 扫描全表输出结果
    val results = table.getScanner(scan)
    val it: java.util.Iterator[Result] = results.iterator()
    // 列族和列的set
    var resultSet = Set[String]()
    while (it.hasNext) {
      val result = it.next()
      val cells = result.rawCells()
      for (cell <- cells) {
        var str = Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell))
        resultSet += str
      }
    }
    var columnsDes = handleSet(resultSet)
    // 创建表的scheme
    def catalog = s"""{
                      |"table":{"namespace":"default", "name":"${tablename}"},
                      |"rowkey":"key",
                      |"columns":{
                      |"rowkey":{"cf":"rowkey", "col":"key", "type":"${columns_type.getOrElse("id","string")}"},
                      |${columnsDes}
                      |}
                      |}""".stripMargin
     // 关闭资源
    results.close()
    table.close()
    
    catalog
  }

  // 生成所有列scheme的方法
  def handleSet(rs: Set[String]): String = {
    var str = ""
    rs.foreach(r => {
      val arr = r.split(":", 2)
      //str += "\"" + r +"\":{\"cf\":\"" + arr(0) + "\", \"col\":\"" + arr(1) + "\", \"type\":\"string\"},"
      str += "\"" + arr(1) + "\":{\"cf\":\"" + arr(0) + "\", \"col\":\"" + arr(1) + "\", \"type\":\"" + columns_type.getOrElse(arr(1), "string") + "\"},"
    })
    // TODO 表中没有数据时，str为空，会报错
    str.substring(0, str.length - 1)
  }

}