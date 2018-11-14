package com.thtf.sparksql

import java.util.Properties
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar

object ReadMysql {
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local")
    .config("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._
  def main(args: Array[String]): Unit = {

    //定义数据库和表信息
    //"jdbc:mysql://localhost:3306/my_test_db01?useUnicode=true&characterEncoding=utf8&zeroDateTimeBehavior=convertToNull"
    
    val user = "user"
    val teacher = "teacher"
    val test = "test"
    
    val options = Map(
      "url" -> "jdbc:mysql://localhost:3306/my_test_db01",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "root")

    //读MySQL的方法1
    val reader = spark.read.format("jdbc").options(options)
    
    reader.option("dbtable", user)
    val uer_df = reader.load()
    uer_df.createOrReplaceTempView("user")
    spark.sql("select * from user where age > 26").show()
    
    reader.option("dbtable", teacher)
    val teacher_df = reader.load()
    teacher_df.show()
    spark.sql("""select * from user 
                    where age > 20""").foreach(row => {
      val line = row.toString()
      println(line)
      println("foreach")
    })
    
    reader.option("dbtable", test).load().filter(conditionExpr = "20010 >= time").filter($"time" >= 20000).show()
    reader.option("dbtable", test).load().filter($"time" === 20000 || $"time" === 20010).show()
    
    
    
    println(spark.sql("select count(*) from user").take(1)(0).get(0))
    var YC_ids = ""
    spark.sql("select * from user").take(4).map(Row=>{
      YC_ids += ("`"+Row.get(0).toString()+"`,")
    })
    println(YC_ids)
    YC_ids = YC_ids.dropRight(1)
    println(YC_ids)
    //println(YC_ids.dropRight(1))
    println(YC_ids.replaceAll("`", ""))
    //println(spark.sql("select * from user where age > 100").take(1)(0).get(0))
    
    //println("修改临时表")
    //spark.sql("insert into people values('jack',18)")//Inserting into an RDD-based table is not allowed.;
    //spark.sql("update people set age = 18 where name = 'tom'").show()//不支持
    //println("创建表")
    //spark.sql("""create table man(name string, age int)""").show()
    
    
    //println(spark.sql("select age from user where age > 26").take(1)(0).get(0))

    //uer_df.write.save("d:\\test")
    //uer_df.write.mode("append")csv("f:/java/csvtest")

    //uer_df.createGlobalTempView("student")
    //val sqldf = spark.sql("select * from global_temp.student").show()
    
    // 计算冷机(d5)的瞬时指标
    def getCOP(){}
    // 获取冷机运行状态
    

    // 计算KPI指标二十分钟统计数据并写到数据库表
    def proc_statistics_kpi_hour(start: String, end: String) {
      var startTime = start
      var endTime = end
      var done: Int = 0
      var done1: Int = 0
      var done2: Int = 0
      var done3: Int = 0
      var done4: Int = 0
      var done5: Int = 0
      var done6: Int = 0
      var lName: String = ""
      var lineAbb: String = ""
      var STName: String = ""
      var stationAbb: String = ""
      var tableName: String = ""
      var strCol: String = ""
      var strSql: String = ""
      var strSql2: String = ""
      var dt: Date = null
      var i: Int = 0
      var j: Int = 0
      var k: Int = 0
      // 统计表中总字段数
      var nColCount: Int = 0
      var nRowCount: Int = 0
      var bStart: Int = 0
      var vall: Double = 0
      // 如果输入的两个参数都为0，则需要获取统计开始和结束时间，结束时间在开始时间之后二十分钟
      if((startTime == "0000-00-00 00:00:00")&&(endTime == "0000-00-00 00:00:00")){
        val calendar = Calendar.getInstance
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:00")
        endTime = simpleDateFormat.format(calendar.getTime)
        calendar.add(Calendar.MINUTE, -20)
        startTime = simpleDateFormat.format(calendar.getTime)
      }
      // 从kpi_point中获取统计表中总列数
      nColCount = spark.sql("select count(*) from kpi_point").take(1)(0).get(0).asInstanceOf
      val centers = spark.sql("select lineName, abbreviation from center where eneryMgr = 1 and lineName='八号线'")
        .collect()
      for(center <- centers){
        lName = center.get(0).asInstanceOf
        lineAbb = center.get(1).asInstanceOf
        // 创建小时统计表
        tableName = "statistics_kpi_hour_" + lineAbb
        // TODO 创建表
        // 获得车站的名称
        val lines = spark.sql(s"SELECT stationName FROM line WHERE lineName = ${lName}").collect()
        for(line <- lines){
          STName = line.get(0).asInstanceOf
          // 获取指定时间段内kpi_day_target表中的kpi指标，然后取平均值，需要逐字段计算
          // 第一个字段是`all`
          val sum_all = spark.sql(s"""SELECT SUM(`all`) FROM kpi_day_target 
                                        WHERE `stationName` = '${STName}' 
                                          AND `lineName` = '${lName}' 
                                          AND `KPI_time` >= '${startTime}' 
                                          AND `KPI_time` < '${endTime}' 
                                          AND `all` >= 0 AND `all` <= 100""").collect()
          vall = sum_all.take(1)(0).get(0).asInstanceOf
          if (vall >= 0 && vall <= 300){
            // TODO 写入表
          }else if(vall < 0 || vall > 300){
        	  // TODO 写入表
          }
          if (vall != null){
            // 从d0开始需要循环获取列名
            i = 0
            while (i < nColCount) {
              strCol = "d"+i
              val sum_strCol = spark.sql(s"""SELECT SUM(`${strCol}`) FROM kpi_day_target 
                                                WHERE `stationName` = '${STName}' 
                                                AND `lineName` = '${lName}' 
                                                AND `KPI_time` >= '${startTime}' 
                                                AND `KPI_time` < '${endTime}' 
                                                AND `${strCol}` >= 0 
                                                AND `${strCol}` <= 100""").collect()
              vall = sum_strCol.take(1)(0).get(0).asInstanceOf
              // TODO 更新表
              // TODO 更新表
              i = i + 1
            }
          }
        }
        // 计算全线KPI
        // 第一个字段是`all`
        val sum_allAndCount = spark.sql(s"""SELECT SUM(`all`), COUNT(*) INTO @val, @nRow FROM ${tableName} 
                                          WHERE `staticsticsTime` = '${startTime}'""").collect()
        vall = sum_allAndCount.take(1)(0).get(0).asInstanceOf
        nRowCount = sum_allAndCount.take(1)(0).get(1).asInstanceOf
        if(nRowCount > 0){
          // TODO 写入表
          // 从d0开始需要循环获取列名，计算总行数时排除all那一行
          i = 0
          while (i < nColCount) {
            strCol = "d"+i
            val sum_strColAndCount = spark.sql(s"""SELECT SUM(`${strCol}`), COUNT(*) FROM ${tableName} 
                                                      WHERE `staticsticsTime` = '${startTime}' 
                                                      AND `stationName` <> 'all'""").collect()
            vall = sum_strColAndCount.take(1)(0).get(0).asInstanceOf
            nRowCount = sum_strColAndCount.take(1)(0).get(1).asInstanceOf
            // TODO 更新表
            i = i + 1
          }
        }
      }
    }

    /*
    //读MySQL的方法2
    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map(
        "url" -> "jdbc:mysql://localhost:3306/baidubaike?useUnicode=true&characterEncoding=UTF-8",
        "dbtable" -> "(select name,info,summary from baike_pages) as some_alias",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> "root",
        //"partitionColumn"->"day_id",
        "lowerBound" -> "0",
        "upperBound" -> "1000",
        //"numPartitions"->"2",
        "fetchSize" -> "100",
        "password" -> "root")).load()
    jdbcDF.show()
    */

    /*
    //读MySQL的方法3
    val properties = new Properties()
    properties.put("user", "feigu")
    properties.put("password", "feigu")
    val stud_scoreDF = sqlContext.read.jdbc(url, "stud_score", properties)
    stud_scoreDF.show()
    */

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