package com.thtf.sparksql

import java.util.{ Properties, Calendar }
import org.apache.spark.sql.{ SparkSession, Row, functions }
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.io.{ FileInputStream, File }
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.util.{ Base64, Bytes }
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.ColumnName
import org.slf4j.LoggerFactory
import com.thtf.entity.DateEntity
import com.thtf.entity.RegexEntity

object ProcessingHisdata {
  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(ProcessingHisdata.getClass)
    println("Start!")

    // 时间
    val dateEntity = new DateEntity
    val endTime = dateEntity.getCurrentTime
    val startTime = dateEntity.getStartTime
    val lastDay = dateEntity.getYesterday

    // 读取默认配置文件
    val defaults = new Properties
    try {
      defaults.load(new FileInputStream(new File("defaults.properties")))
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        println("Error reading default configuration file!")
        return
    }

    // 从外部配置文件中读取需要变更的参数
    val settings = new Properties
    try {
      settings.load(new FileInputStream(new File("settings.properties")))
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        println("Error reading settings configuration file!")
    }

    
    // TODO 包装成对象，方法里边直接传入对象
    // 常量参数
    val water_specificHeat: Double = if(settings.getProperty("water_specificHeat") == null){
      defaults.getProperty("water_specificHeat").toDouble
    }else{settings.getProperty("water_specificHeat").toDouble}
    
    val water_density: Double = if(settings.getProperty("water_density") == null){
      defaults.getProperty("water_density").toDouble
    }else{settings.getProperty("water_density").toDouble}
   
    val air_specificHeat: Double = if(settings.getProperty("air_specificHeat") == null){
    	defaults.getProperty("air_specificHeat").toDouble
    }else{settings.getProperty("air_specificHeat").toDouble}
    
    val air_density: Double = if(settings.getProperty("air_density") == null){
    	defaults.getProperty("air_density").toDouble
    }else{settings.getProperty("air_density").toDouble}
    
    // 设定参数
    val run_time: Double = if(settings.getProperty("run_time") == null){
    	defaults.getProperty("run_time").toDouble
    }else{settings.getProperty("run_time").toDouble} // 系统设置值
    
    val KT_F: Double = if(settings.getProperty("KT_F") == null){
    	defaults.getProperty("KT_F").toDouble
    }else{settings.getProperty("KT_F").toDouble} // 系统设定值
    
    val HPF_F: Double = if(settings.getProperty("HPF_F") == null){
    	defaults.getProperty("HPF_F").toDouble
    }else{settings.getProperty("HPF_F").toDouble} // 系统设定值
    
    // 指标标准
    val LSJZ_standard: Double = if(settings.getProperty("LSJZ_standard") == null){
    	defaults.getProperty("LSJZ_standard").toDouble
    }else{settings.getProperty("LSJZ_standard").toDouble}
    
    val LD_standard: Double = if(settings.getProperty("LD_standard") == null){
    	defaults.getProperty("LD_standard").toDouble
    }else{settings.getProperty("LD_standard").toDouble}
    
    val LQ_standard: Double = if(settings.getProperty("LQ_standard") == null){
    	defaults.getProperty("LQ_standard").toDouble
    }else{settings.getProperty("LQ_standard").toDouble}
    
    val LT_standard: Double = if(settings.getProperty("LT_standard") == null){
    	defaults.getProperty("LT_standard").toDouble
    }else{settings.getProperty("LT_standard").toDouble}
    
    val DKT_standard: Double = if(settings.getProperty("DKT_standard") == null){
    	defaults.getProperty("DKT_standard").toDouble
    }else{settings.getProperty("DKT_standard").toDouble}
    
    val LL_standard: Double = if(settings.getProperty("LL_standard") == null){
    	defaults.getProperty("LL_standard").toDouble
    }else{settings.getProperty("LL_standard").toDouble}
    
    val power_standard: Double = if(settings.getProperty("power_standard") == null){
    	defaults.getProperty("power_standard").toDouble
    }else{settings.getProperty("power_standard").toDouble}
    
    val XL_standard: Double = if(settings.getProperty("XL_standard") == null){
    	defaults.getProperty("XL_standard").toDouble
    }else{settings.getProperty("XL_standard").toDouble}
    
    val HFT_standard: Double = if(settings.getProperty("HFT_standard") == null){
    	defaults.getProperty("HFT_standard").toDouble
    }else{settings.getProperty("HFT_standard").toDouble}

    // 创建正则对象
    val regexEntity = new RegexEntity

    // 创建sparksession
    val spark = SparkSession
      .builder()
      .appName("ProcessingHisdata")
      //.master("local")
      //.config("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // 静态表名
    val point = defaults.getProperty("point")
    val center = defaults.getProperty("center")
    val line = defaults.getProperty("line")
    // 历史数据表名
    val hisdataYC = defaults.getProperty("hisdataYC")
    val hisdataYX = defaults.getProperty("hisdataYX")
    // 状态记录表名
    val hisstatus = defaults.getProperty("hisstatus")
    // KPI表名
    val KPI_real = defaults.getProperty("KPI_real")
    val KPI_day = defaults.getProperty("KPI_day")

    // KPI表的字段名
    val KPI_fields = ArrayBuffer[String]("abbreviation", "time", "d1", "d2", "d3", "d4", "d5", "d10", "d15", "d20", "d24")

    // 表的字段名
    val center_fields = "lineName abbreviation projectName description eneryMgr".split(" ")
    val line_fields = "stationName abbreviation lineName description area eneryMgr transferstation".split(" ")
    val point_fields = "lineName stationName pointName type controlID equipmentID equipmentName dataType statics unit quantity lowerLimitValue upperLimitValue description".split(" ")
    val tables = Map(
      "center" -> center_fields,
      "line" -> line_fields,
      "point" -> point_fields)

    // =================== 将读取hbase，将rdd转换为dataframe ===================
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
      spark.createDataFrame(tableRDD, StructType(fields)).cache()
    }
    // ========================================

    // =================== 将读取hbase，将rdd转换为dataframe ===================
    def hisdataYCToDF(tablename: String) = {
      // 定义Hbase的配置
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.zookeeper.quorum", "stest")
      // 直接从 HBase 中读取数据并转成 Spark 能直接操作的 RDD[K,V]
      conf.set(TableInputFormat.INPUT_TABLE, tablename)
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
      spark.createDataFrame(tableRDD, StructType(fields)).cache()
    }
    // ========================================

    // =================== 将读取hbase，将rdd转换为dataframe ===================
    def hisdataYXToDF(tablename: String) = {
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
          val str = Bytes.toString(result.getRow)
          Row(str.replaceAll("[0-9]", ""), str.replaceAll("[^0-9]", "").takeRight(14), str.replaceAll("[^0-9]", "").dropRight(14), Bytes.toString(result.getValue("c".getBytes, "value".getBytes)))
        })
      // 以编程的方式指定 Schema，将RDD转化为DataFrame
      val schema = StructType(StructField("abbreviation", StringType, true) ::
        StructField("time", StringType, true) ::
        StructField("id", StringType, true) ::
        StructField("value", StringType, true) :: Nil)
      spark.createDataFrame(tableRDD, schema).filter($"time" > lastDay && $"time" < endTime).createTempView(tablename)
      // SELECT t1.* FROM `hisdata` t1 ,(SELECT MAX(TIME) AS mt,abbr,id FROM `hisdata` GROUP BY abbr,id) t2 WHERE t1.id=t2.id AND t1.time =t2.mt AND t1.abbr=t2.abbr;
      spark.sql(s"SELECT data.* FROM `${tablename}` data ,(SELECT MAX(time) AS mt,abbreviation,id FROM `${tablename}` GROUP BY abbreviation,id) t WHERE data.id=t.id AND data.time =t.mt AND data.abbreviation=t.abbreviation")
        .cache()
    }
    // ========================================

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
      spark.createDataFrame(tableRDD, StructType(schema)).filter($"time" > lastDay && $"time" <= endTime).cache()
    }
    // ========================================

    // =================== 将dataframe保存到hbase表 ===================
    def saveToHbase(tablename: String, KPI: ArrayBuffer[Array[String]]) {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set("hbase.zookeeper.quorum", "stest")
      // 定义jobconf的配置
      val jobConf = new JobConf(conf, this.getClass)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

      val KPIRdd_real = spark.sparkContext.parallelize(KPI).map(arr => {
        val p = new Put(Bytes.toBytes(arr(0)))
        val startnum = KPI_fields.length - (arr.length - 1)
        for (i <- 0 until arr.length - 1) {
          p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(KPI_fields(i + startnum)), Bytes.toBytes(arr(i + 1)))
        }
        (new ImmutableBytesWritable, p)
      })
      KPIRdd_real.saveAsHadoopDataset(jobConf)
    }
    /*def saveToHbase(tablename: String, KPI: Array[String]) {
    	val conf = HBaseConfiguration.create()
    			conf.set("hbase.zookeeper.property.clientPort", "2181")
    			conf.set("hbase.zookeeper.quorum", "stest")
    			// 定义jobconf的配置
    			val jobConf = new JobConf(conf, this.getClass)
    			jobConf.setOutputFormat(classOf[TableOutputFormat])
    			jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    			val p = new Put(Bytes.toBytes(KPI(0)))
    			val startnum = KPI_fields.length - (KPI.length - 1)
    			for (i <- 0 until KPI.length - 1) {
    				p.addColumn(Bytes.toBytes("c"), Bytes.toBytes(KPI_fields(i + startnum)), Bytes.toBytes(KPI(i + 1)))
    			}

    			val KPIRdd_real = spark.sparkContext.parallelize(Array(p))
    			KPIRdd_real.map(p => (new ImmutableBytesWritable,p)).saveAsHadoopDataset(jobConf)
    }*/
    // ========================================

    try {
    // 创建各个表临时视图
    val hisdataYC_DF = hisdataYCToDF(hisdataYC)
    if (hisdataYC_DF.count() == 0) {
      // 如果没有历史数据，直接结束程序
      println("There's no historical data!")
      return
    }
    val hisdataYX_DF = hisdataYXToDF(hisdataYX)
    val center_DF = finalHtableToDF(center)
    val point_DF = finalHtableToDF(point)
    val line_DF = finalHtableToDF(line)
    val KPI_real_DF = hisdataKPIToDF(KPI_real)
    val hisstatus_DF = hisdataKPIToDF(hisstatus)

    // 创建存放KPI的可变数组
    val KPIs_real: ArrayBuffer[Array[String]] = ArrayBuffer[Array[String]]()
    val KPIs_day: ArrayBuffer[Array[String]] = ArrayBuffer[Array[String]]()
    val KPIs_status: ArrayBuffer[Array[String]] = ArrayBuffer[Array[String]]()

    // 查询线路
    //val center_records = null
    val lines_df = center_DF.select("lineName", "abbreviation").filter($"eneryMgr" === 1)
    if (lines_df.count() == 0) {
      // return
    } else {
      for (line <- lines_df.collect()) {
        // 遍历线路
        val lineName = line.getString(0)
        val line_abbr = line.getString(1)
        // 查询车站
        val stations_df = line_DF.select("stationName", "abbreviation").filter($"lineName" === lineName && $"eneryMgr" === 1)
        if (stations_df.count() == 0) {
          // return
        } else {
          for (station <- stations_df.collect()) {
            // 遍历车站
            // 运行状态
            var SF_run_status: Double = 0
            // kpi瞬时指标分数
            var d1: Double = 0
            var d2: Double = 0
            var d3: Double = 0
            var d4: Double = 0
            var d5: Double = 0
            var d10: Double = 0
            var d15: Double = 0
            var d20: Double = 0
            var d24: Double = 0
            // 瞬时运行状态
            var d2_Status: Double = 0
            var d3_Status: Double = 0
            var d4_Status: Double = 0
            var d5_Status: Double = 0
            var d10_Status: Double = 0
            var d15_Status: Double = 0
            var d20_Status: Double = 0
            var d24_Status: Double = 0
            // 累计有效开机记录
            var d2_sumStatus: Double = 0
            var d3_sumStatus: Double = 0
            var d4_sumStatus: Double = 0
            var d5_sumStatus: Double = 0
            var d10_sumStatus: Double = 0
            var d15_sumStatus: Double = 0
            var d20_sumStatus: Double = 0
            var d24_sumStatus: Double = 0

            val stationName = station.getString(0)
            val station_abbr = station.getString(1)

            val sum_KPI = KPI_real_DF.filter($"abbreviation" === station_abbr)
              .agg("d2" -> "sum", "d3" -> "sum", "d4" -> "sum", "d5" -> "sum", "d10" -> "sum", "d15" -> "sum", "d20" -> "sum", "d24" -> "sum")
              .select("sum(d2)", "sum(d3)", "sum(d4)", "sum(d5)", "sum(d10)", "sum(d15)", "sum(d20)", "sum(d24)")
              .collect()
            val sum_status = hisstatus_DF.filter($"abbreviation" === station_abbr)
              .agg("d2" -> "sum", "d3" -> "sum", "d4" -> "sum", "d5" -> "sum", "d10" -> "sum", "d15" -> "sum", "d20" -> "sum", "d24" -> "sum")
              .select("sum(d2)", "sum(d3)", "sum(d4)", "sum(d5)", "sum(d10)", "sum(d15)", "sum(d20)", "sum(d24)")
              .collect()

            // 计算各项指标
            // 电耗d1
            val area = line_DF.select("area").filter($"lineName" === lineName && $"stationName" === stationName).collect()(0).getString(0).toDouble
            // 赋值24小时总能耗
            var IDs = selectByRegex(regexEntity.getALL_ZNDB_Regex, lineName, stationName)
            var IDAndValue = selectConsumptionData(IDs, lastDay, station_abbr)
            var consumption = assign_powerConsumption(IDAndValue)
            var result = getIndicatorPower(consumption, area)
            if (power_standard > result || result == 0) {
              d1 = 100
            } else {
              d1 = 100 - (result - power_standard)./(result).*(100)
            }
            // 赋值回风温度
            IDs = selectByRegex(regexEntity.getHF_T_Regex, lineName, stationName)
            IDAndValue = selectYCData(IDs, station_abbr)
            var HF_T = assign_instant(IDAndValue)
            // 赋值室内温度
            IDs = selectByRegex(regexEntity.getZTX_T_Regex, lineName, stationName)
            IDAndValue = selectYCData(IDs, station_abbr)
            var ZT_T = assign_instant(IDAndValue)
            IDs = selectByRegex(regexEntity.getZTS_T_Regex, lineName, stationName)
            IDAndValue = selectYCData(IDs, station_abbr)
            ZT_T = (ZT_T + assign_instant(IDAndValue))./(2)
            // 赋值送风机运行状态
            IDs = selectByRegex(regexEntity.getKT_Run_Regex, lineName, stationName)
            IDAndValue = selectYXData(IDs, station_abbr)
            SF_run_status = assign_runStatus(IDAndValue)
            // 赋值运行状态
            if (SF_run_status == 0) {
              // 获取排风机运行状态
              IDs = selectByRegex(regexEntity.getHPF_Run_Regex, lineName, stationName)
              IDAndValue = selectYXData(IDs, station_abbr)
              d2_Status = assign_runStatus(IDAndValue)
              d3_Status = d2_Status
              d24_Status = d2_Status
            } else {
              d2_Status = 1
              d3_Status = 1
              d24_Status = 1
            }
            d2_sumStatus = d2_Status + getKPIDay(sum_status, 0)
            d3_sumStatus = d3_Status + getKPIDay(sum_status, 1)
            d24_sumStatus = d24_Status + getKPIDay(sum_status, 7)
            if (d2_Status == 0) {
              d2 = 0
              d3 = 0
              d24 = 0
            } else {
              // 重新赋值20分钟总能耗
              IDAndValue = selectConsumptionData(IDs, startTime, station_abbr)
              consumption = assign_powerConsumption(IDAndValue)
              // 赋值送风机频率
              IDs = selectByRegex(regexEntity.getKT_Hz_Regex, lineName, stationName)
              IDAndValue = selectYCData(IDs, station_abbr)
              var KT_Hz = assign_instant(IDAndValue)
              // 赋值回排风机频率
              IDs = selectByRegex(regexEntity.getHPF_Hz_Regex, lineName, stationName)
              IDAndValue = selectYCData(IDs, station_abbr)
              var HPF_Hz = assign_instant(IDAndValue)
              // 赋值进风量
              var air_yield = getAirYield(HPF_Hz, KT_Hz)
              // 赋值送风温度
              IDs = selectByRegex(regexEntity.getSF_T_Regex, lineName, stationName)
              IDAndValue = selectYCData(IDs, station_abbr)
              var SF_T = assign_instant(IDAndValue)
              // 赋值新风温度
              IDs = selectByRegex(regexEntity.getXF_T_Regex, lineName, stationName)
              IDAndValue = selectYCData(IDs, station_abbr)
              var XF_T = assign_instant(IDAndValue)
              // 效率d2
              if (consumption == 0) {
                d2 = 100
              } else {
                result = getIndicatorLL(SF_run_status, air_yield, ZT_T, XF_T, SF_T)./(consumption.*(3))
                if (result > XL_standard) {
                  d2 = 100
                } else {
                  d2 = result./(XL_standard).*(100)
                }
              }
              // 冷量d3
              result = getIndicatorLL(SF_run_status, air_yield, ZT_T, XF_T, SF_T)
              if (result > LL_standard) {
                d3 = LL_standard./(result).*(100)
              } else {
                d3 = 100
              }
              // 重新赋值20分钟风机能耗
              IDs = selectByRegex(regexEntity.getKT_ZNDB_Regex, lineName, stationName)
              IDAndValue = selectConsumptionData(IDs, startTime, station_abbr)
              consumption = assign_powerConsumption(IDAndValue)
              IDs = selectByRegex(regexEntity.getHPF_ZNDB_Regex, lineName, stationName)
              IDAndValue = selectConsumptionData(IDs, startTime, station_abbr)
              consumption = consumption + assign_powerConsumption(IDAndValue)
              // 大系统通风d24
              if (consumption == 0) {
                d24 = 0
              } else {
                result = getIndicatorKT(SF_run_status, consumption, air_yield, ZT_T, XF_T, SF_T)
                if (result > DKT_standard) {
                  d24 = 100
                } else {
                  d24 = result./(DKT_standard).*(100)
                }
              }
            }
            // 室温d4
            if (false) {
              // TODO 非通风时间
              d4_Status = 0
              d4 = 0
            } else {
              d4_Status = 1
              if (HFT_standard - ZT_T > 6) {
                d4 = 0
              } else {
                d4 = 100 - Math.abs(HFT_standard - ZT_T)./(6).*(100)
              }
            }
            d4_sumStatus = d4_Status + getKPIDay(sum_status, 2)
            // 赋值冷冻水瞬时流量
            IDs = selectByRegex(regexEntity.getLD_yield_Regex, lineName, stationName)
            IDAndValue = selectYCData(IDs, station_abbr)
            var water_yield = assign_instant(IDAndValue)
            if(water_yield == 0){
              // 从外部配置文件中读取瞬时流量数据
              // TODO 瞬时流量参数配置公用一个，还是每个站一个？
              water_yield = settings.getProperty("").toDouble
            }
            // 赋值冷冻水的供、回水温度
            IDs = selectByRegex(regexEntity.getLD_IT_Regex, lineName, stationName)
            IDAndValue = selectYCData(IDs, station_abbr)
            var water_IT = assign_instant(IDAndValue)
            IDs = selectByRegex(regexEntity.getLD_OT_Regex, lineName, stationName)
            IDAndValue = selectYCData(IDs, station_abbr)
            var water_OT = assign_instant(IDAndValue)
            // 冷机d5
            // 赋值冷机运行状态
            IDs = selectByRegex(regexEntity.getLSJZ_Run_Regex, lineName, stationName)
            IDAndValue = selectYXData(IDs, station_abbr)
            d5_Status = assign_runStatus(IDAndValue)
            d5_sumStatus = d5_Status + getKPIDay(sum_status, 3)
            if (d5_Status == 0) {
              d5 = 0
            } else {
              // 赋值20分钟冷机能耗
              IDs = selectByRegex(regexEntity.getLSJZ_ZNDB_Regex, lineName, stationName)
              IDAndValue = selectConsumptionData(IDs, startTime, station_abbr)
              consumption = assign_powerConsumption(IDAndValue)
              if (consumption == 0) {
                d5 = 0
              } else {
                result = getIndicatorTwo(consumption, water_yield, water_OT, water_IT)
                if (result > LSJZ_standard) {
                  d5 = 100
                } else {
                  d5 = result./(LSJZ_standard).*(100)
                }
              }
            }
            // 冷冻泵d10
            // 赋值冷冻泵运行状态
            IDs = selectByRegex(regexEntity.getLDB_Run_Regex, lineName, stationName)
            IDAndValue = selectYXData(IDs, station_abbr)
            d10_Status = assign_runStatus(IDAndValue)
            d10_sumStatus = d10_Status + getKPIDay(sum_status, 4)
            if (d10_Status == 0) {
              d10 = 0
            } else {
              // 赋值20分钟冷冻泵能耗
              IDs = selectByRegex(regexEntity.getLDB_ZNDB_Regex, lineName, stationName)
              IDAndValue = selectConsumptionData(IDs, startTime, station_abbr)
              consumption = assign_powerConsumption(IDAndValue)
              if (consumption == 0) {
                d10 = 0
              } else {
                result = getIndicatorTwo(consumption, water_yield, water_OT, water_IT)
                if (result > LD_standard) {
                  d10 = 100
                } else {
                  d10 = result./(LD_standard).*(100)
                }
              }
            }
            // 赋值冷却水瞬时流量
            IDs = selectByRegex(regexEntity.getLQ_yield_Regex, lineName, stationName)
            IDAndValue = selectYCData(IDs, station_abbr)
            water_yield = assign_instant(IDAndValue)
            if(water_yield == 0){
              // 从外部配置文件中读取瞬时流量数据
              // TODO 瞬时流量参数配置公用一个，还是每个站一个？
              water_yield = settings.getProperty("").toDouble
            }
            // 赋值冷却水的供、回水温度
            IDs = selectByRegex(regexEntity.getLQ_IT_Regex, lineName, stationName)
            IDAndValue = selectYCData(IDs, station_abbr)
            water_IT = assign_instant(IDAndValue)
            IDs = selectByRegex(regexEntity.getLQ_OT_Regex, lineName, stationName)
            IDAndValue = selectYCData(IDs, station_abbr)
            water_OT = assign_instant(IDAndValue)
            // 冷却泵d15
            // 赋值冷却泵运行状态
            IDs = selectByRegex(regexEntity.getLQB_Run_Regex, lineName, stationName)
            IDAndValue = selectYXData(IDs, station_abbr)
            d15_Status = assign_runStatus(IDAndValue)
            d15_sumStatus = d15_Status + getKPIDay(sum_status, 5)
            if (d15_Status == 0) {
              d15 = 0
            } else {
              // 赋值20分钟冷却泵能耗
              IDs = selectByRegex(regexEntity.getLQB_ZNDB_Regex, lineName, stationName)
              IDAndValue = selectConsumptionData(IDs, startTime, station_abbr)
              consumption = assign_powerConsumption(IDAndValue)
              if (consumption == 0) {
                d15 = 0
              } else {
                result = getIndicatorTwo(consumption, water_yield, water_OT, water_IT)
                if (result > LQ_standard) {
                  d15 = 100
                } else {
                  d15 = result./(LQ_standard).*(100)
                }
              }
            }
            // 冷冻塔d20
            // 赋值冷冻塔运行状态
            IDs = selectByRegex(regexEntity.getLT_Run_Regex, lineName, stationName)
            IDAndValue = selectYXData(IDs, station_abbr)
            d20_Status = assign_runStatus(IDAndValue)
            d20_sumStatus = d20_Status + getKPIDay(sum_status, 6)
            if (d20_Status == 0) {
              d20 = 0
            } else {
              // 赋值20分钟冷冻塔能耗
              IDs = selectByRegex(regexEntity.getLT_ZNDB_Regex, lineName, stationName)
              IDAndValue = selectConsumptionData(IDs, startTime, station_abbr)
              consumption = assign_powerConsumption(IDAndValue)
              if (consumption == 0) {
                d20 = 0
              } else {
                result = getIndicatorTwo(consumption, water_yield, water_OT, water_IT)
                if (result > LT_standard) {
                  d20 = 100
                } else {
                  d20 = result./(LT_standard).*(100)
                }
              }
            }
            KPIs_real += Array(
              station_abbr + endTime,
              d2.toString(),
              d3.toString(),
              d4.toString(),
              d5.toString(),
              d10.toString(),
              d15.toString(),
              d20.toString(),
              d24.toString())
            KPIs_status += Array(
              station_abbr + endTime,
              d2_Status.toString(),
              d3_Status.toString(),
              d4_Status.toString(),
              d5_Status.toString(),
              d10_Status.toString(),
              d15_Status.toString(),
              d20_Status.toString(),
              d24_Status.toString())
            KPIs_day += Array(
              station_abbr + endTime,
              d1.toString(),
              if (d2_sumStatus == 0) {
                //"-",一类设备未开启，则其相应的指标得分上位显示“—”
                "0"
              } else {
                (d2 + getKPIDay(sum_KPI, 0))./(d2_sumStatus).toString()
              },
              if (d3_sumStatus == 0) {
                "0"
              } else {
                (d3 + getKPIDay(sum_KPI, 1))./(d3_sumStatus).toString()
              },
              if (d4_sumStatus == 0) {
                "0"
              } else {
                (d4 + getKPIDay(sum_KPI, 2))./(d4_sumStatus).toString()
              },
              if (d5_sumStatus == 0) {
                "0"
              } else {
                (d5 + getKPIDay(sum_KPI, 3))./(d5_sumStatus).toString()
              },
              if (d10_sumStatus == 0) {
                "0"
              } else {
                (d10 + getKPIDay(sum_KPI, 4))./(d10_sumStatus).toString()
              },
              if (d15_sumStatus == 0) {
                "0"
              } else {
                (d15 + getKPIDay(sum_KPI, 5))./(d15_sumStatus).toString()
              },
              if (d20_sumStatus == 0) {
                "0"
              } else {
                (d20 + getKPIDay(sum_KPI, 6))./(d20_sumStatus).toString()
              },
              if (d24_sumStatus == 0) {
                "0"
              } else {
                (d24 + getKPIDay(sum_KPI, 7))./(d24_sumStatus).toString()
              })

            //println("Done!")
            //return // 测试，直接结束

          }
        }
      }
    }
    saveToHbase(KPI_real, KPIs_real)
    saveToHbase(hisstatus, KPIs_status)
    saveToHbase(KPI_day, KPIs_day)
    
    println("Done!")
    spark.stop()

    // =================== 查询所需历史数据数据的方法 ===================
    // 根据type查询对应表controlID数据
    def selectYXData(IDs: Array[Row], station_abbr: String): Array[Row] = {
      if (IDs.length == 0) {
        return Array[Row]()
      }
      val YX_IDs = IDs.map(row => {
        row.get(1).toString()
      })
      // SELECT * FROM `hisdataYX_bj9_qlz` WHERE ID IN (99,106,113,120,127);
      // 参数 : _*表示将数组的单个元素作为传入的参数
      return hisdataYX_DF.select("time", "id", "value").filter($"abbreviation" === station_abbr && $"id".isin(YX_IDs: _*)).collect()
    }
    // 根据type查询对应表controlID定存数据
    def selectYCData(IDs: Array[Row], station_abbr: String): Array[Row] = {
      if (IDs.length == 0) {
        return Array[Row]()
      }
      val YC_IDs = IDs.map(row => {
        new ColumnName(row.getString(1))
      })
      // SELECT TIME,`77`,`84`,`133` FROM `hisdata_bj9_qlz_1`;
      return hisdataYC_DF.select(YC_IDs: _*).filter($"abbreviation" === station_abbr && $"time" === endTime).collect()
    }
    // 查询能耗对应表controlID两个时间的定存数据
    def selectConsumptionData(IDs: Array[Row], startTime: String, station_abbr: String): Array[Row] = {
      if (IDs.length == 0) {
        return Array[Row]()
      }
      val YC_IDs = IDs.map(row => {
        new ColumnName(row.getString(1))
      })
      // SELECT TIME,`77`,`84`,`133` FROM `hisdata_bj9_qlz_1`;
      return hisdataYC_DF.select(YC_IDs: _*).filter($"abbreviation" === station_abbr && $"time".isin(startTime, endTime)).collect()
    }
    // 根据regex查询type和controlID
    def selectByRegex(regex: String, lineName: String, stationName: String): Array[Row] = {
      return point_DF.select("type", "controlID").filter($"lineName" === lineName && $"stationName" === stationName && $"pointName".like(regex)).collect()
    }
    // ========================================

    // =================== 赋值各个数据的方法 ===================
    // 获取运行状态
    def assign_runStatus(IDAndValue: Array[Row]): Double = {
      if (IDAndValue.length == 0) {
        return 0
      } else {
        // time,id,value
        for (row <- IDAndValue) {
          if (row.getString(2) == "1") {
            return 1
          }
        }
        return 0
      }
    }
    // 获取累计消耗电量
    def assign_powerConsumption(IDAndValue: Array[Row]): Double = {
      var sumPower: Double = 0
      if (IDAndValue.length == 0 || IDAndValue.length == 1) {
        return sumPower
      } else {
        for (i <- 1 to IDAndValue(0).size - 1) {
          var v1 = 0d
          var v0 = 0d
          if (IDAndValue(1).getString(i) != "\\N") {
            v1 = IDAndValue(1).getString(i).toDouble
          }
          if (IDAndValue(0).getString(i) != "\\N") {
            v0 = IDAndValue(0).getString(i).toDouble
          }
          sumPower += (v1 - v0)
        }
        return sumPower
      }
    }
    // 平均值数据(瞬时流量，进出水温度，温度，频率)
    def assign_instant(IDAndValue: Array[Row]): Double = {
      if (IDAndValue.length == 0) {
        // TODO 如果没有瞬时流量指标，则读取配置文件。（不在此处处理，赋值后再进行判定）
        return 0
      } else {
        var value: Double = 0
        val num = IDAndValue(0).size - 1
        for (i <- 0 to num) {
          var v = 0d
          if (IDAndValue(0).getString(i) != "\\N") {
            v = IDAndValue(0).getString(i).toDouble
          }
          value += v
        }
        return value./(num)
      }
    }
    // ========================================

    // =================== 根据历史数据计算指标的方法 ===================
    // 计算二级指标的瞬时指标
    def getIndicatorTwo(consumption: Double, water_yield: Double, water_OT: Double, water_IT: Double): Double = {
      return (water_specificHeat.*(water_density).*(water_yield).*(water_OT - water_IT).*(run_time))./(216000000.*(consumption))
    }
    // 计算进风量
    def getAirYield(HPF_Hz: Double, KT_Hz: Double): Double = {
      return (KT_Hz./(50).*(KT_F)) + (HPF_Hz./(50).*(HPF_F))
    }
    // 计算二级指标大系统通风的瞬时指标
    def getIndicatorKT(SF_run_status: Double, consumption: Double, air_yield: Double, ZT_T: Double, XF_T: Double, SF_T: Double): Double = {
      if (SF_run_status == 0) {
        return (air_specificHeat.*(air_density).*(air_yield).*(ZT_T - XF_T).*(run_time))./(216000000.*(consumption))
      } else {
        return (air_specificHeat.*(air_density).*(air_yield).*(ZT_T - SF_T).*(run_time))./(216000000.*(consumption))
      }
    }
    // 计算一级指标冷量,效率的瞬时指标
    def getIndicatorLL(SF_run_status: Double, air_yield: Double, ZT_T: Double, XF_T: Double, SF_T: Double): Double = {
      if (SF_run_status == 0) {
        return (air_specificHeat.*(air_density).*(air_yield).*(ZT_T - XF_T))./(3600000)
      } else {
        return (air_specificHeat.*(air_density).*(air_yield).*(ZT_T - SF_T))./(3600000)
      }
    }
    // 计算一级指标电耗的瞬时指标
    def getIndicatorPower(consumption: Double, area: Double): Double = {
      return consumption./(area.*(24))
    }
    // ========================================
    // =================== 计算kpi_day和累积运行状态的方法 ===================
    def getKPIDay(sum: Array[Row], dn: Int): Double = {
      if (sum(0).get(dn) == null) {
        return 0
      } else {
        return sum(0).getDouble(dn)
      }
    }
    // ========================================
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
  }
  // 将scan配置的conf前先转换为string
  def convertScanToString(scan: Scan) = {
    Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
  }
}