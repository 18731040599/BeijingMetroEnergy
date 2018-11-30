package com.thtf.sparksql

import java.util.{ Properties, Calendar }
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._
import java.text.SimpleDateFormat
import java.io.{ FileInputStream, File }
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.{ Base64, Bytes }
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.filter._

case class KPI(
  Time: String,
  d1: Double,
  d2: Double,
  d3: Double,
  d4: Double,
  d5: Double,
  d10: Double,
  d15: Double,
  d20: Double,
  d24: Double)
object TestOnMySQL {
  def main(args: Array[String]): Unit = {

    println("Start!")

    // 时间
    val calendar = Calendar.getInstance
    val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm00")
    //val endTime = simpleDateFormat.format(calendar.getTime)
    val endTime = "20181010102000"
    calendar.add(Calendar.MINUTE, -20)
    //val startTime = simpleDateFormat.format(calendar.getTime)
    val startTime = "20181010100000"
    calendar.add(Calendar.MINUTE, -(20 * 71))
    //val lastDay = simpleDateFormat.format(calendar.getTime)
    val lastDay = "20181009102000"

    // 读取配置文件
    val properties = new Properties
    try {
      properties.load(new FileInputStream(new File("conf/settings.properties")))
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }

    // 常量参数
    val water_specificHeat: Double = properties.getProperty("water_specificHeat").toDouble
    val water_density: Double = properties.getProperty("water_density").toDouble
    val air_specificHeat: Double = properties.getProperty("air_specificHeat").toDouble
    val air_density: Double = properties.getProperty("air_density").toDouble
    // 设定参数
    val run_time: Double = properties.getProperty("run_time").toDouble // 系统设置值
    val KT_F: Double = properties.getProperty("KT_F").toDouble // 系统设定值
    val HPF_F: Double = properties.getProperty("HPF_F").toDouble // 系统设定值
    // 指标标准
    val LSJZ_standard: Double = properties.getProperty("LSJZ_standard").toDouble
    val LD_standard: Double = properties.getProperty("LD_standard").toDouble
    val LQ_standard: Double = properties.getProperty("LQ_standard").toDouble
    val LT_standard: Double = properties.getProperty("LT_standard").toDouble
    val DKT_standard: Double = properties.getProperty("DKT_standard").toDouble
    val LL_standard: Double = properties.getProperty("LL_standard").toDouble
    val power_standard: Double = properties.getProperty("power_standard").toDouble
    val XL_standard: Double = properties.getProperty("XL_standard").toDouble
    val HFT_standard: Double = properties.getProperty("HFT_standard").toDouble

    // 线路
    var lineName: String = ""
    var line_abbr: String = ""
    // 车站
    var stationName: String = ""
    var station_abbr: String = ""
    // 面积
    var area: Double = 0
    // 运行状态
    var run_status: Double = 0
    var SF_run_status: Double = 0
    // 能耗
    var consumption: Double = 0
    // 累计次数
    var cumulative_number: Double = 0
    // 冷水
    var water_yield: Double = 0
    var water_IT: Double = 0
    var water_OT: Double = 0
    // 风量
    var air_yield: Double = 0
    // 频率
    var KT_Hz: Double = 0
    var HPF_Hz: Double = 0
    // 温度
    var HF_T: Double = 0
    var SF_T: Double = 0
    var XF_T: Double = 0
    var ZT_T: Double = 0

    // =================== regex ===================
    // 冷水机组
    val LSJZ_Run_Regex = "LSJZ___i%Run_St"
    val LSJZ_ZNDB_Regex = "ZNDB_LS___Wp_Tm"
    // 冷冻泵
    val LDB_Run_Regex = "LDB___i%Run_St"
    val LDB_ZNDB_Regex = "ZNDB_LD___Wp_Tm"
    // 冷却泵
    val LQB_Run_Regex = "LQB___i%Run_St"
    val LQB_ZNDB_Regex = "ZNDB_LQ___Wp_Tm"
    // 冷却塔
    val LT_Run_Regex = "LQT___i%Run_St"
    val LT_ZNDB_Regex = "ZNDB_LT___Wp_Tm"
    // 送风机
    val KT_Run_Regex = "KT____iRun_St"
    val KT_ZNDB_Regex = "ZNDB_KT____Wp_Tm"
    val KT_Hz_Regex = "KT____FreqFB_Hz"
    // 排风机
    val HPF_Run_Regex = "HPF____iRun_St"
    val HPF_ZNDB_Regex = "ZNDB_HPF____Wp_Tm"
    val HPF_Hz_Regex = "HPF____FreqFB_Hz"
    // 温度
    val HF_T_Regex = "TH_HFD____Temp_Tm"
    val SF_T_Regex = "TH_SFD____Temp_Tm"
    val XF_T_Regex = "TH_SWD____Temp_Tm"
    val ZTX_T_Regex = "TH_ZTX_____Temp_Tm"
    val ZTS_T_Regex = "TH_ZTS_____Temp_Tm"
    // 冷冻水
    val LD_yield_Regex = "FT_LDD_0__Ft_Tm"
    val LD_IT_Regex = "TE_LDJD_01_Temp_Tm"
    val LD_OT_Regex = "TE_LDCD_01_Temp_Tm"
    // 冷却水
    val LQ_yield_Regex = "FT_LQD_0__Ft_Tm"
    val LQ_IT_Regex = "TE_LQJD_01_Temp_Tm"
    val LQ_OT_Regex = "TE_LQCD_01_Temp_Tm"
    // 总电耗
    val ALL_ZNDB_Regex = "ZNDB%Wp_Tm"
    // =========================================================

    // 创建sparksession
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    // 静态表名
    val point = "point"
    val center = "center"
    val line = "line"

    val hisdataYC = "hisdataYC"
    val hisdataYX = "hisdataYX"

    // 表的字段名
    val center_fields = "lineName abbreviation projectName description eneryMgr".split(" ")
    val line_fields = "stationName abbreviation lineName description area eneryMgr transferstation".split(" ")
    val point_fields = "lineName stationName pointName type controlID equipmentID equipmentName dataType statics unit quantity lowerLimitValue upperLimitValue description".split(" ")
    val tables = Map(
      "center" -> center_fields,
      "line" -> line_fields,
      "point" -> point_fields)

    // =================== 将读取hbase，将rdd转换为dataframe ===================
    def finalHtableToDF(tablename: String) {
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
          var row = Row(Bytes.toString(result.getRow))
          for (i <- 1 to result.size()) {
            row = Row.merge(row, Row(Bytes.toString(result.getValue("c".getBytes, tables.getOrElse(tablename, null)(i).getBytes))))
          }
          row
        })
      // 以编程的方式指定 Schema，将RDD转化为DataFrame
      val fields = tables.getOrElse(point, null).map(field => StructField(field, StringType, nullable = true))
      spark.createDataFrame(tableRDD, StructType(fields)).createTempView(tablename)
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
          for (i <- 0 to result.size() - 1) {
            row = Row.merge(row, Row(Bytes.toString(result.getValue("c".getBytes, i.toString().getBytes))))
          }
          row
        })
      // 以编程的方式指定 Schema，将RDD转化为DataFrame
      val fields = (-2 to 599).toArray.map(field => StructField(field.toString(), StringType, nullable = true))
      fields(0) = StructField("abbreviation".toString(), StringType, nullable = true)
      fields(1) = StructField("TIME".toString(), StringType, nullable = true)
      spark.createDataFrame(tableRDD, StructType(fields))
    }
    // ========================================

    // =================== 将读取hbase，将rdd转换为dataframe ===================
    def hisdataYXToDF(tablename: String) {
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
      spark.createDataFrame(tableRDD, schema).filter($"time" > lastDay && $"time" <= endTime).createTempView(tablename)
      spark.sql(s"SELECT data.* FROM `${tablename}` data ,(SELECT MAX(time) AS mt,abbreviation,id FROM `${tablename}` GROUP BY abbreviation,id) t WHERE data.id=t.id AND data.time =t.mt AND data.abbreviation=t.abbreviation").createOrReplaceTempView(tablename)
    }
    // ========================================

    finalHtableToDF(center)
    finalHtableToDF(point)
    finalHtableToDF(line)
    hisdataYCToDF(hisdataYC)
    hisdataYXToDF(hisdataYX)

    // 查询线路
    //val center_records = null
    val lines_df = spark.sql(s"SELECT lineName,abbreviation FROM `${center}` WHERE eneryMgr=1")
    if (lines_df == null) {
      return
    }
    val lines = lines_df.collect()
    // 遍历线路
    for (i <- 0 to (lines.length - 1)) {
      lineName = lines(i).getString(0)
      line_abbr = lines(i).getString(1)
      // 查询车站
      val stations_df = spark.sql(s"SELECT stationName,abbreviation FROM `${line}` WHERE lineName='${lineName}' AND eneryMgr=1")
      if (stations_df == null) {
        return
      }
      val stations = stations_df.collect()
      // 遍历车站
      for (i <- 0 to (stations.length - 1)) {
        var d1: Double = 0
        var d2: Double = 0
        var d3: Double = 0
        var d4: Double = 0
        var d5: Double = 0
        var d10: Double = 0
        var d15: Double = 0
        var d20: Double = 0
        var d24: Double = 0
        stationName = stations(i).getString(0)
        station_abbr = stations(i).getString(1)
        println(spark.sql(s"select * from ${hisdataYC}").count())
        println(spark.sql(s"select * from ${hisdataYX}").count())
        // 计算各项指标
        // 电耗d1
        area = spark.sql(s"SELECT area FROM `${line}` WHERE lineName='${lineName}' AND stationName='${stationName}'").take(1)(0).get(0).asInstanceOf[Double]
        // 赋值24小时总能耗
        var IDs = selectByRegex(ALL_ZNDB_Regex)
        var IDAndValue = selectConsumptionData(IDs, lastDay)
        consumption = assign_powerConsumption(IDAndValue)
        var result = getIndicatorPower()
        if (power_standard > result) {
          d1 = 100
        } else {
          d1 = 100 - (result - power_standard)./(result).*(100)
        }
        // 赋值回风温度
        IDs = selectByRegex(HF_T_Regex)
        IDAndValue = selectYCData(IDs)
        HF_T = assign_instant(IDAndValue)
        // 赋值室内温度
        IDs = selectByRegex(ZTX_T_Regex)
        IDAndValue = selectYCData(IDs)
        ZT_T = assign_instant(IDAndValue)
        IDs = selectByRegex(ZTS_T_Regex)
        IDAndValue = selectYCData(IDs)
        ZT_T = (ZT_T + assign_instant(IDAndValue))./(2)
        // 赋值送风机运行状态
        IDs = selectByRegex(KT_Run_Regex)
        IDAndValue = selectYXData(IDs)
        SF_run_status = assign_runStatus(IDAndValue)
        // 赋值运行状态
        if (SF_run_status == 0) {
          // 获取排风机运行状态
          IDs = selectByRegex(HPF_Run_Regex)
          IDAndValue = selectYCData(IDs)
          run_status = assign_runStatus(IDAndValue)
        } else {
          run_status = 1
        }
        if (run_status == 0) {
          d2 = 0
          d3 = 0
          d24 = 0
        } else {
          // 重新赋值20分钟总能耗
          IDAndValue = selectConsumptionData(IDs, startTime)
          consumption = assign_powerConsumption(IDAndValue)
          // 赋值送风机频率
          IDs = selectByRegex(KT_Hz_Regex)
          IDAndValue = selectYCData(IDs)
          KT_Hz = assign_instant(IDAndValue)
          // 赋值回排风机频率
          IDs = selectByRegex(HPF_Hz_Regex)
          IDAndValue = selectYCData(IDs)
          HPF_Hz = assign_instant(IDAndValue)
          // 赋值进风量
          air_yield = getAirYield()
          // 赋值送风温度
          IDs = selectByRegex(SF_T_Regex)
          IDAndValue = selectYCData(IDs)
          SF_T = assign_instant(IDAndValue)
          // 赋值新风温度
          IDs = selectByRegex(XF_T_Regex)
          IDAndValue = selectYCData(IDs)
          XF_T = assign_instant(IDAndValue)
          // 效率d2
          if (consumption == 0) {
            d2 = 0
          } else {
            result = getIndicatorLL()./(consumption.*(3))
            if (result > XL_standard) {
              d2 = 100
            } else {
              d2 = result./(XL_standard).*(100)
            }
          }
          // 冷量d3
          result = getIndicatorLL()
          if (result > LL_standard) {
            d3 = LL_standard./(result).*(100)
          } else {
            d3 = 100
          }
          // 重新赋值20分钟风机能耗
          IDs = selectByRegex(KT_ZNDB_Regex)
          IDAndValue = selectConsumptionData(IDs, startTime)
          consumption = assign_powerConsumption(IDAndValue)
          IDs = selectByRegex(HPF_ZNDB_Regex)
          IDAndValue = selectConsumptionData(IDs, startTime)
          consumption = consumption + assign_powerConsumption(IDAndValue)
          // 大系统通风d24
          if (consumption == 0) {
            d24 = 0
          } else {
            result = getIndicatorKT()
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
          d4 = 0
        } else {
          if (HFT_standard - ZT_T > 6) {
            d4 = 0
          } else {
            d4 = 100 - Math.abs(HFT_standard - ZT_T)./(6).*(100)
          }
        }
        // 赋值冷冻水瞬时流量
        IDs = selectByRegex(LD_yield_Regex)
        IDAndValue = selectYCData(IDs)
        water_yield = assign_instant(IDAndValue)
        // 赋值冷冻水的供、回水温度
        IDs = selectByRegex(LD_IT_Regex)
        IDAndValue = selectYCData(IDs)
        water_IT = assign_instant(IDAndValue)
        IDs = selectByRegex(LD_OT_Regex)
        IDAndValue = selectYCData(IDs)
        water_OT = assign_instant(IDAndValue)
        // 冷机d5
        // 赋值冷机运行状态
        IDs = selectByRegex(LSJZ_Run_Regex)
        IDAndValue = selectYCData(IDs)
        run_status = assign_runStatus(IDAndValue)
        if (run_status == 0) {
          d5 = 0
        } else {
          // 赋值20分钟冷机能耗
          IDs = selectByRegex(LSJZ_ZNDB_Regex)
          IDAndValue = selectConsumptionData(IDs, startTime)
          consumption = assign_powerConsumption(IDAndValue)
          if (consumption == 0) {
            d5 = 0
          } else {
            result = getIndicatorTwo()
            if (result > LSJZ_standard) {
              d5 = 100
            } else {
              d5 = result./(LSJZ_standard).*(100)
            }
          }
        }
        // 冷冻泵d10
        // 赋值冷冻泵运行状态
        IDs = selectByRegex(LDB_Run_Regex)
        IDAndValue = selectYCData(IDs)
        run_status = assign_runStatus(IDAndValue)
        if (run_status == 0) {
          d10 = 0
        } else {
          // 赋值20分钟冷冻泵能耗
          IDs = selectByRegex(LDB_ZNDB_Regex)
          IDAndValue = selectConsumptionData(IDs, startTime)
          consumption = assign_powerConsumption(IDAndValue)
          if (consumption == 0) {
            d10 = 0
          } else {
            result = getIndicatorTwo()
            if (result > LD_standard) {
              d10 = 100
            } else {
              d10 = result./(LD_standard).*(100)
            }
          }
        }
        // 赋值冷却水瞬时流量
        IDs = selectByRegex(LQ_yield_Regex)
        IDAndValue = selectYCData(IDs)
        water_yield = assign_instant(IDAndValue)
        // 赋值冷却水的供、回水温度
        IDs = selectByRegex(LQ_IT_Regex)
        IDAndValue = selectYCData(IDs)
        water_IT = assign_instant(IDAndValue)
        IDs = selectByRegex(LQ_OT_Regex)
        IDAndValue = selectYCData(IDs)
        water_OT = assign_instant(IDAndValue)
        // 冷却泵d15
        // 赋值冷却泵运行状态
        IDs = selectByRegex(LQB_Run_Regex)
        IDAndValue = selectYCData(IDs)
        run_status = assign_runStatus(IDAndValue)
        if (run_status == 0) {
          d15 = 0
        } else {
          // 赋值20分钟冷却泵能耗
          IDs = selectByRegex(LQB_ZNDB_Regex)
          IDAndValue = selectConsumptionData(IDs, startTime)
          consumption = assign_powerConsumption(IDAndValue)
          if (consumption == 0) {
            d15 = 0
          } else {
            result = getIndicatorTwo()
            if (result > LQ_standard) {
              d15 = 100
            } else {
              d15 = result./(LQ_standard).*(100)
            }
          }
        }
        // 冷冻塔d20
        // 赋值冷冻塔运行状态
        IDs = selectByRegex(LT_Run_Regex)
        IDAndValue = selectYCData(IDs)
        run_status = assign_runStatus(IDAndValue)
        if (run_status == 0) {
          d20 = 0
        } else {
          // 赋值20分钟冷冻塔能耗
          IDs = selectByRegex(LT_ZNDB_Regex)
          IDAndValue = selectConsumptionData(IDs, startTime)
          consumption = assign_powerConsumption(IDAndValue)
          if (consumption == 0) {
            d20 = 0
          } else {
            result = getIndicatorTwo()
            if (result > LT_standard) {
              d20 = 100
            } else {
              d20 = result./(LT_standard).*(100)
            }
          }
        }
        val kpi_instant = KPI.apply(endTime, d1, d2, d3, d4, d5, d10, d15, d20, d24)
        println(kpi_instant)
        println("Done!")
        return // 测试，直接结束
      }
    }
    println("Done!")

    // =================== 查询所需历史数据数据的方法 ===================
    // 根据type查询对应表controlID数据
    def selectYXData(IDs: Array[Row]): Array[Row] = {
      if (IDs == null) {
        return null
      }
      var YX_IDs = ""
      IDs.map(row => {
        YX_IDs += (row.get(1).toString() + ",")
      })
      YX_IDs = YX_IDs.dropRight(1)
      // SELECT * FROM `hisdataYX_bj9_qlz` WHERE ID IN (99,106,113,120,127);
      return spark.sql(s"SELECT time,id,value FROM `${hisdataYX}` WHERE abbreviation = '${station_abbr}' AND id IN (${YX_IDs})").collect()
    }
    // 根据type查询对应表controlID定存数据
    def selectYCData(IDs: Array[Row]): Array[Row] = {
      if (IDs == null) {
        return null
      }
      var YC_IDs = ""
      IDs.map(row => {
        YC_IDs += ("`" + row.get(1).toString() + "`,")
      })
      YC_IDs = YC_IDs.dropRight(1)
      // SELECT TIME,`77`,`84`,`133` FROM `hisdata_bj9_qlz_1`;
      return spark.sql(s"SELECT TIME,${YC_IDs} FROM `${hisdataYC}` WHERE abbreviation = '${station_abbr}' AND time = '${endTime}'").collect()
    }
    // 查询能耗对应表controlID两个时间的定存数据
    def selectConsumptionData(IDs: Array[Row], startTime: String): Array[Row] = {
      if (IDs == null) {
        return null
      }
      var YC_IDs = ""
      IDs.map(row => {
        YC_IDs += ("`" + row.getString(1) + "`,")
      })
      YC_IDs = YC_IDs.dropRight(1)
      // SELECT TIME,`77`,`84`,`133` FROM `hisdata_bj9_qlz_1`;
      return spark.sql(s"SELECT time,${YC_IDs} FROM `${hisdataYC}` WHERE abbreviation = '${station_abbr}' AND time IN ('${startTime}','${endTime}')").collect()
    }
    // 根据regex查询type和controlID
    def selectByRegex(regex: String): Array[Row] = {
      val result = spark.sql(s"SELECT type,controlID FROM `${point}` WHERE lineName='${lineName}' AND stationName='${stationName}' AND pointName LIKE '${regex}'")
      if (result == null) {
        return null
      } else {
        result.collect()
      }
    }
    // ========================================

    // =================== 赋值各个数据的方法 ===================
    // 获取运行状态
    def assign_runStatus(IDAndValue: Array[Row]): Double = {
      if (IDAndValue == null) {
        return 0
      } else {
        // time,id,value
        for (row <- IDAndValue) {
          if (row.getString(0) == endTime && row.getString(2) == "0") {
            return 1
          } else if (row.getString(0) != endTime && row.getString(2) == "1") {
            return 1
          }
        }
        return 0
      }
    }
    // 获取累计消耗电量
    def assign_powerConsumption(IDAndValue: Array[Row]): Double = {
      var sumPower: Double = 0
      if (IDAndValue == null || IDAndValue.length == 1) {
        return sumPower
      } else {
        for (i <- 1 to IDAndValue(0).size - 1) {
          sumPower += (IDAndValue(1).get(i).asInstanceOf[Double] - IDAndValue(0).get(i).asInstanceOf[Double])
        }
        return sumPower
      }
    }
    // 平均值数据(瞬时流量，进出水温度，温度，频率)
    def assign_instant(IDAndValue: Array[Row]): Double = {
      if (IDAndValue == null) {
        // TODO 如果没有瞬时流量指标，则读取配置文件
        return 0
      } else {
        var value: Double = 0
        val num = IDAndValue(0).size - 1
        for (i <- 1 to num) {
          value += IDAndValue(0).get(i).asInstanceOf[Double]
        }
        return value./(num)
      }
    }
    // ========================================

    // =================== 根据历史数据计算指标的方法 ===================
    // 计算二级指标的瞬时指标
    def getIndicatorTwo(): Double = {
      return (water_specificHeat.*(water_density).*(water_yield).*(water_OT - water_IT).*(run_time))./(216000000.*(consumption))
    }
    // 计算进风量
    def getAirYield(): Double = {
      return (KT_Hz./(50).*(KT_F)) + (HPF_Hz./(50).*(HPF_F))
    }
    // 计算二级指标大系统通风的瞬时指标
    def getIndicatorKT(): Double = {
      if (SF_run_status == 0) {
        return (air_specificHeat.*(air_density).*(air_yield).*(ZT_T - XF_T).*(run_time))./(216000000.*(consumption))
      } else {
        return (air_specificHeat.*(air_density).*(air_yield).*(ZT_T - SF_T).*(run_time))./(216000000.*(consumption))
      }
    }
    // 计算一级指标冷量,效率的瞬时指标
    def getIndicatorLL(): Double = {
      if (SF_run_status == 0) {
        return (air_specificHeat.*(air_density).*(air_yield).*(ZT_T - XF_T))./(3600000)
      } else {
        return (air_specificHeat.*(air_density).*(air_yield).*(ZT_T - SF_T))./(3600000)
      }
    }
    // 计算一级指标电耗的瞬时指标
    def getIndicatorPower(): Double = {
      return consumption./(area.*(24))
    }
    // ========================================
  }
  // 将scan配置到conf前先转换为string
  def convertScanToString(scan: Scan) = {
    Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray)
  }
}