package com.thtf.sparksql

import java.util.{ Properties, Calendar }
import org.apache.spark.sql.{ SparkSession, Row }
import org.apache.spark.sql.types._
import java.util.Date
import java.text.SimpleDateFormat
import java.io.FileInputStream
import java.io.File

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
  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local")
    .config("spark.sql.warehouse.dir", "C:/Users/Wyh/eclipse-workspace/BeijingMetroEnergy/spark-warehouse")
    .getOrCreate()
  // For implicit conversions like converting RDDs to DataFrames
  import spark.implicits._
  def main(args: Array[String]): Unit = {

    println("Start!")

    // 读取配置文件
    val properties = new Properties
    try {
      properties.load(new FileInputStream(new File("conf/settings.properties")))
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }

    // 时间
    val calendar = Calendar.getInstance
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:00")
    //val endTime = simpleDateFormat.format(calendar.getTime)
    val endTime = "2018-10-10 10:20:00"
    calendar.add(Calendar.MINUTE, -20)
    //val startTime = simpleDateFormat.format(calendar.getTime)
    val startTime = "2018-10-10 10:00:00"
    calendar.add(Calendar.MINUTE, -(20 * 71))
    val lastDay = simpleDateFormat.format(calendar.getTime)

    // 静态表名
    val point = "point"
    val center = "center"
    val line = "line"

    // 动态表名
    var hisdata_1 = "hisdata_bj8_slgynm_1"
    var hisdata_2 = "hisdata_bj8_slgynm_2"
    var hisdata_3 = "hisdata_bj8_slgynm_3"
    var hisdataYX = "hisdataYX_bj8_slgynm"
    //var hisdata_1 = ""
    //var hisdata_2 = ""
    //var hisdata_3 = ""
    //var hisdataYX = ""

    // 配置信息
    val options = Map(
      "url" -> "jdbc:mysql://stest:3306/metrosavingexpert",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "thtf106")

    // 读取表数据
    val reader = spark.read.format("jdbc").options(options)

    reader.option("dbtable", point).load().createTempView(point)
    spark.sql(s"select count(*) from `${point}`").show()

    reader.option("dbtable", center).load().createTempView(center)
    spark.sql(s"select count(*) from `${center}`").show()

    reader.option("dbtable", line).load().createTempView(line)
    spark.sql(s"select count(*) from `${line}`").show()

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

    // 查询线路
    //val center_records = null
    val lines_df = spark.sql(s"SELECT lineName,abbreviation FROM `${center}` WHERE eneryMgr=1")
    if (lines_df == null) {
      return
    }
    val lines_count = lines_df.count().toInt
    val lines = lines_df.take(lines_count)
    // 遍历线路
    for (i <- 0 to (lines_count - 1)) {
      lineName = lines(i).getString(0)
      line_abbr = lines(i).getString(1)
      // 查询车站
      val stations_df = spark.sql(s"SELECT stationName,abbreviation FROM `${line}` WHERE lineName='${lineName}'")
      if (stations_df == null) {
        return
      }
      val stations_count = stations_df.count().toInt
      val stations = stations_df.take(stations_count)
      // 遍历车站
      for (i <- 0 to (stations_count - 1)) {
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
        hisdataYX = s"hisdataYX_${line_abbr}_${station_abbr}"
        hisdata_1 = s"hisdata_${line_abbr}_${station_abbr}_1"
        hisdata_2 = s"hisdata_${line_abbr}_${station_abbr}_2"
        hisdata_3 = s"hisdata_${line_abbr}_${station_abbr}_3"
        // 注册变存表
        reader.option("dbtable", hisdataYX).load().filter($"TIME" >= lastDay && $"TIME" <= endTime).createTempView(hisdataYX)
        // 注册24小时定存表
        reader.option("dbtable", hisdata_1).load().filter($"TIME" === lastDay || $"TIME" === endTime).createTempView(hisdata_1)
        reader.option("dbtable", hisdata_2).load().filter($"TIME" === lastDay || $"TIME" === endTime).createTempView(hisdata_2)
        reader.option("dbtable", hisdata_3).load().filter($"TIME" === lastDay || $"TIME" === endTime).createTempView(hisdata_3)
        println(hisdataYX)
        println(hisdata_1)
        println(hisdata_2)
        println(hisdata_3)
        //spark.sql(s"select * from ${hisdataYX}").show()
        //spark.sql(s"select * from ${hisdata_1}").show()
        println(spark.sql(s"select * from ${hisdataYX}").count())
        println(spark.sql(s"select * from ${hisdata_1}").count())
        println(spark.sql(s"select * from ${hisdata_2}").count())
        println(spark.sql(s"select * from ${hisdata_3}").count())
        // 计算各项指标
        // 电耗d1
        area = spark.sql(s"SELECT area FROM `${line}` WHERE lineName='${lineName}' AND stationName='${stationName}'").take(1)(0).getDouble(0)
        // 赋值24小时总能耗
        var typeAndID = selectByRegex(ALL_ZNDB_Regex)
        var IDAndValue = selectInstantByType(typeAndID)
        consumption = assign_powerConsumption(IDAndValue)
        var result = getIndicatorPower()
        if (power_standard > result) {
          d1 = 100
        } else {
          d1 = 100 - (result - power_standard)./(result).*(100)
        }
        // 重新注册20分钟定存表
        reader.option("dbtable", hisdata_1).load().filter($"TIME" === startTime || $"TIME" === endTime).createOrReplaceTempView(hisdata_1)
        reader.option("dbtable", hisdata_2).load().filter($"TIME" === startTime || $"TIME" === endTime).createOrReplaceTempView(hisdata_2)
        reader.option("dbtable", hisdata_3).load().filter($"TIME" === startTime || $"TIME" === endTime).createOrReplaceTempView(hisdata_3)
        // 赋值回风温度
        typeAndID = selectByRegex(HF_T_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        HF_T = assign_instant(IDAndValue)
        // 赋值室内温度
        typeAndID = selectByRegex(ZTX_T_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        ZT_T = assign_instant(IDAndValue)
        typeAndID = selectByRegex(ZTS_T_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        ZT_T = (ZT_T + assign_instant(IDAndValue))./(2)
        // 赋值送风机运行状态
        typeAndID = selectByRegex(HPF_Run_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        SF_run_status = assign_runStatus(IDAndValue)
        // 赋值运行状态
        if (SF_run_status == 0) {
          typeAndID = selectByRegex(KT_Run_Regex)
          IDAndValue = selectInstantByType(typeAndID)
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
          IDAndValue = selectInstantByType(typeAndID)
          consumption = assign_powerConsumption(IDAndValue)
          // 赋值送风机频率
          typeAndID = selectByRegex(KT_Hz_Regex)
          IDAndValue = selectInstantByType(typeAndID)
          KT_Hz = assign_instant(IDAndValue)
          // 赋值回排风机频率
          typeAndID = selectByRegex(HPF_Hz_Regex)
          IDAndValue = selectInstantByType(typeAndID)
          HPF_Hz = assign_instant(IDAndValue)
          // 赋值进风量
          air_yield = getAirYield()
          // 赋值送风温度
          typeAndID = selectByRegex(SF_T_Regex)
          IDAndValue = selectInstantByType(typeAndID)
          SF_T = assign_instant(IDAndValue)
          // 赋值新风温度
          typeAndID = selectByRegex(XF_T_Regex)
          IDAndValue = selectInstantByType(typeAndID)
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
          typeAndID = selectByRegex(KT_ZNDB_Regex)
          IDAndValue = selectInstantByType(typeAndID)
          consumption = assign_powerConsumption(IDAndValue)
          typeAndID = selectByRegex(HPF_ZNDB_Regex)
          IDAndValue = selectInstantByType(typeAndID)
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
        typeAndID = selectByRegex(LD_yield_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        water_yield = assign_instant(IDAndValue)
        // 赋值冷冻水的供、回水温度
        typeAndID = selectByRegex(LD_IT_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        water_IT = assign_instant(IDAndValue)
        typeAndID = selectByRegex(LD_OT_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        water_OT = assign_instant(IDAndValue)
        // 冷机d5
        // 赋值冷机运行状态
        typeAndID = selectByRegex(LSJZ_Run_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        run_status = assign_runStatus(IDAndValue)
        if (run_status == 0) {
          d5 = 0
        } else {
          // 赋值20分钟冷机能耗
          typeAndID = selectByRegex(LSJZ_ZNDB_Regex)
          IDAndValue = selectInstantByType(typeAndID)
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
        typeAndID = selectByRegex(LDB_Run_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        run_status = assign_runStatus(IDAndValue)
        if (run_status == 0) {
          d10 = 0
        } else {
          // 赋值20分钟冷冻泵能耗
          typeAndID = selectByRegex(LDB_ZNDB_Regex)
          IDAndValue = selectInstantByType(typeAndID)
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
        typeAndID = selectByRegex(LQ_yield_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        water_yield = assign_instant(IDAndValue)
        // 赋值冷却水的供、回水温度
        typeAndID = selectByRegex(LQ_IT_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        water_IT = assign_instant(IDAndValue)
        typeAndID = selectByRegex(LQ_OT_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        water_OT = assign_instant(IDAndValue)
        // 冷却泵d15
        // 赋值冷却泵运行状态
        typeAndID = selectByRegex(LQB_Run_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        run_status = assign_runStatus(IDAndValue)
        if (run_status == 0) {
          d15 = 0
        } else {
          // 赋值20分钟冷却泵能耗
          typeAndID = selectByRegex(LQB_ZNDB_Regex)
          IDAndValue = selectInstantByType(typeAndID)
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
        typeAndID = selectByRegex(LT_Run_Regex)
        IDAndValue = selectInstantByType(typeAndID)
        run_status = assign_runStatus(IDAndValue)
        if (run_status == 0) {
          d20 = 0
        } else {
          // 赋值20分钟冷冻塔能耗
          typeAndID = selectByRegex(LT_ZNDB_Regex)
          IDAndValue = selectInstantByType(typeAndID)
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
    // 获取运行状态
    def assign_runStatus(IDAndValue: Array[Row]): Double = {
      // TODO
      if (IDAndValue == null) {
        return 0
      } else {
        return 1
      }
    }
    // 获取累计消耗电量
    def assign_powerConsumption(IDAndValue: Array[Row]): Double = {
      var sumPower: Double = 0
      if (IDAndValue == null || IDAndValue.length == 1) {
        return sumPower
      } else {
        for (i <- 1 to IDAndValue(0).size - 1) {
          sumPower += Math.abs((IDAndValue(0).getDouble(i) - IDAndValue(1).getDouble(i)))
        }
        return sumPower
      }
    }
    // 平均值数据(瞬时流量，进出水温度，温度，频率)
    def assign_instant(IDAndValue: Array[Row]): Double = {
      if (IDAndValue == null) {
        return 0
      } else {
        var value: Double = 0
        val num = IDAndValue(0).size - 1
        for (i <- 1 to num) {
          value += IDAndValue(0).getDouble(i)
        }
        return value./(num)
      }
    }
    // 根据type查询对应表controlID数据
    def selectByType(typeAndID: Array[Row]): Array[Row] = {
      if (typeAndID == null) {
        return null
      }
      var types = ""
      var YC_IDs = ""
      typeAndID.map(row => {
        types = row.getString(0)
        YC_IDs += ("`" + row.get(1).toString() + "`,")
      })
      YC_IDs = YC_IDs.dropRight(1)
      var YX_IDs = YC_IDs.replaceAll("`", "")
      if (types == "YX") {
        // SELECT * FROM `hisdataYX_bj9_qlz` WHERE ID IN (99,106,113,120,127);
        val result = spark.sql(s"SELECT * FROM `${hisdataYX}` WHERE ID IN (${YX_IDs})")
        // TODO 根据Hbase表结构
      } else if (types == "YC") {
        // SELECT TIME,`77`,`84`,`133` FROM `hisdata_bj9_qlz_1`;
        val result = spark.sql(s"SELECT TIME,${YC_IDs} FROM `${hisdata_1}`")
        // TODO 根据Hbase表结构
      }
      return null
    }
    // 根据type查询对应表controlID定存数据
    def selectInstantByType(typeAndID: Array[Row]): Array[Row] = {
      if (typeAndID == null) {
        return null
      }
      var YC_IDs = ""
      typeAndID.map(row => {
        YC_IDs += ("`" + row.get(1).toString() + "`,")
      })
      YC_IDs = YC_IDs.dropRight(1)
      // SELECT TIME,`77`,`84`,`133` FROM `hisdata_bj9_qlz_1`;
      val result = spark.sql(s"SELECT TIME,${YC_IDs} FROM `${hisdata_1}` WHERE TIME = '${endTime}'")
      // TODO 根据Hbase表结构
      return null
    }
    // 根据regex查询type和controlID
    def selectByRegex(regex: String): Array[Row] = {
      val result = spark.sql(s"SELECT type,controlID FROM `${point}` WHERE lineName='${lineName}' AND stationName='${stationName}' AND pointName LIKE '${regex}'")
      if (result == null) {
        return null
      } else {
        val stations_count = result.count().toInt
        result.take(stations_count)
      }
    }

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