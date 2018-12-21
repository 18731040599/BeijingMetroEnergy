package com.thtf.sparktest

import org.apache.hadoop.hbase.util.Bytes
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import java.io.File
import java.io.FilenameFilter
import java.util.Arrays
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import scala.collection.mutable.ArrayBuffer
import java.util.Properties
import java.io.FileInputStream
import java.util.UUID

case class MyUser(
  name: String,
  age: Int)
object MyTest {
  def main(args: Array[String]): Unit = {

    try {
      val i = 1 / 0
      println("start")
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    println("done")

    val dateStr = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date)
    val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr)
    println(new Date)
    println(dateStr)
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:00")
    val calendar = Calendar.getInstance
    println(simpleDateFormat.format(calendar.getTime))
    println("Calendar.MINUTE"+Calendar.MINUTE)
    calendar.add(Calendar.MINUTE, -calendar.get(Calendar.MINUTE)%20)
    val endTime = simpleDateFormat.format(calendar.getTime)
    //Thread.sleep(1000*60)
    calendar.add(Calendar.MINUTE, -20)
    val startTime = simpleDateFormat.format(calendar.getTime)
    calendar.add(Calendar.MINUTE, -(20 * 71))
    val lastDay = simpleDateFormat.format(calendar.getTime)
    println(endTime)
    println(startTime)
    println(lastDay)
    println(endTime == "2018-11-14 15:37:00")
    println(-15%20)
    println("=======================================")

    val map = Map("x" -> "x")

    val s = s"""row${"%03d".format(1)}"""
    val s1 = s"""row${"%010d".format(4)}"""
    println(s)
    println(s1)
    
    val file = new File("F:/java/mysql")
    val files = file.list(new FilenameFilter(){
      def accept(dir: File, name: String): Boolean = {
        if(name.startsWith("p")){
          return true
        }
        return false
      }
    })
    files.map(println(_))
    
    val aaa = "aaa.txt"
    println(aaa.dropRight(4))
    println(aaa.takeRight(4))
    println(aaa)
    val rowkey = "qlz2018-08-10 00:20:00"
    println("qlz".hashCode())
    println("slgynm".hashCode())
    println(rowkey.length())
    println(rowkey.replaceAll("[^0-9]", ""))
    println('9'.toInt)
    println(58.toChar)
    println('|'.toInt)
    println(date.getTime)
    println(Int.MaxValue)
    println(Long.MaxValue)
    println()
    val time = "2018-08-10 00:20:00"
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:00")
    val da = sdf.parse(time)
    val da2 = new Date(0l)
    println(da)
    println(sdf.format(da2))
    
    
    println("array")
    var arr = new Array[Any](10)
    arr = arr.map(a => {"aaa"})
    println(arr.toList)
    
    println("qlz2018-08-10 00:40:00".matches(".*(2018-08-10 00:00:00|2018-08-10 00:20:00){1}"))
    println("qlz20180810004000".dropRight(14))
    println("qlz20180810004000".takeRight(14))
    
    var num = -2 to 100
    var array = (-2 to 100).toArray
    println(num)
    println(Arrays.toString(array))
    
    
    val kpi_real = Array[String]("d1", "d2", "d3", "d4", "d5", "d10", "d15", "d20", "d24")

    val a = Array("1","2")
    val b = Array("3","4")
    Array.concat(a,b)
    
    println("7.8753e-43".toDouble.*(10))
    
    val KPI_fields = ArrayBuffer[String]("abbreviation","time","d1","d2","d3","d4","d5","d10","d15","d20","d24")
    val kkk = KPI_fields.clone()
    //                              0      1    2      3      4      5    6       7
    val KPIs_real = Array(endTime, "d2", "d3", "d4", "d5", "d10", "d15", "d20", "d24")
    val KPIs_day = Array(endTime,"d1", "d2", "d3", "d4", "d5", "d10", "d15", "d20", "d24")
    
    val startnum = KPI_fields.length - (KPIs_day.length - 1)
        for (i <- 0 until KPIs_day.length - 1) {
          println(KPI_fields(i + startnum)+KPIs_day(i + 1))
        }
    KPI_fields.remove(2)
    for(z <- kkk){
      print(z + "~")
    }
    
    println(null)
    
    // 读取配置文件
    val properties = new Properties
    try {
      properties.load(new FileInputStream(new File("conf/params.properties")))
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    println(properties.getProperty("water_specificHeat"))
    
    
    val numarr = ArrayBuffer[String]("1","2")
    for(i <- 3 to 5){
      numarr += i+""
    	println(numarr)
    }
    println(numarr)
    println(UUID.randomUUID())
    
    println("yzl20171213104000".matches(".*(20171212104000|20171213102000|20171213104000){1}"))
    
    val value = Bytes.toString(null)
    println(value)
    println(value == null)
    println(value == "null")
    
    
    
    /*
    val str = "123,,12,33"
    str.split(",").foreach(println(_))

    val s = s"""row${"%03d".format(1)}"""
    println(s)

    def i = 1
    println(i + "" + i.getClass())

    val t1 = "123,456,789"
    val t2 = """This is
               |a multiline
               |String""".stripMargin
    println(t1)
    println(t2)

    println("----------")
    println("f:age".hashCode())
    println("rowkey".hashCode())
    println("f:name".hashCode())
    println("f:password".hashCode())
    */
    
    /*
     val columns_type = Map[String,String]("id" -> "int",
                           "name" -> "string",
                           "password" -> "string",
                           "age" -> "int")
    println(columns_type.get("id"))
    println(columns_type.getOrElse("id",""))

    Bytes.toBytes(16).foreach(x => {print(x.toChar.toString())})
    */

    /*
    // 随机数
    for(i <- 1 to 10){
      val f = scala.util.Random.nextFloat().*(100)
    	println(i + "~" + f)
    }
    */

  }
}