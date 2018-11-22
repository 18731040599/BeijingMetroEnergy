package com.thtf.sparktest

import org.apache.hadoop.hbase.util.Bytes
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import java.io.File
import java.io.FilenameFilter

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
    val calendar = Calendar.getInstance
    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:m:00")
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
    aaa.dropRight(4)
    println(aaa)
    
    
    
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