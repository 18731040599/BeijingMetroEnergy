package com.thtf.sparktest

import org.apache.hadoop.hbase.util.Bytes
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar

case class MyUser(
    name: String,
    age: Int
    )
object MyTest {
  def main(args: Array[String]): Unit = {
    
    println("—" == "—")
    
    val x = "bc"
    val y = "e"
    val z = s"${x}d${y}"
    println(z)
    
   try {
     val i = 1/0
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
        val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:00")
        val endTime = simpleDateFormat.format(calendar.getTime)
        //Thread.sleep(1000*60)
        calendar.add(Calendar.MINUTE, -20)
        val startTime = simpleDateFormat.format(calendar.getTime)
        calendar.add(Calendar.MINUTE, -(20*71))
        val lastDay = simpleDateFormat.format(calendar.getTime)
        println(endTime)
        println(startTime)
        println(lastDay)
        println(endTime == "2018-11-14 15:37:00")
        
   val map = Map("x" -> "x")
   
   println(100./(20).*(5))
   println(Math.abs(-6))
   
   val a = MyUser.apply("Li", 18)
   println(a)
   val b = MyUser.apply("Wang", 20)
   println(a)
   println(b)
    
    /*
    val str = "123,,12,33"
    str.split(",").foreach(println(_))
    
    val line = "九号线	七里庄	KT_A1_FreqFB_Hz	YC	0	KT_A1	A1送风机	DI	1		1	0	50	送风机1频率反馈".split("\t")
    println(line.length)
    
    
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
    
    /*//val point = Bytes.toInt(null);
    val a = Array(1, 2, 3)
    val b = a.map(x => x + 1)
    b.foreach(x => print(x))*/

    /*
    for(i <- 1 to 10){
      val f = scala.util.Random.nextFloat().*(100)
    	println(i + "~" + f)
    }
    */
    
    /*
    var x = 0
    var sum:Double = 0
    while (x < 60) {
      if (sum < 100) {
        sum += 8
        x += 1
        println(x + "-" + sum)
      }
      if (sum < 150 && sum > 100) {
        sum += 6.4d
        x += 1
        println(x + "-" + sum)
      }
      if (sum >= 150) {
        sum += 4
        x += 1
        println(x + "-" + sum)
      }
    }
    */
    
    
    
  }
}