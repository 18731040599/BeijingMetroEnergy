package com.thtf.sparktest

object MoneyTest {
  def main(args: Array[String]): Unit = {
    var sum = 0d
    var day = 0
    val first = 4
    for(i <- 1 to 44){
      var each = 0d
      if(sum < 100){
    	  each = first
      }else if(sum >= 100 && sum < 150){
        each = 0.8*first
      }else if(sum >= 150){
        each = 0.5*first
      }
      sum += each
      println("第" + i + "次," + each + "," + sum)
    }
    println("sum:"+sum)
  }
}