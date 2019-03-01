package com.thtf.entity

import com.thtf.util.PropertiesUtils
import java.util.Properties
import java.io.FileInputStream
import java.io.File
import java.text.SimpleDateFormat


object test {
  def main(args: Array[String]): Unit = {
    val t = new DateEntity
    
    // println(p.toString())
    // println(p.getProperty("aaaaaaaaaaaaa"))
    println(Long.MaxValue)
    //println(new SimpleDateFormat("yyyyMMddHHmmss").parse("20180304112000"))
    //println(new SimpleDateFormat("yyyyMMddHHmm00").parse("20180304112123"))
    
    println(test.getClass.getResource("../../"))
    println(test.getClass.getResource("../../../defaults.properties"))
    
    val pp = test.getClass.getResource("").toString()
    println(pp)
    println(pp.substring(0,pp.lastIndexOf("/")+1))
    println(pp.substring(0,pp.lastIndexOf("/")+1-16))
    val ppp = pp.substring(pp.indexOf("/"),pp.lastIndexOf("/")+1-16)
    // println(new File(ppp).listFiles().map(f=>f.getAbsolutePath).toString())
    val p = new Properties()
    try {
    	p.load(new FileInputStream(new File(ppp+"defaults.properties")))
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    
    
  }
}