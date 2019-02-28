package com.thtf.entity

import com.thtf.util.PropertiesUtils
import java.util.Properties
import java.io.FileInputStream
import java.io.File


object test {
  def main(args: Array[String]): Unit = {
    val t = new DateEntity
    val p = new Properties()
    try {
    	p.load(new FileInputStream(new File("conf/defaults.properties")))
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    }
    println(p.toString())
    println(p.getProperty("aaaaaaaaaaaaa"))
  }
}