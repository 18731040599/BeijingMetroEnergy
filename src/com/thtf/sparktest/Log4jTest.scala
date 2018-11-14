package com.thtf.sparktest

import org.apache.log4j.Logger

object Log4jTest {
  def main(args: Array[String]): Unit = {
    //PropertyConfigurator.configure( "log4j.properties" );
    val logger  =  Logger.getLogger(Log4jTest.getClass);
    logger.debug( "debug" );
    logger.error( "error" );
    logger.info( "info" );
  }
}