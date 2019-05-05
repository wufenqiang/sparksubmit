package com.weather.bigdata.it.spark.sparksubmit.platformOperation.signalLib

object online_sec {
  def main(args:Array[String]): Unit ={
    val region_source=args(0)
    val dataType=args(1)
    mv_signalLib_core.cpJars_online_sec(region_source,dataType)
  }
}
