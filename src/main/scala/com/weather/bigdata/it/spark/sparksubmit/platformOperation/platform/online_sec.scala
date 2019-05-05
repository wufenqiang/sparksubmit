package com.weather.bigdata.it.spark.sparksubmit.platformOperation.platform

object online_sec {
  def main(args:Array[String]): Unit ={
    val region_source=args(0)
    mv_platformLib_core.cpJars_online_sec(region_source)
  }
}
