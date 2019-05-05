package com.weather.bigdata.it.spark.sparksubmit.platformOperation.mt_commonsLib

object online_sec {
  def main(args:Array[String]): Unit ={
    val region_source=args(0)
    mv_mt_commonsLib_core.cpJars_online_sec(region_source)
  }
}
