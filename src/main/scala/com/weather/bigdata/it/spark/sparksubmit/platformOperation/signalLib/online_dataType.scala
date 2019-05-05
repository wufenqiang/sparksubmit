package com.weather.bigdata.it.spark.sparksubmit.platformOperation.signalLib

object online_dataType {
  def main(args:Array[String]): Unit ={
    val dataType=args(0)
    mv_signalLib_core.cpJars_online(dataType)
  }
}
