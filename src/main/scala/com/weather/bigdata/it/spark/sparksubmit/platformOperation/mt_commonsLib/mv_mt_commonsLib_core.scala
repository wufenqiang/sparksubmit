package com.weather.bigdata.it.spark.sparksubmit.platformOperation.mt_commonsLib

import com.weather.bigdata.it.spark.sparksubmit.platformOperation.{mvjar_core, platformRole}
import com.weather.bigdata.it.spark.sparksubmit.sparksubmit_PropertiesUtil

private object mv_mt_commonsLib_core {
  private val regionKey_online:String=platformRole.getregionKey_online
  private val regionKey_online_sec:String=platformRole.getregionKey_online_sec

  //  def cpJars_splitFile(splitFile_source: String,splitFile_target: String): Unit ={
  //    val region_source=ConfUtil.getRegion(splitFile_source)
  //    val region_target=ConfUtil.getRegion(splitFile_target)
  //    this.cpJars(region_source,region_target)
  //  }
  /**
    *
    * @param region_source
    * @param region_target
    * @return
    */
  private def cpJars_regionKey(region_source:String,region_target:String):Boolean={
    val mt_commonsLib_source:String=sparksubmit_PropertiesUtil.getmt_commonsLibPath_regionKey(region_source)
    val mt_commonsLib_target:String=sparksubmit_PropertiesUtil.getmt_commonsLibPath_regionKey(region_target)
    mvjar_core.cpJars(mt_commonsLib_source,mt_commonsLib_target)
  }
  def cpJars_online(): Boolean ={
    this.cpJars_regionKey(this.regionKey_online,this.regionKey_online_sec)
  }
  def cpJars_online_sec(region_source:String):Boolean={
    this.cpJars_regionKey(region_source,this.regionKey_online_sec)
  }
  def main(args:Array[String]): Unit ={
    val region_source=args(0)
    val region_target=args(1)
    if(args.length==2){
      if(region_target.equals(this.regionKey_online) && (!region_source.equals(this.regionKey_online_sec))){
        val msg="设置不可通过region_source="+region_source+"->region_target"+region_target
        sparksubmit_PropertiesUtil.log_sparksubmit.error(msg)
      }else{
        this.cpJars_regionKey(region_source,region_target)
      }
    }
  }
}
