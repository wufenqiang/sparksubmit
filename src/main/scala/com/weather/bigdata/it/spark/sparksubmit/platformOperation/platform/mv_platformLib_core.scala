package com.weather.bigdata.it.spark.sparksubmit.platformOperation.platform

import com.weather.bigdata.it.spark.sparksubmit.platformOperation.{mvjar_core, platformRole}
import com.weather.bigdata.it.spark.sparksubmit.sparksubmit_PropertiesUtil

private object mv_platformLib_core {
  private val regionKey_online:String=platformRole.getregionKey_online
  private val regionKey_online_sec:String=platformRole.getregionKey_online_sec
  //  def cpJars_splitFile(splitFile_source: String,splitFile_target: String): Unit ={
  //    val region_source=ConfUtil.getRegion(splitFile_source)
  //    val region_target=ConfUtil.getRegion(splitFile_target)
  //    this.cpJars_regionKey(region_source,region_target)
  //  }
  /**
    *
    * @param region_source
    * @param region_target
    */
  private def cpJars_regionKey(region_source:String,region_target:String): Boolean ={
    val platfromLib_source:String=sparksubmit_PropertiesUtil.getplatformlibPath_regionKey(region_source)
    val platfromLib_target:String=sparksubmit_PropertiesUtil.getplatformlibPath_regionKey(region_target)

    mvjar_core.cpJars(platfromLib_source,platfromLib_target)
//    val platfromLib_source_files=HDFSOperation1.listChildrenAbsoluteFile(platfromLib_source)
//    platfromLib_source_files.map(platfromLib_source_file=>{
//      val platformLib_target_file=platfromLib_source_file.replace(platfromLib_source,platfromLib_target)
//      HDFSOperation1.copyfile(platfromLib_source_file,platformLib_target_file,true,false)
//    }).reduce((x,y)=>(x && y))
  }
  def cpJars_online(): Boolean ={
    this.cpJars_regionKey(this.regionKey_online_sec,this.regionKey_online)
  }
  def cpJars_online_sec(region_source:String): Boolean ={
    this.cpJars_regionKey(region_source,this.regionKey_online_sec)
  }
  def main(args:Array[String]): Unit ={
    if(args.length==2){
      val region_source=args(0)
      val region_target=args(1)
      if(region_target.equals(this.regionKey_online) && (!region_source.equals(this.regionKey_online_sec))){
        val msg="设置不可通过region_source="+region_source+"->region_target"+region_target
        sparksubmit_PropertiesUtil.log_sparksubmit.error(msg)
      }else{
        this.cpJars_regionKey(region_source,region_target)
      }
    }
  }
}
