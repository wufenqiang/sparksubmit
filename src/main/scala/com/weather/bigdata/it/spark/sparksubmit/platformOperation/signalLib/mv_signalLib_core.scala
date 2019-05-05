package com.weather.bigdata.it.spark.sparksubmit.platformOperation.signalLib

import com.weather.bigdata.it.spark.sparksubmit.platformOperation.{mvjar_core, platformRole}
import com.weather.bigdata.it.spark.sparksubmit.sparksubmit_PropertiesUtil

private object mv_signalLib_core {
  private val regionKey_online:String=platformRole.getregionKey_online
  private val regionKey_online_sec:String=platformRole.getregionKey_online_sec
  //  def cpJars_signalMsgJSON(splitFile_source: String,splitFile_target: String,signalMsgJSON:JSONObject): Boolean ={
  //    val dataType:String=JsonStream.analysisDataType(signalMsgJSON)
  //    this.cpJars_dataType(splitFile_source,splitFile_target,dataType)
  //  }
  //  def cpJars_signalMsg(splitFile_source: String,splitFile_target: String,signalMsg:String): Boolean ={
  //    this.cpJars_signalMsgJSON(splitFile_source,splitFile_target,JSON.parseObject(signalMsg))
  //  }
  //  def cpJars_signalFile(splitFile_source: String,splitFile_target: String,signalFile:String): Boolean ={
  //    val signalMsgs=HDFSReadWriteUtil.readTXT(signalFile)
  //    signalMsgs.map(signalMsg=>{
  //      this.cpJars_signalMsg(splitFile_source,splitFile_target,signalFile)
  //    }).reduce((x,y)=>(x && y))
  //  }
  //  def cpJars_dataType(splitFile_source: String,splitFile_target: String,dataType:String): Boolean ={
  //    val region_source=ConfUtil.getRegion(splitFile_source)
  //    val region_target=ConfUtil.getRegion(splitFile_target)
  //
  //    this.cpJars_regionKey_dataType(region_source,region_target,dataType)
  //  }

  /**
    *
    * @param region_source
    * @param region_target
    * @param dataType
    * @return
    */
  private def cpJars_regionKey_dataType(region_source: String,region_target: String,dataType:String): Boolean ={
    val signalLib_source:String=sparksubmit_PropertiesUtil.getsignallibPath_dataType_regionKey(dataType,region_source)
    val signalLib_target:String=sparksubmit_PropertiesUtil.getsignallibPath_dataType_regionKey(dataType,region_target)
    mvjar_core.cpJars(signalLib_source,signalLib_target)
    //    val signalLib_source_files=HDFSOperation1.listChildrenAbsoluteFile(signalLib_source)
    //    signalLib_source_files.map(signalLib_source_file=>{
    //      val signalLib_target_file=signalLib_source_file.replace(signalLib_source,signalLib_target)
    //      HDFSOperation1.copyfile(signalLib_source_file,signalLib_target_file,true,false)
    //    }).reduce((x,y)=>(x && y))
  }
  def cpJars_online(dataType:String): Boolean ={
    this.cpJars_regionKey_dataType(this.regionKey_online_sec,this.regionKey_online,dataType)
  }
  def cpJars_online_sec(region_source: String,dataType:String):Boolean={
    this.cpJars_regionKey_dataType(region_source,this.regionKey_online_sec,dataType)
  }
  def cpJars_online_All(): Boolean ={
    val eles=sparksubmit_PropertiesUtil.eles_regionKey(this.regionKey_online_sec)
    eles.map(dataType=>{
      this.cpJars_online(dataType)
    }).reduce((x,y)=>(x && y))
  }
  def main(args:Array[String]): Unit ={
    if(args.length==2){
      val region_source=args(0)
      val region_target=args(1)
      if(region_target.equals(this.regionKey_online) && (!region_source.equals(this.regionKey_online_sec))){
        val msg="设置不可通过region_source="+region_source+"->region_target"+region_target
        sparksubmit_PropertiesUtil.log_sparksubmit.error(msg)
      }else{
        val eles=sparksubmit_PropertiesUtil.eles_regionKey(region_source)
        eles.map(dataType=>{
          this.cpJars_regionKey_dataType(region_source,region_target,dataType)
        })
      }
    }else if(args.length==3){
      val region_source=args(0)
      val region_target=args(1)
      val dataType=args(2)
      if(region_target.equals(this.regionKey_online) && (!region_source.equals(this.regionKey_online_sec))){
        val msg="设置不可通过region_source="+region_source+"->region_target"+region_target
        sparksubmit_PropertiesUtil.log_sparksubmit.error(msg)
      }else{
        this.cpJars_regionKey_dataType(region_source,region_target,dataType)
      }
    }
  }
}
