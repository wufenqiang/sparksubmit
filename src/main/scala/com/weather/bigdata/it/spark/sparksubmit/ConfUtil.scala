package com.weather.bigdata.it.spark.sparksubmit

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.weather.bigdata.it.spark.platform.signal.{JsonStream, anaAttribute}
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.it.spark.sparksubmit.platformOperation.platformRole
import com.weather.bigdata.it.utils.hdfsUtil.HDFSReadWriteUtil

private object ConfUtil {
  //  #splitNum0分块文件数;
  //  #splitNum1理论最优入库数量splitNum1=splitNum0+(splitNum0/eachpartition)+1,eachpartition=5(见下)
  //  #splitNum2根据timeStep计算的reduceLatLon的个数,仅针对reduceLatLon
  private val splitNum0Key="splitNum0"
  private val splitNum1Key="splitNum1"
  private val splitNum2Key="splitNum2"
  private val defaultMem:(String,String,Int)=("1g","1g",3)

  private def eachpartition(signalMsg: String,splitFile:String): Int = {
    this.eachpartition(JSON.parseObject(signalMsg),splitFile)
  }
  private def eachpartition(signalMsgJSON: JSONObject, splitFile: String): Int = {
    sparksubmit_PropertiesUtil.eachpartition(signalMsgJSON,splitFile)
  }


  private val mbUnit:Array[String]=Array("M","MB","m","mb")
  private val gbUnit:Array[String]=Array("G","GB","g","gb")

  //解析输入的资源值
  private def splitMem(memStr0:String): (Double,String) ={
    val memStr=memStr0.toUpperCase
    val mems:Double=memStr.split("\\D").head.toDouble
    val unit:String=memStr.split("\\d").last
    //    println(mem.toUpperCase)
    sparksubmit_PropertiesUtil.log_monitorHDFS.info("memStr0="+memStr0+"  ==>("+mems+","+unit+")")
    (mems,unit)
  }
  //非线上资源限制
  private def down2MemLimit(splitFile:String,memorrys:(String,String,Int)):(String,String,Int) ={
    val region=Analysis.getRegion(splitFile)
    if(!region.equals(platformRole.getregionKey_online)){
      val driverMemorry0=memorrys._1
      val executorMemorry0=memorrys._2
      val executorNum0=memorrys._3

      val memLimit:String=sparksubmit_PropertiesUtil.memLimit(splitFile)
      val (memLimit_Num0:Double,memLimit_Unit0:String)=this.splitMem(memLimit)

      val (driverMem_Num0:Double,driverMem_Unit0:String)=this.splitMem(driverMemorry0)
      val (executorMem_Num0:Double,executorMem_Unit0:String)=this.splitMem(executorMemorry0)

      val ismb_memLimit=this.mbUnit.contains(memLimit_Unit0)
      val isgb_memLimit=this.gbUnit.contains(memLimit_Unit0)

      if(isgb_memLimit.equals(ismb_memLimit)){
        val msg="memLimit="+memLimit+";单位转换判断有误.ismb_memLimit="+ismb_memLimit+ ";isgb_memLimit="+isgb_memLimit+";取默认值:"+defaultMem+";规范输入可通过"
        sparksubmit_PropertiesUtil.log_monitorHDFS.error(msg)
        this.defaultMem
      }else{
        val memLimit_Num1:Double={
          if(ismb_memLimit){
            memLimit_Num0
          }else{
            memLimit_Num0 * 1024.0d
          }
        }
        val ismb_driverMem_Num0=this.mbUnit.contains(driverMem_Unit0)
        val isgb_driverMem_Num0=this.gbUnit.contains(driverMem_Unit0)
        if(ismb_driverMem_Num0.equals(isgb_driverMem_Num0)){
          val msg="driverMemorry0="+driverMemorry0+";单位转换判断有误.ismb_memLimit="+ismb_memLimit+ ";isgb_memLimit="+isgb_memLimit+";取默认值:"+defaultMem+";规范输入可通过"
          sparksubmit_PropertiesUtil.log_monitorHDFS.error(msg)
          this.defaultMem
        }else{
          val driverMem_Num1:Double={
            if(ismb_driverMem_Num0){
              driverMem_Num0
            }else{
              driverMem_Num0 * 1024.0d
            }
          }
          val ismb_executorMem_Num0=this.mbUnit.contains(executorMem_Unit0)
          val isgb_executorMem_Num0=this.gbUnit.contains(executorMem_Unit0)
          if(ismb_executorMem_Num0.equals(isgb_executorMem_Num0)){
            val msg="executorMemorry0="+executorMemorry0+";单位转换判断有误.ismb_memLimit="+ismb_memLimit+ ";isgb_memLimit="+isgb_memLimit+";取默认值:"+defaultMem+";规范输入可通过"
            sparksubmit_PropertiesUtil.log_monitorHDFS.error(msg)
            this.defaultMem
          }else{
            val executorMem_Num1:Double={
              if(ismb_executorMem_Num0){
                executorMem_Num0
              }else{
                executorMem_Num0 *1024.0d
              }
            }
            val totalMem_mb=executorMem_Num1*executorNum0+driverMem_Num1
            val totalMem_gb=totalMem_mb/1024.0d
            val memLimit_Num1_mb=memLimit_Num1
            val memLimit_Num1_gb=memLimit_Num1/1024.0d
            if( totalMem_mb <= memLimit_Num1){
              val msg="申请资源量为:"+memorrys+"="+totalMem_mb+"mb("+totalMem_gb+"gb);符合要求<="+memLimit_Num1_mb+"mb"+"("+memLimit_Num1_gb+"gb)"
              sparksubmit_PropertiesUtil.log_monitorHDFS.info(msg)
              memorrys
            }else{
              val msg="非线上区域,申请资源过载,请调低资源量或与it人员联系。本次运行取默认值:"+defaultMem
              sparksubmit_PropertiesUtil.log_monitorHDFS.error(msg)
              this.defaultMem
            }
          }
        }
      }
    }else{
      val msg="线上区域不做资源检测"
      sparksubmit_PropertiesUtil.log_monitorHDFS.info(msg)
      memorrys
    }
  }


  //open,driverMemorry,executorMemorry,executorNum,mainClass
  private def getdataTypeConf(signalMsg: String, splitFile: String):(Boolean,String,String,Int,String)={
    this.getdataTypeConf(JSON.parseObject(signalMsg), splitFile)
  }
  private def getdataTypeConf (signalMsgJSON: JSONObject, splitFile: String): (Boolean,String, String, Int, String) = {
    val (dataType: String, applicationTime: Date, generationFileName: String, timeStamp: Date, attribute: String) = JsonStream.analysisJsonStream(signalMsgJSON)

    val dataTypeConfStr = sparksubmit_PropertiesUtil.dataTypeConf(signalMsgJSON,splitFile)
    val dataTypeInfo:Array[String]=dataTypeConfStr.split(",")
    val (dataTypeOpen0:Boolean,driverMemorry0:String,executorMemorry0:String,executorNum0:Int,mainClass0:String)={
      if(dataTypeInfo.length==5){
        val dataTypeOpen1=dataTypeInfo(0).toBoolean
        val driverMemorry1=dataTypeInfo(1)
        val executorMemorry1=dataTypeInfo(2)
        val NumStr=dataTypeInfo(3)
        val mainClass1=dataTypeInfo(4)
        val Num1:Int={
          if(NumStr.equals(this.splitNum0Key)){
            val splitNum0 = HDFSReadWriteUtil.readTXT(splitFile).length
            splitNum0
          }else if(NumStr.equals(this.splitNum1Key)){
            val splitNum0 = HDFSReadWriteUtil.readTXT(splitFile).length
            val splitNum1 = splitNum0 + this.partitionCoefficient(splitNum0,signalMsgJSON,splitFile)
            splitNum1
          }else if(NumStr.equals(this.splitNum2Key)){
            val timeSteps=anaAttribute.getTimeSteps(attribute)
            val splitNum2=this.reduceLatLonfromtimeSteps(timeSteps)
            splitNum2
          }else{
            NumStr.toInt
          }
        }
        (dataTypeOpen1,driverMemorry1,executorMemorry1,Num1,mainClass1)
      }else{
        val msg=dataType + "的资源配置错误,("+dataTypeConfStr+")"
        sparksubmit_PropertiesUtil.log_sparksubmit.error(msg)
        (false,"","",0,"")
      }
    }

    val (driverMemorry1,executorMemorry1,executorNum1)=this.down2MemLimit(splitFile,(driverMemorry0,executorMemorry0,executorNum0))


    (dataTypeOpen0,driverMemorry1,executorMemorry1,executorNum1:Int,mainClass0)
  }

  def getdataTypeOpen(signalMsgJSON: JSONObject, splitFile: String):Boolean=this.getdataTypeConf(signalMsgJSON,splitFile)._1
  def getdataTypeOpen(signalMsg: String, splitFile: String):Boolean=this.getdataTypeConf(signalMsg, splitFile)._1
  def getdataTypeResource(signalMsgJSON: JSONObject, splitFile: String):(String,String,Int)={
    val f=this.getdataTypeConf(signalMsgJSON,splitFile)
    (f._2,f._3,f._4)
  }
  def getdataTypeResource(signalMsg: String, splitFile: String):(String,String,Int)={
    val f=this.getdataTypeConf(signalMsg,splitFile)
    (f._2,f._3,f._4)
  }
  def getdataTypeMainClass(signalMsg: String, splitFile: String):String=this.getdataTypeConf(signalMsg,splitFile)._5
  def getdataTypeMainClass(signalMsgJSON: JSONObject, splitFile: String):String=this.getdataTypeConf(signalMsgJSON,splitFile)._5

  def partitionCoefficient(splitNum:Int,signalMsg: String,splitFile:String):Int={
    this.partitionCoefficient(splitNum,JSON.parseObject(signalMsg),splitFile)
  }
  def partitionCoefficient(splitNum:Int,signalMsgJSON: JSONObject,splitFile:String):Int={
    (splitNum/this.eachpartition(signalMsgJSON,splitFile))+1
  }

  private def reduceLatLonfromtimeSteps(timeSteps:Array[Double]):Int={
    if(timeSteps.contains(1.0d) && timeSteps.contains(12.0d)){
      4
    }else if(timeSteps.contains(1.0d) && !timeSteps.contains(12.0d)){
      3
    }else if(!timeSteps.contains(1.0d) && timeSteps.contains(12.0d)){
      1
    }else{
      0
    }
  }

  //  private var region0:String=null
  //  def getRegion(splitFile:String): String ={
  //    if(this.region0==null){
  //      val uri = URI.create(splitFile)
  //      val pathName=new Path(uri)
  //      val kkk={
  //        if(HDFSConfUtil.isLocal(splitFile)){
  //          pathName.getParent.getName
  //        }else{
  //          pathName.getName
  //        }
  //      }
  //      this.region0=kkk.split("\\.").head
  //    }
  //    this.region0
  //  }

//  def getRegion(splitFile:String): String ={
//    val uri = URI.create(splitFile)
//    val pathName=new Path(uri)
//    val kkk={
//      if(HDFSConfUtil.isLocal(splitFile)){
//        pathName.getParent.getName
//      }else{
//        pathName.getName
//      }
//    }
//    kkk.split("\\.").head
//  }
}
