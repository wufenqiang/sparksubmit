package com.weather.bigdata.it.spark

import java.util.Date

import com.weather.bigdata.it.spark.platform.signal.{JsonStream, anaAttribute}
import com.weather.bigdata.it.utils.hdfsUtil.HDFSReadWriteUtil

object ResourceConf {
  private val splitNum0Key="splitNum0"
  private val splitNum1Key="splitNum1"
  private val splitNum2Key="splitNum2"

  private val eachpartition:Int=PropertiesUtil.prop.getProperty("eachpartition").toInt

  //driverMemorry,executorMemorry,executorNum
  def getResource(signalMsg: String, splitFile: String):(String,String,String)={
    val (dataType: String, applicationTime: Date, generationFileName: String, timeStamp: Date, attribute: String) = JsonStream.analysisJsonStream(signalMsg)

    val memorryStr:String={
      if(PropertiesUtil.prop.containsKey(dataType)){
        PropertiesUtil.prop.getProperty(dataType)
      }else{
        "3g,3g,8"
      }
    }
    val memorryInfo:Array[String]=memorryStr.split(",")
    val (driverMemorry0:String,executorMemorry0:String,executorNum0:String)={
      if(memorryInfo.length==3){
        val driverMemorry1=memorryInfo(0)
        val executorMemorry1=memorryInfo(1)
        val NumStr=memorryInfo(2)
        val Num1:Int={
          if(NumStr.equals(this.splitNum0Key)){
            val splitNum0 = HDFSReadWriteUtil.readTXT(splitFile).length
            splitNum0
          }else if(NumStr.equals(this.splitNum1Key)){
            val splitNum0 = HDFSReadWriteUtil.readTXT(splitFile).length
            val splitNum1 = splitNum0 + this.partitionCoefficient(splitNum0)
            splitNum1
          }else if(NumStr.equals(this.splitNum2Key)){
            val timeSteps=anaAttribute.getTimeSteps(attribute)
            val splitNum2=this.reduceLatLonfromtimeSteps(timeSteps)
            splitNum2
          }else{
            NumStr.toInt
          }
        }
        (driverMemorry1,executorMemorry1,Num1.toString)
      }else{
        val memorryDefault=PropertiesUtil.prop.getProperty("memorryDefault")
        val msg="没有配置" + dataType + "的资源类型,("+memorryStr+")使用默认配置"+memorryDefault
        PropertiesUtil.log.warn(msg)
        val memorryDefaultSplit=memorryDefault.split(",")
        (memorryDefaultSplit(0),memorryDefaultSplit(1),memorryDefaultSplit(2))
      }
    }

    (driverMemorry0:String,executorMemorry0:String,executorNum0:String)
  }

  def partitionCoefficient(splitNum:Int):Int=(splitNum/this.eachpartition)+1

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
}
