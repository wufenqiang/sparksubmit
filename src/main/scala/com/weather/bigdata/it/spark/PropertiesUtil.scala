package com.weather.bigdata.it.spark

import java.io.{File, FileInputStream, InputStream}
import java.net.URI
import java.util.{Date, Properties}

import com.weather.bigdata.it.spark.platform.signal.JsonStream
import com.weather.bigdata.it.utils.hdfsUtil.HDFSConfUtil
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger

private object PropertiesUtil {
  val log:Logger=Logger.getRootLogger
  private val prop: java.util.Properties = {
    val prop0: Properties = new java.util.Properties( )
    val loader = Thread.currentThread.getContextClassLoader()
    val loadfile: InputStream=loader.getResourceAsStream("sparksubmit_config.properties")
    prop0.load(loadfile)
    this.log.info("load sparksubmit_config.properties")
    this.log.info(prop0)
    prop0
  }

  val sparksubmit_config_out = this.prop.getProperty("sparksubmit_config_out")
  private val prop_out: java.util.Properties = {
    val prop0 = new java.util.Properties( )
    val propertiesfile: File = new File(sparksubmit_config_out)
    val inputStream: InputStream = new FileInputStream(propertiesfile)
    prop0.load(inputStream)
    this.log.info("load " + sparksubmit_config_out)
    this.log.info(prop0)
    prop0
  }


  val sparksubmitOpen = PropertiesUtil.prop.getProperty("sparksubmitOpen").toBoolean

  val SPARK_HOME=this.prop.getProperty("SPARK_HOME")
  val JAVA_HOME=this.prop.getProperty("JAVA_HOME")
  val Master=this.prop.getProperty("Master")
  val DeployMode=this.prop.getProperty("DeployMode")

  val logPath: String = this.prop.getProperty("logPath")
  //  val mainClass:String=this.prop.getProperty("mainClass")
  val libPathRoot:String=this.prop.getProperty("libPathRoot")

  val basePath: String = PropertiesUtil.prop_out.getProperty("basePath")
  val eachpartition: Int = PropertiesUtil.prop_out.getProperty("eachpartition").toInt
  val eles: Array[String] = PropertiesUtil.prop_out.getProperty("eles").split(",")
  val baseMd5Path = PropertiesUtil.prop_out.getProperty("baseMd5Path")
  val breakTime: Long = PropertiesUtil.prop_out.getProperty("breakTime").toLong

  def memorryDefault: String = {
    if (this.prop_out.containsKey("memorryDefault")) {
      this.log.info("使用sparksubmit_config_out.properties默认资源分配")
      this.prop_out.getProperty("memorryDefault")
    } else {
      this.log.warn("使用内置默认资源分配")
      "3g,3g,8"
    }
  }

  def memorryStr (dataType: String): String = {
    if (this.prop_out.containsKey(dataType)) {
      this.prop_out.getProperty(dataType)
    } else {
      "3g,3g,8"
    }
  }

  val mainClass0: String = this.prop.getProperty("mainClass0")

  def getmainClass1 (signalMsg: String): String = {
    val (dataType: String, applicationTime: Date, generationFileName: String, timeStamp: Date, attribute: String) = JsonStream.analysisJsonStream(signalMsg)
    this.prop.getProperty("mainClass1Root") + "." + dataType
  }

  def getlibPath (signalMsg: String, splitFile: String): String = {
    val (dataType: String, applicationTime: Date, generationFileName: String, timeStamp: Date, attribute: String) = JsonStream.analysisJsonStream(signalMsg)
    val regionKey=this.getRegion(splitFile)
    libPathRoot + "/" + dataType + "/" + regionKey + "/"
  }

  def getmainJar (signalMsg: String, splitFile: String): String = {
    val mainJar0:String=this.prop.getProperty("mainJar")
    this.getlibPath(signalMsg, splitFile) + "/" + mainJar0
  }
  def getRegion(splitFile:String): String ={
    val uri = URI.create(splitFile)
    val pathName=new Path(uri)
    val kkk={
      if(HDFSConfUtil.isLocal(splitFile)){
        pathName.getParent.getName
      }else{
        pathName.getName
      }
    }


    val region=kkk.split("\\.").head
    region
  }
}
