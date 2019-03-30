package com.weather.bigdata.it.spark

import java.io.InputStream
import java.net.URI

import com.weather.bigdata.it.utils.hdfsUtil.HDFSConfUtil
import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger

private object PropertiesUtil {
  val log:Logger=Logger.getRootLogger
  val prop:java.util.Properties={
    val prop = new java.util.Properties()
    val loader = Thread.currentThread.getContextClassLoader()
    val loadfile: InputStream=loader.getResourceAsStream("sparksubmit_config.properties")
    prop.load(loadfile)
    prop
  }

  val SPARK_HOME=this.prop.getProperty("SPARK_HOME")
  val JAVA_HOME=this.prop.getProperty("JAVA_HOME")
  val Master=this.prop.getProperty("Master")
  val DeployMode=this.prop.getProperty("DeployMode")


  val mainClass:String=this.prop.getProperty("mainClass")
  val libPathRoot:String=this.prop.getProperty("libPathRoot")

  def getlibPath(splitFile:String):String={
    val regionKey=this.getRegion(splitFile)
    libPathRoot+regionKey+"/"
  }
  def getmainJar(splitFile:String): String ={
    val mainJar0:String=this.prop.getProperty("mainJar")
    this.getlibPath(splitFile)+"/"+mainJar0
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
