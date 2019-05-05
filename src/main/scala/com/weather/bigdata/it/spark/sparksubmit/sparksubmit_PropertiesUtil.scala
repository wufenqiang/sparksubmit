package com.weather.bigdata.it.spark.sparksubmit

import java.io.InputStream
import java.util.{Date, Properties}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.weather.bigdata.it.spark.platform.signal.JsonStream
import com.weather.bigdata.it.spark.platform.split.Analysis
import com.weather.bigdata.it.spark.sparksubmit.fileFilter.{jarfilter, propertiesfilter}
import com.weather.bigdata.it.utils.formatUtil.DateFormatUtil
import com.weather.bigdata.it.utils.hdfsUtil.{HDFSFile, HDFSOperation1}
import com.weather.bigdata.it.utils.systemUtil.ipUtil
import org.apache.log4j._

import scala.collection.mutable.ListBuffer

object sparksubmit_PropertiesUtil {
  PropertyConfigurator.configure(Thread.currentThread.getContextClassLoader().getResource("sparksubmit_log4j.properties"))

  val timeStamp=System.currentTimeMillis()
  MDC.put(Constants.timeStampKey,this.timeStamp)
  private val ip:String={
    val host=ipUtil.getHost()
    if(!host.equals("")){
      host
    }else{
      ipUtil.getIp()
    }
  }
  MDC.put(Constants.sparksubmit_ip,this.ip)

  private val prop: java.util.Properties = {
    val prop0: Properties = new java.util.Properties( )
    val loader = Thread.currentThread.getContextClassLoader()
    val loadfile: InputStream=loader.getResourceAsStream("sparksubmit_config.properties")

    prop0.load(loadfile)
    this.log_monitorHDFS.info("load sparksubmit_config.properties")
    this.log_monitorHDFS.info(prop0)
    prop0
  }

  val mainClass0: String = this.prop.getProperty("mainClass0")
  val SPARK_HOME=this.prop.getProperty("SPARK_HOME")
  val JAVA_HOME=this.prop.getProperty("JAVA_HOME")
  val Master=this.prop.getProperty("Master")
  val DeployMode=this.prop.getProperty("DeployMode")
  //  val regionKey0:String=this.prop.getProperty("regionKey")
  private val mainClass1Root:String=this.prop.getProperty("mainClass1Root")
  private val mainJar0:String=this.prop.getProperty("mainJar")
  //  private val platformLibRoot:String=this.prop.getProperty("platformLibRoot")
  //  private val signalLibRoot:String=this.prop.getProperty("signalLibRoot")
  //  private val mt_commonsLibRoot:String=this.prop.getProperty("mt_commonLibRoot")
  private val grib_spark_platformRoot:String=this.prop.getProperty("grib_spark_platformRoot")

  final val platformLibKey="platformLib"
  final val mt_commonsLibKey="mt_commonsLib"
  final val signalLibKey="signalLib"


  def getplatformlibPath_regionKey(regionKey:String,platformRoot:String=this.grib_spark_platformRoot): String ={
    val platformlib= platformRoot + "/" + regionKey + "/"+this.platformLibKey+"/"
    if(!HDFSOperation1.exists(platformlib)){
      HDFSOperation1.mkdirs(platformlib)
    }
    platformlib
  }
  def getmt_commonsLibPath_regionKey(regionKey:String,platformRoot:String=this.grib_spark_platformRoot): String ={
    val mt_commonslib=platformRoot+"/"+regionKey+"/"+this.mt_commonsLibKey+"/"
    if(!HDFSOperation1.exists(mt_commonslib)){
      HDFSOperation1.mkdirs(mt_commonslib)
    }
    mt_commonslib
  }
  def getsignallibPath_dataType_regionKey(dataType:String, regionKey: String,platformRoot:String=this.grib_spark_platformRoot): String ={
    val signallib=platformRoot+"/"+regionKey+"/"+this.signalLibKey+"/"+dataType+"/"
    if(!HDFSOperation1.exists(signallib)){
      HDFSOperation1.mkdirs(signallib)
    }
    signallib
  }

  //  def getmt_commonsLibPath_regionKey(regionKey:String): String ={
  //    val mt_commonslib=this.mt_commonsLibRoot+"/"+regionKey+"/"
  //    if(!HDFSOperation1.exists(mt_commonslib)){
  //      HDFSOperation1.mkdirs(mt_commonslib)
  //    }
  //    mt_commonslib
  //  }


  def getplatformlibPath (splitFile: String): String = {
    val regionKey=Analysis.getRegion(splitFile)
    //    val platformlib=this.platformLibRoot + "/" + regionKey + "/"
    //    if(!HDFSOperation1.exists(platformlib)){
    //      HDFSOperation1.mkdirs(platformlib)
    //    }
    //    platformlib
    this.getplatformlibPath_regionKey(regionKey)
  }
  def getmt_commonsLibPath(splitFile: String): String ={
    val regionKey=Analysis.getRegion(splitFile)
    //    val mt_commonslib=this.mt_commonsLibRoot+"/"+regionKey+"/"
    //    if(!HDFSOperation1.exists(mt_commonslib)){
    //      HDFSOperation1.mkdirs(mt_commonslib)
    //    }
    //    mt_commonslib
    this.getmt_commonsLibPath_regionKey(regionKey)
  }

  def getsignallibPath_dataType(dataType:String, splitFile: String): String ={
    val regionKey=Analysis.getRegion(splitFile)
    //    val signallib=this.signalLibRoot+"/"+regionKey+"/"+dataType+"/"
    //    if(!HDFSOperation1.exists(signallib)){
    //      HDFSOperation1.mkdirs(signallib)
    //    }
    //    signallib
    this.getsignallibPath_dataType_regionKey(dataType,regionKey)
  }
  def getsignallibPath_regionKey(signalMsgJSON: JSONObject, regionKey:String): String ={
    val dataType:String=JsonStream.analysisDataType(signalMsgJSON)
    this.getsignallibPath_dataType_regionKey(dataType,regionKey)
  }
  def getsignallibPath_regionKey(signalMsg: String, regionKey:String): String ={
    val dataType:String=JsonStream.analysisDataType(signalMsg)
    this.getsignallibPath_dataType_regionKey(dataType,regionKey)
  }
  def getsignallibPath(signalMsg: String, splitFile: String): String ={
    this.getsignallibPath(JSON.parseObject(signalMsg),splitFile)
  }
  def getsignallibPath(signalMsgJSON: JSONObject, splitFile: String): String ={
    val dataType:String=JsonStream.analysisDataType(signalMsgJSON)
    //    val regionKey=ConfUtil.getRegion(splitFile)
    //    val signallib=this.signalLibRoot+"/"+regionKey+"/"+dataType+"/"
    //    if(!HDFSOperation1.exists(signallib)){
    //      HDFSOperation1.mkdirs(signallib)
    //    }
    //    signallib
    this.getsignallibPath_dataType(dataType,splitFile)
  }


  private val sparksubmit_configs_out_rootPath=this.prop.getProperty("sparksubmit_configs_out_rootPath")

  private var sparksubmit_configs_out0:String=null
  def getSparkSubmit_configs_out_regionKey(regionKey: String):String={
    if(this.sparksubmit_configs_out0==null){
      this.sparksubmit_configs_out0=this.sparksubmit_configs_out_rootPath+"/"+regionKey+"/"
    }
    this.sparksubmit_configs_out0
  }
  def getSparkSubmit_configs_out(splitFile: String):String={
    if(this.sparksubmit_configs_out0==null){
      val regionKey=Analysis.getRegion(splitFile)
      this.getSparkSubmit_configs_out_regionKey(regionKey)
    }else{
      this.sparksubmit_configs_out0
    }
  }
  //  def getSparkSubmit_config_out_mt(splitFile: String):String={
  //    if(this.sparksubmit_config_out_mt0==null){
  //      this.sparksubmit_config_out_mt0=this.getSparkSubmit_configs_out(splitFile)+"/"+sparksubmit_config_out_mt_fileName
  //    }
  //    this.sparksubmit_config_out_mt0
  //  }
  //  def getSparkSubmit_config_out_it(splitFile: String):String={
  //    if(this.sparksubmit_config_out_it0==null){
  //      this.sparksubmit_config_out_it0=this.getSparkSubmit_configs_out(splitFile)+"/"+sparksubmit_config_out_it_fileName
  //    }
  //    this.sparksubmit_config_out_it0
  //  }
  //  ****************************************************************************

  private var signal_prop_out0: java.util.Properties = null
  private var sparksubmit_prop_out0: java.util.Properties = null


  private def signal_prop_out(signalMsg: String, splitFile: String):java.util.Properties={
    this.signal_prop_out(JSON.parseObject(signalMsg),splitFile)
  }
  private def signal_prop_out(signalMsgJSON: JSONObject, splitFile: String):java.util.Properties ={
    if(this.signal_prop_out0 == null){
      this.signal_prop_out0=new java.util.Properties( )

      val signallibPath:String=this.getsignallibPath(signalMsgJSON,splitFile)
      this.log_monitorHDFS.info("signallibPath(" + signallibPath +"):")
      val configs0:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(signallibPath,new ListBuffer[String],new propertiesfilter)

      val mt_commonsLibPath:String=this.getmt_commonsLibPath(splitFile)
      this.log_monitorHDFS.info("mt_commonsLibPath(" + mt_commonsLibPath +"):")
      val configs1:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(mt_commonsLibPath,new ListBuffer[String],new propertiesfilter)

      val platformlibPath:String=this.getplatformlibPath(splitFile)
      this.log_monitorHDFS.info("platformlibPath(" + platformlibPath +"):")
      val configs2:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(platformlibPath,new ListBuffer[String],new propertiesfilter)

      val configs=configs0.union(configs1).union(configs2)

      configs.foreach(conffile=>{
        val prop_config=new java.util.Properties( )
        this.log_monitorHDFS.info("load_signal_prop_out(" + conffile +"):")
        val is: InputStream = HDFSFile.fileInputStream(conffile)
        prop_config.load(is)
        this.log_monitorHDFS.info("load(" + conffile +"):"+ prop_config)
        this.signal_prop_out0.putAll(prop_config)
      })

      this.log_monitorHDFS.info("load(signal_prop_out):" + signal_prop_out0 )

      //      val prop_mt = new java.util.Properties( )
      //      val prop_it = new java.util.Properties( )
      //
      //      val sparksubmit_config_out_mt=this.getSparkSubmit_config_out_mt(splitFile)
      //      val sparksubmit_config_out_it=this.getSparkSubmit_config_out_it(splitFile)
      //
      //      val inputStream_mt: InputStream = HDFSFile.fileInputStream(sparksubmit_config_out_mt)
      //      val inputStream_it: InputStream = HDFSFile.fileInputStream(sparksubmit_config_out_it)
      //      prop_mt.load(inputStream_mt)
      //      prop_it.load(inputStream_it)
      //      this.log_monitorHDFS.info("load_mt(" + sparksubmit_config_out_mt +"):"+ prop_mt)
      //      this.log_monitorHDFS.info("load_it(" + sparksubmit_config_out_it +"):"+ prop_it)
      //      this.log_monitorHDFS.info(prop_mt)
      //      this.log_monitorHDFS.info(prop_it)
      //      this.prop_out0.putAll(prop_it)
      //      this.prop_out0.putAll(prop_mt)
    }
    this.signal_prop_out0
  }
  private def sparksubmit_prop_out(splitFile:String):java.util.Properties={
    //    if(this.sparksubmit_prop_out0 == null){
    //      this.sparksubmit_prop_out0=new java.util.Properties( )
    //
    //
    //      val sparksubmit_configs_out:String=this.getSparkSubmit_configs_out(splitFile)
    //      this.log_monitorHDFS.info("sparksubmit_configs_out(" + sparksubmit_configs_out +"):")
    //      val configs:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(sparksubmit_configs_out,new ListBuffer[String],new propertiesfilter)
    //
    //
    //      configs.foreach(conffile=>{
    //        val prop_config=new java.util.Properties( )
    //        this.log_monitorHDFS.info("load_sparksubmit_configs_out(" + conffile +")")
    //        val is: InputStream = HDFSFile.fileInputStream(conffile)
    //        prop_config.load(is)
    //        this.log_monitorHDFS.info("load_sparksubmit_configs_out(" + conffile +"):"+ prop_config)
    //        this.sparksubmit_prop_out0.putAll(prop_config)
    //      })
    //
    //      this.log_monitorHDFS.info("load(sparksubmit_prop_out):" + this.sparksubmit_prop_out0 )
    //    }
    //    this.sparksubmit_prop_out0
    val regionKey=Analysis.getRegion(splitFile)
    this.sparksubmit_prop_out_regionKey(regionKey)
  }
  private def sparksubmit_prop_out_regionKey(regionKey: String):java.util.Properties={
    if(this.sparksubmit_prop_out0 == null){
      this.sparksubmit_prop_out0=new java.util.Properties( )


      val sparksubmit_configs_out:String=this.getSparkSubmit_configs_out_regionKey(regionKey)
      this.log_monitorHDFS.info("sparksubmit_configs_out(" + sparksubmit_configs_out +"):")
      val configs:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(sparksubmit_configs_out,new ListBuffer[String],new propertiesfilter)


      configs.foreach(conffile=>{
        val prop_config=new java.util.Properties( )
        this.log_monitorHDFS.info("load_sparksubmit_configs_out(" + conffile +")")
        val is: InputStream = HDFSFile.fileInputStream(conffile)
        prop_config.load(is)
        this.log_monitorHDFS.info("load_sparksubmit_configs_out(" + conffile +"):"+ prop_config)
        this.sparksubmit_prop_out0.putAll(prop_config)
      })

      this.log_monitorHDFS.info("load(sparksubmit_prop_out):" + this.sparksubmit_prop_out0 )
    }
    this.sparksubmit_prop_out0
  }

  private def getlogPath(splitFile: String): String = {
    val log_detail=this.sparksubmit_prop_out(splitFile).getProperty("log_detail")
    println("log_detail="+log_detail)
    log_detail
  }
  private var log_detail_PathRoot0:String=null
  def log_detail_PathRoot(splitFile: String):String={
    if(this.log_detail_PathRoot0==null){
      this.log_detail_PathRoot0=this.getlogPath(splitFile)
      if(!HDFSOperation1.exists(log_detail_PathRoot0)){
        HDFSOperation1.mkdirs(log_detail_PathRoot0)
      }
    }
    this.log_detail_PathRoot0
  }

  private var log_monitorHDFS0:Logger=null
  private var log_sparksubmit0:Logger=null
  private var log_sparksubmit_Detail0:Logger=null

  def log_monitorHDFS:Logger={
    if(this.log_monitorHDFS0==null){
      this.log_monitorHDFS0={
        val log=Logger.getLogger("monitorHDFS")
        log.info("")
        log
      }
    }
    this.log_monitorHDFS0
  }
  def log_sparksubmit:Logger={
    if(this.log_sparksubmit0==null){
      this.log_sparksubmit0={
        val log=Logger.getLogger("sparksubmit")
        log.info("")
        log
      }
    }
    this.log_sparksubmit0
  }

  def sparksubmit_Detail_Open(splitFile: String):Boolean={
    this.sparksubmit_prop_out(splitFile).getProperty("sparksubmit_Detail_Open").toBoolean
  }
  val sparksubmit_logName="sparksubmit_Detail"
  private var sparksubmit_Detail_Path0:String=null
  private def sparksubmit_Detail_Path(splitFile: String):String={
    if(this.sparksubmit_Detail_Path0==null){
      this.sparksubmit_Detail_Path0=this.log_detail_PathRoot( splitFile)+"/"+DateFormatUtil.YYYYMMDDStr0(new Date)+"/"
      if(!HDFSOperation1.exists(this.sparksubmit_Detail_Path0)){
        HDFSOperation1.mkdirs(this.sparksubmit_Detail_Path0)
      }
    }
    this.sparksubmit_Detail_Path0
  }
  private var sparksubmit_Detail_File0:String=null
  def sparksubmit_Detail_File(splitFile: String):String={
    if(this.sparksubmit_Detail_File0==null){
      this.sparksubmit_Detail_File0=this.sparksubmit_Detail_Path(splitFile)+"/"+this.timeStamp+".txt"
    }
    this.sparksubmit_Detail_File0
  }

  //  def set_sparksubmit_Detail:Unit={
  //    if(this.log_sparksubmit_Detail0==null){
  //      this.log_sparksubmit_Detail0={
  //        val log=Logger.getLogger(this.sparksubmit_logName)
  //
  //        val appender =log.getAppender(this.sparksubmit_logName).asInstanceOf[DailyRollingFileAppender]
  //        appender.setFile(sparksubmit_Detail_File)
  //        appender.activateOptions()
  //
  //        log.addAppender(appender)
  //
  //        log
  //      }
  //    }
  //  }
  def set_sparksubmit_Detail(splitFile: String):Unit={
    if(this.log_sparksubmit_Detail0==null){
      this.log_sparksubmit_Detail0={
//        if(!HDFSOperation1.exists(this.sparksubmit_logName)){
//          val creat_flag=HDFSOperation1.createFile(this.sparksubmit_logName)
//          println("creat_flag="+creat_flag+"(sparksubmit_logName="+this.sparksubmit_logName+")")
//        }
        val log=Logger.getLogger(this.sparksubmit_logName)


        //
        //                val appender =log.getAppender(this.sparksubmit_logName).asInstanceOf[DailyRollingFileAppender]
        //                appender.setFile(this.sparksubmit_Detail_File(splitFile))

        //  log4j hdfs文件流重定向失败、
        val appender =log.getAppender(this.sparksubmit_logName).asInstanceOf[WriterAppender]
        appender.setWriter(HDFSFile.filterWriter(this.sparksubmit_Detail_File(splitFile),true))





        appender.activateOptions()

        log.addAppender(appender)

        log
      }
    }
  }

  def sparksubmitOpen(splitFile: String) = this.sparksubmit_prop_out(splitFile).getProperty("sparksubmitOpen").toBoolean

  def basePath(splitFile: String): String = this.sparksubmit_prop_out(splitFile).getProperty("basePath")
  def eachpartition(signalMsg: String, splitFile: String): Int = this.eachpartition(JSON.parseObject(signalMsg),splitFile)
  def eachpartition(signalMsgJSON:JSONObject, splitFile: String): Int =this.signal_prop_out(signalMsgJSON,splitFile).getProperty("eachpartition").toInt
  //  def eles(splitFile: String): Array[String] = this.sparksubmit_prop_out(splitFile).getProperty("eles").split(",")
  def eles(splitFile: String): Array[String] = {
    val regionKey=Analysis.getRegion(splitFile)
    this.eles_regionKey(regionKey)
  }
  //  this.sparksubmit_prop_out(splitFile).getProperty("eles").split(",")
  def eles_regionKey(regionKey:String):Array[String]=this.sparksubmit_prop_out_regionKey(regionKey).getProperty("eles").split(",")

  def baseMd5Path(splitFile: String) = {
    val prop_out=this.sparksubmit_prop_out(splitFile)
    prop_out.getProperty("baseMd5Path")
  }
  def breakTime(splitFile: String): Long = this.sparksubmit_prop_out(splitFile).getProperty("breakTime").toLong
  def memLimit(splitFile: String):String=this.sparksubmit_prop_out(splitFile).getProperty("memLimit")


  def dataTypeConf (signalMsg: String, splitFile: String): String = {
    this.dataTypeConf(JSON.parseObject(signalMsg),splitFile)
  }
  def dataTypeConf (signalMsgJSON: JSONObject, splitFile: String): String = {
    val dataType: String=JsonStream.analysisDataType(signalMsgJSON)
    if (this.signal_prop_out(signalMsgJSON,splitFile).containsKey(dataType)) {
      this.signal_prop_out(signalMsgJSON,splitFile).getProperty(dataType)
    } else {
      val msg="未配置dataType="+dataType+"相应参数(signalMsgJSON="+signalMsgJSON+";splitFile"+splitFile
      this.log_sparksubmit.error(msg)
      ""
    }
  }

  def getmainClass1 (signalMsg: String): String = {
    this.getmainClass1(JSON.parseObject(signalMsg))
  }
  def getmainClass1(signalMsgJSON:JSONObject):String={
    val (dataType: String, applicationTime: Date, generationFileName: String, timeStamp: Date, attribute: String) = JsonStream.analysisJsonStream(signalMsgJSON)
    this.mainClass1Root+ "." + dataType+"."+dataType+"_cal"
  }

  def getmainJar (signalMsg: String, splitFile: String): String = {
    val jarpath=this.getplatformlibPath( splitFile)

    val mainJar1s:ListBuffer[String]=HDFSOperation1.listChildrenAbsoluteFile(jarpath,new ListBuffer[String],new jarfilter).filter(p=>p.contains(mainJar0))
    val mainJar:String={
      val mainJar1=mainJar1s.head
      if(mainJar1s.size!=1){
        val msg="jar包冲突取"+mainJar1+";mainJar0="+mainJar0+";包含的mainJar有:"+mainJar1s.reduce((x,y)=>(x+";"+y))
        this.log_monitorHDFS.warn(msg)
      }
      mainJar1
    }
    mainJar
  }

  def main(args:Array[String]): Unit ={
    val flag0=false
    val flag1=false
    println(flag0.equals(flag1))
  }

}
